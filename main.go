package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/goware/urlx"
	"github.com/scizorman/go-ndjson"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// NdJson model output
type NdJson struct {
	TimeStamp int64  `json:"ts"`
	SourceIp  string `json:"source_ip"`
	Url       Url    `json:"url"`
	Size      string `json:"size"`
	Note      string `json:"note"`
}

// Url Normalized url
type Url struct {
	Scheme string `json:"Scheme"`
	Host   string `json:"Host"`
	Path   string `json:"Path"`
	Opaque string `json:"Opaque"`
}

// Args filename input && output files
var filename = flag.String("f", "REQUIRED", "source CSV file")
var output = flag.String("o", "REQUIRED", "output json file")

var DATA []NdJson

// main
// Parallel processing of CSV using goroutines
func main() {
	start := time.Now()
	flag.Parse()
	fmt.Print(strings.Join(flag.Args(), "\n"))
	if *filename == "REQUIRED" {
		return
	}
	if *output == "REQUIRED" {
		return
	}

	csvfile, err := os.Open(*filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer csvfile.Close()

	reader := csv.NewReader(csvfile)

	i := 0
	ch := make(chan []string)
	// sync.WaitGroup provides a goroutine synchronization mechanism in Golang, and is used for waiting for a collection of goroutines to finis
	// https://nathanleclaire.com/blog/2014/02/15/how-to-wait-for-all-goroutines-to-finish-executing-before-continuing/
	var wg sync.WaitGroup
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println(err)
			return
		}
		i++

		wg.Add(1)
		// We’ll use sync.WaitGroup to encapsulate the counting of goroutines.
		// Package sync provides basic synchronization primitives such as mutual exclusion locks.
		// Other than the Once and WaitGroup types, most are intended for use by low-level library routines.
		// Higher-level synchronization is better done via channels and communication..
		go func(r []string, i int) {
			defer wg.Done()
			processData(i, r)
			ch <- r

		}(record, i)

		log.Println("---------record-------", record)
	}

	// closer
	go func() {
		wg.Wait()
		close(ch)
	}()

	// print channel results (necessary to prevent exit programm before)
	j := 0
	for range ch {
		j++
		log.Println("---ch---", ch)
	}

	fmt.Printf("\n%2fs\n", time.Since(start).Seconds())

}

// processData to process each line of the csv file and create the final ndjson file
func processData(i int, r []string) []NdJson {
	var data NdJson

	time.Sleep(time.Duration(1000+rand.Intn(8000)) * time.Millisecond)
	log.Println("---i---", i)

	if i > 1 {
		log.Println("---r---", r)
		data = parseStruct(r)
		log.Println("-----data----", data)
		DATA = append(DATA, data)
		log.Println("----DATA---", DATA)

		file, _ := ndjson.Marshal(DATA)
		//if err != nil {
		//	return nil, errors.WithStack(err)
		//}

		//file, _ := json.MarshalIndent(DATA, "", " ")
		_ = ioutil.WriteFile(*output, file, 0644)
	}

	return DATA

}

// parseStruct return the json data
func parseStruct(data []string) NdJson {
	var ip string
	url, _ := urlx.Parse(data[3])
	normalized, _ := urlx.Normalize(url)

	fmt.Println("---normalized----", normalized)

	note := callApiMath("http://numbersapi.com/random/math")
	ts := toTimeStamp(data[1])
	isValidIP := IsValidIp(data[2])
	if isValidIP == true {
		ip = data[2]
	} else {
		ip = "IP Not Valid"
	}

	return NdJson{
		TimeStamp: ts,
		SourceIp:  ip,
		Url:       Url{Scheme: url.Scheme, Host: url.Host, Path: url.Path, Opaque: url.Opaque},
		Size:      data[4],
		Note:      note,
	}
}

// callApiMath call api for note info
func callApiMath(api string) string {
	response, err := http.Get(api)
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}
	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(responseData))
	return string(responseData)
}

// toTimeStamp convert string to time then time to timestamp
func toTimeStamp(timeString string) int64 {
	dateTime, e := time.Parse(time.RFC3339, timeString)
	if e != nil {
		panic("Parse error")
	}
	timestamp := dateTime.Unix()
	fmt.Println("Date to Timestamp : ", timestamp)

	return timestamp
}

func IsValidIp(ip string) bool {
	if r := net.ParseIP(ip); r == nil {
		return false
	}
	return true
}
