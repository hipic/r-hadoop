#!/usr/bin/env Rscript

# Calculate wordcount (https://github.com/RevolutionAnalytics/rmr2/blob/master/docs/tutorial.md)
# Requires rmr package (https://github.com/RevolutionAnalytics/RHadoop/wiki).

library(rmr2)


wordcount = 
  function(
    input, 
    output = NULL, 
    pattern = " "){
    wc.map = 
      function(., lines) {
        keyval(
          unlist(
            strsplit(
              x = lines,
              split = pattern)),
          1)}
    wc.reduce =
      function(word, counts ) {
        keyval(word, sum(counts))}

    mapreduce(
      input = input ,
      output = output,
      input.format = "text",
      map = wc.map,
      reduce = wc.reduce,
      combine = T)}
