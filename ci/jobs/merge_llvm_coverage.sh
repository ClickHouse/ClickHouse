#!/bin/bash

echo "Merging LLVM coverage files..."
llvm-profdata merge -sparse *.profraw -o merged.profdata

llvm-cov show 
  ./clickhouse 
  ./unit_tests_dbms
  -instr-profile=merged.profdata 
  -format=html 
  -output-dir=clickhouse_coverage

zip -r clickhouse_coverage.zip clickhouse_coverage