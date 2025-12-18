#!/bin/bash

set -e

echo "Merging LLVM coverage files..."

# Auto-detect available LLVM tools
if [ -z "$LLVM_PROFDATA" ]; then
  for ver in 20 18 19 17 16 ""; do
    if command -v "llvm-profdata${ver:+-$ver}" &> /dev/null; then
      LLVM_PROFDATA="llvm-profdata${ver:+-$ver}"
      break
    fi
  done
fi

if [ -z "$LLVM_COV" ]; then
  for ver in 20 18 19 17 16 ""; do
    if command -v "llvm-cov${ver:+-$ver}" &> /dev/null; then
      LLVM_COV="llvm-cov${ver:+-$ver}"
      break
    fi
  done
fi

echo "Using LLVM tools: $LLVM_PROFDATA, $LLVM_COV"

# Merge profraw files
"$LLVM_PROFDATA" merge -sparse *.profraw -o merged.profdata

# Generate HTML coverage report
"$LLVM_COV" show \
  ./home/ubuntu/actions-runner/_work/ClickHouse/ClickHouse/ci/tmp/clickhouse \
  ./home/ubuntu/actions-runner/_work/ClickHouse/ClickHouse/ci/tmp/unit_tests_dbms \
  -instr-profile=merged.profdata \
  -format=html \
  -output-dir=clickhouse_coverage

# Create archive
tar -czf clickhouse_coverage.tar.gz clickhouse_coverage