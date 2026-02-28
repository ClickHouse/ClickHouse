#!/bin/bash
set -e

SRC="bss_benchmark_clickhouse_match.cpp"
BIN_GCC="bss_benchmark_gcc"
BIN_CLANG="bss_benchmark_clang"

echo "Using source file: $SRC"
echo ""

# --- build with Clang ---
echo "Building $SRC with Clang..."
clang++ -O2 -march=native -std=c++17 -o "$BIN_CLANG" "$SRC"
echo "Clang build OK"
echo ""

echo "Generating Clang assembly..."
clang++ -O2 -march=native -std=c++17 -S "$SRC" -o clang.s
echo "Clang assembly generated: clang.s"
echo ""

echo "Running Clang binary..."
perf stat ./"$BIN_CLANG"
echo ""