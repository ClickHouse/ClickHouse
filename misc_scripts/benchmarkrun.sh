#!/bin/bash
set -e

SRC_XSIMD="${1:-bss_benchmark_arrow_impl.cpp}"
SRC_OURS="${2:-bss_benchmark_clickhouse_match.cpp}"
XSIMD_DIR="xsimd"
XSIMD_INCLUDE="$XSIMD_DIR/include"

# --- download xsimd if not present ---
if [ ! -d "$XSIMD_DIR" ]; then
  echo "Downloading xsimd..."
  git clone --depth 1 https://github.com/xtensor-stack/xsimd.git "$XSIMD_DIR"
  echo ""
fi

echo "=========================================="
echo "  Arrow (xsimd): $SRC_XSIMD"
echo "=========================================="
echo ""

echo "Building with Clang -O2..."
clang++ -O2 -march=native -std=c++20 -mavx2 -I"$XSIMD_INCLUDE" -o bss_arrow "$SRC_XSIMD"
echo "Build OK"
echo ""

echo "Generating assembly..."
clang++ -O2 -march=native -std=c++20 -mavx2 -I"$XSIMD_INCLUDE" -S "$SRC_XSIMD" -o arrow.s
echo "Assembly generated: arrow.s"
echo ""

echo "Running..."
./bss_arrow
echo ""

echo "=========================================="
echo "  Ours: $SRC_OURS"
echo "=========================================="
echo ""

echo "Building with Clang -O2..."
clang++ -O2 -march=native -std=c++17 -mavx2 -o bss_ours "$SRC_OURS"
echo "Build OK"
echo ""

echo "Generating assembly..."
clang++ -O2 -march=native -std=c++17 -mavx2 -S "$SRC_OURS" -o ours.s
echo "Assembly generated: ours.s"
echo ""

echo "Running..."
./bss_ours
echo ""

