#!/usr/bin/env bash

# This script is responsible for building all fuzzers, and copy them to output directory
# as an archive.
# Script is supposed that we are in build directory.

set -x -e

printenv

# Delete previous cache, because we add a new flags -DENABLE_FUZZING=1 and -DFUZZER=libfuzzer
rm -f CMakeCache.txt
read -ra CMAKE_FLAGS <<< "${CMAKE_FLAGS:-}"
# Hope, that the most part of files will be in cache, so we just link new executables
cmake --debug-trycompile --verbose=1 -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_C_COMPILER="$CC" -DCMAKE_CXX_COMPILER="$CXX" -DENABLE_CLICKHOUSE_ODBC_BRIDGE=OFF \
    -DENABLE_LIBRARIES=0 -DENABLE_SSL=1 -DUSE_INTERNAL_SSL_LIBRARY=1 -DUSE_UNWIND=ON -DENABLE_EMBEDDED_COMPILER=0 \
    -DENABLE_EXAMPLES=0 -DENABLE_UTILS=0 -DENABLE_THINLTO=0 "-DSANITIZE=$SANITIZER" \
    -DENABLE_FUZZING=1 -DFUZZER='libfuzzer' -DENABLE_TCMALLOC=0 -DENABLE_JEMALLOC=0 \
    -DENABLE_CHECK_HEAVY_BUILDS=1 "${CMAKE_FLAGS[@]}" ..

FUZZER_TARGETS=$(find ../src -name '*_fuzzer.cpp' -execdir basename {} .cpp ';' | tr '\n' ' ')

mkdir -p /output/fuzzers
for FUZZER_TARGET in $FUZZER_TARGETS
do
    # shellcheck disable=SC2086 # No quotes because I want it to expand to nothing if empty.
    ninja $NINJA_FLAGS $FUZZER_TARGET
    # Find this binary in build directory and strip it
    FUZZER_PATH=$(find ./src -name "$FUZZER_TARGET")
    strip --strip-unneeded "$FUZZER_PATH"
    mv "$FUZZER_PATH" /output/fuzzers
done

tar -zcvf /output/fuzzers.tar.gz /output/fuzzers
rm -rf /output/fuzzers
