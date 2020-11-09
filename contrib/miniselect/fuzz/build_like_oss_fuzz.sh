#!/bin/sh
#
# This script emulates how oss fuzz invokes the build
# process, handy for trouble shooting cmake issues and possibly
# recreating testcases. For proper debugging of the oss fuzz
# build, follow the procedure at https://google.github.io/oss-fuzz/getting-started/new-project-guide/#testing-locally

set -eu

ossfuzz=$(readlink -f $(dirname $0))/ossfuzz.sh

mkdir -p ossfuzz-out
export OUT=$(pwd)/ossfuzz-out
export CC=clang
export CXX="clang++"
export CFLAGS="-fsanitize=fuzzer-no-link"
export CXXFLAGS="-fsanitize=fuzzer-no-link,address,undefined -O1"
export LIB_FUZZING_ENGINE="-fsanitize=fuzzer"

$ossfuzz

echo "look at the results in $OUT"
