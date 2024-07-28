#!/usr/bin/env bash

set -x -e


STATIC_DATA=${STATIC_DATA:-/woboq_codebrowser/data}
SOURCE_DIRECTORY=${SOURCE_DIRECTORY:-/build}
BUILD_DIRECTORY=${BUILD_DIRECTORY:-/workdir/build}
OUTPUT_DIRECTORY=${OUTPUT_DIRECTORY:-/workdir/output}
HTML_RESULT_DIRECTORY=${HTML_RESULT_DIRECTORY:-$OUTPUT_DIRECTORY/html_report}
SHA=${SHA:-nosha}
DATA=${DATA:-https://s3.amazonaws.com/clickhouse-test-reports/codebrowser/data}
nproc=$(($(nproc) + 2)) # increase parallelism

read -ra CMAKE_FLAGS <<< "${CMAKE_FLAGS:-}"

mkdir -p "$BUILD_DIRECTORY" && cd "$BUILD_DIRECTORY"
cmake "$SOURCE_DIRECTORY" -DCMAKE_CXX_COMPILER="/usr/bin/clang++-${LLVM_VERSION}" -DCMAKE_C_COMPILER="/usr/bin/clang-${LLVM_VERSION}" -DENABLE_WOBOQ_CODEBROWSER=ON "${CMAKE_FLAGS[@]}"
mkdir -p "$HTML_RESULT_DIRECTORY"
echo 'Filter out too noisy "Error: filename" lines and keep them in full codebrowser_generator.log'
/woboq_codebrowser/generator/codebrowser_generator -b "$BUILD_DIRECTORY" -a \
  -o "$HTML_RESULT_DIRECTORY" --execute-concurrency="$nproc" -p "ClickHouse:$SOURCE_DIRECTORY:$SHA" \
  -d "$DATA" \
    |& ts '%Y-%m-%d %H:%M:%S' \
    | tee "$OUTPUT_DIRECTORY/codebrowser_generator.log" \
    | grep --line-buffered -v ':[0-9]* Error: '
cp -r "$STATIC_DATA" "$HTML_RESULT_DIRECTORY/"
/woboq_codebrowser/indexgenerator/codebrowser_indexgenerator "$HTML_RESULT_DIRECTORY" \
  -d "$DATA" |& ts '%Y-%m-%d %H:%M:%S'
