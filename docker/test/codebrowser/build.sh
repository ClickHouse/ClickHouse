#!/usr/bin/env bash

set -x -e


STATIC_DATA=${STATIC_DATA:-/woboq_codebrowser/data}
SOURCE_DIRECTORY=${SOURCE_DIRECTORY:-/build}
BUILD_DIRECTORY=${BUILD_DIRECTORY:-/workdir/build}
HTML_RESULT_DIRECTORY=${HTML_RESULT_DIRECTORY:-/workdir/output/html_report}
SHA=${SHA:-nosha}
DATA=${DATA:-https://s3.amazonaws.com/clickhouse-test-reports/codebrowser/data}

read -ra CMAKE_FLAGS <<< "${CMAKE_FLAGS:-}"

mkdir -p "$BUILD_DIRECTORY" && cd "$BUILD_DIRECTORY"
cmake "$SOURCE_DIRECTORY" -DCMAKE_CXX_COMPILER="/usr/bin/clang++-${LLVM_VERSION}" -DCMAKE_C_COMPILER="/usr/bin/clang-${LLVM_VERSION}" -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DENABLE_EMBEDDED_COMPILER=0 "${CMAKE_FLAGS[@]}"
mkdir -p "$HTML_RESULT_DIRECTORY"
/woboq_codebrowser/generator/codebrowser_generator -b "$BUILD_DIRECTORY" -a \
  -o "$HTML_RESULT_DIRECTORY" --execute-concurrency=0 -p "ClickHouse:$SOURCE_DIRECTORY:$SHA" \
  -d "$DATA" |& ts '%Y-%m-%d %H:%M:%S'
cp -r "$STATIC_DATA" "$HTML_RESULT_DIRECTORY/"
/woboq_codebrowser/indexgenerator/codebrowser_indexgenerator "$HTML_RESULT_DIRECTORY" \
  -d "$DATA" |& ts '%Y-%m-%d %H:%M:%S'
