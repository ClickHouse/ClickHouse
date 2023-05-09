#!/usr/bin/env bash

set -x -e


STATIC_DATA=${STATIC_DATA:-/woboq_codebrowser/data}
SOURCE_DIRECTORY=${SOURCE_DIRECTORY:-/repo_folder}
BUILD_DIRECTORY=${BUILD_DIRECTORY:-/build}
HTML_RESULT_DIRECTORY=${HTML_RESULT_DIRECTORY:-$BUILD_DIRECTORY/html_report}
SHA=${SHA:-nosha}
DATA=${DATA:-https://s3.amazonaws.com/clickhouse-test-reports/codebrowser/data}

mkdir -p "$BUILD_DIRECTORY" && cd "$BUILD_DIRECTORY"
cmake "$SOURCE_DIRECTORY" -DCMAKE_CXX_COMPILER="/usr/bin/clang++-${LLVM_VERSION}" -DCMAKE_C_COMPILER="/usr/bin/clang-${LLVM_VERSION}" -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_S3=0
mkdir -p "$HTML_RESULT_DIRECTORY"
/woboq_codebrowser/generator/codebrowser_generator -b "$BUILD_DIRECTORY" -a \
  -o "$HTML_RESULT_DIRECTORY" --execute-concurrency=0 -p "ClickHouse:$SOURCE_DIRECTORY:$SHA" \
  -d "$DATA" | ts '%Y-%m-%d %H:%M:%S'
cp -r "$STATIC_DATA" "$HTML_RESULT_DIRECTORY/"
/woboq_codebrowser/indexgenerator/codebrowser_indexgenerator "$HTML_RESULT_DIRECTORY" \
  -d "$DATA" | ts '%Y-%m-%d %H:%M:%S'
mv "$HTML_RESULT_DIRECTORY" /test_output
