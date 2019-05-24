#!/usr/bin/env bash

set -x -e

mkdir -p build/build_docker
cd build/build_docker
ccache --show-stats ||:
ccache --zero-stats ||:
rm -f CMakeCache.txt
cmake .. -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DSANITIZE=$SANITIZER $CMAKE_FLAGS
ninja
ccache --show-stats ||:
mv ./dbms/programs/clickhouse* /output
mv ./dbms/unit_tests_dbms /output
