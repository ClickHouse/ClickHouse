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
find . -name '*.so' -print -exec mv '{}' /output \;
find . -name '*.so.*' -print -exec mv '{}' /output \;
tar -czvf shared_build.tgz /output
rm -r /output/*
mv shared_build.tgz /output
