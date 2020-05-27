#!/usr/bin/env bash

set -x -e

mkdir -p build/cmake/toolchain/darwin-x86_64
tar xJf MacOSX10.14.sdk.tar.xz -C build/cmake/toolchain/darwin-x86_64 --strip-components=1

mkdir -p build/cmake/toolchain/linux-aarch64
tar xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz -C build/cmake/toolchain/linux-aarch64 --strip-components=1

mkdir -p build/cmake/toolchain/freebsd-x86_64
tar xJf freebsd-11.3-toolchain.tar.xz -C build/cmake/toolchain/freebsd-x86_64 --strip-components=1

mkdir -p build/build_docker
cd build/build_docker
ccache --show-stats ||:
ccache --zero-stats ||:
ln -s /usr/lib/x86_64-linux-gnu/libOpenCL.so.1.0.0 /usr/lib/libOpenCL.so ||:
rm -f CMakeCache.txt
cmake .. -LA -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DSANITIZE=$SANITIZER $CMAKE_FLAGS
ninja
ccache --show-stats ||:
mv ./programs/clickhouse* /output
mv ./src/unit_tests_dbms /output
find . -name '*.so' -print -exec mv '{}' /output \;
find . -name '*.so.*' -print -exec mv '{}' /output \;

# Different files for performance test.
if [ "performance" == "$COMBINED_OUTPUT" ]
then
    cp -r ../tests/performance /output
    cp -r ../docker/test/performance-comparison/config /output ||:
    rm /output/unit_tests_dbms ||:
    rm /output/clickhouse-odbc-bridge ||:

    cp -r ../docker/test/performance-comparison /output/scripts ||:
fi

# May be set for split build or for performance test.
if [ "" != "$COMBINED_OUTPUT" ]
then
    mkdir -p /output/config
    cp ../programs/server/config.xml /output/config
    cp ../programs/server/users.xml /output/config
    cp -r ../programs/server/config.d /output/config
    tar -czvf "$COMBINED_OUTPUT.tgz" /output
    rm -r /output/*
    mv "$COMBINED_OUTPUT.tgz" /output
fi
