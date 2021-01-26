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
# Read cmake arguments into array (possibly empty)
read -ra CMAKE_FLAGS <<< "${CMAKE_FLAGS:-}"
cmake --debug-trycompile --verbose=1 -DCMAKE_VERBOSE_MAKEFILE=1 -LA "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" "-DSANITIZE=$SANITIZER" -DENABLE_CHECK_HEAVY_BUILDS=1 "${CMAKE_FLAGS[@]}" ..
# shellcheck disable=SC2086 # No quotes because I want it to expand to nothing if empty.
ninja $NINJA_FLAGS clickhouse-bundle
mv ./programs/clickhouse* /output
mv ./src/unit_tests_dbms /output ||: # may not exist for some binary builds
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

    # We have to know the revision that corresponds to this binary build.
    # It is not the nominal SHA from pull/*/head, but the pull/*/merge, which is
    # head merged to master by github, at some point after the PR is updated.
    # There are some quirks to consider:
    # - apparently the real SHA is not recorded in system.build_options;
    # - it can change at any time as github pleases, so we can't just record
    #   the SHA and use it later, it might become inaccessible;
    # - CI has an immutable snapshot of repository that it uses for all checks
    #   for a given nominal SHA, but it is not accessible outside Yandex.
    # This is why we add this repository snapshot from CI to the performance test
    # package.
    mkdir /output/ch
    git -C /output/ch init --bare
    git -C /output/ch remote add origin /build
    git -C /output/ch fetch --no-tags --depth 50 origin HEAD:pr
    git -C /output/ch fetch --no-tags --depth 50 origin master:master
    git -C /output/ch reset --soft pr
    git -C /output/ch log -5
fi

# May be set for split build or for performance test.
if [ "" != "$COMBINED_OUTPUT" ]
then
    mkdir -p /output/config
    cp ../programs/server/config.xml /output/config
    cp ../programs/server/users.xml /output/config
    cp -r --dereference ../programs/server/config.d /output/config
    tar -czvf "$COMBINED_OUTPUT.tgz" /output
    rm -r /output/*
    mv "$COMBINED_OUTPUT.tgz" /output
fi
ccache --show-stats ||:
