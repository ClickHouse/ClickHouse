#!/usr/bin/env bash

exec &> >(ts)
set -x -e

cache_status () {
    ccache --show-config ||:
    ccache --show-stats ||:
}

git config --global --add safe.directory /build

mkdir -p build/cmake/toolchain/darwin-x86_64
tar xJf MacOSX11.0.sdk.tar.xz -C build/cmake/toolchain/darwin-x86_64 --strip-components=1
ln -sf darwin-x86_64 build/cmake/toolchain/darwin-aarch64

# Uncomment to debug ccache. Don't put ccache log in /output right away, or it
# will be confusingly packed into the "performance" package.
# export CCACHE_LOGFILE=/build/ccache.log
# export CCACHE_DEBUG=1


mkdir -p build/build_docker
cd build/build_docker
rm -f CMakeCache.txt
# Read cmake arguments into array (possibly empty)
read -ra CMAKE_FLAGS <<< "${CMAKE_FLAGS:-}"
env

if [ -n "$MAKE_DEB" ]; then
  rm -rf /build/packages/root
fi

cache_status
# clear cache stats
ccache --zero-stats ||:

if [ "$BUILD_MUSL_KEEPER" == "1" ]
then
    # build keeper with musl separately
    cmake --debug-trycompile --verbose=1 -DBUILD_STANDALONE_KEEPER=1 -DENABLE_CLICKHOUSE_KEEPER=1 -DCMAKE_VERBOSE_MAKEFILE=1 -DUSE_MUSL=1 -LA -DCMAKE_TOOLCHAIN_FILE=/build/cmake/linux/toolchain-x86_64-musl.cmake "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" "-DSANITIZE=$SANITIZER" -DENABLE_CHECK_HEAVY_BUILDS=1 "${CMAKE_FLAGS[@]}" ..
    # shellcheck disable=SC2086 # No quotes because I want it to expand to nothing if empty.
    ninja $NINJA_FLAGS clickhouse-keeper

    ls -la ./programs/
    ldd ./programs/clickhouse-keeper

    if [ -n "$MAKE_DEB" ]; then
      # No quotes because I want it to expand to nothing if empty.
      # shellcheck disable=SC2086
      DESTDIR=/build/packages/root ninja $NINJA_FLAGS programs/keeper/install
    fi
    rm -f CMakeCache.txt

    # Build the rest of binaries
    cmake --debug-trycompile --verbose=1 -DBUILD_STANDALONE_KEEPER=0 -DCREATE_KEEPER_SYMLINK=0 -DCMAKE_VERBOSE_MAKEFILE=1 -LA "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" "-DSANITIZE=$SANITIZER" -DENABLE_CHECK_HEAVY_BUILDS=1 "${CMAKE_FLAGS[@]}" ..
else
    # Build everything
    cmake --debug-trycompile --verbose=1 -DCMAKE_VERBOSE_MAKEFILE=1 -LA "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" "-DSANITIZE=$SANITIZER" -DENABLE_CHECK_HEAVY_BUILDS=1 "${CMAKE_FLAGS[@]}" ..
fi

if [ "coverity" == "$COMBINED_OUTPUT" ]
then
    mkdir -p /opt/cov-analysis

    wget --post-data "token=$COVERITY_TOKEN&project=ClickHouse%2FClickHouse" -qO- https://scan.coverity.com/download/linux64 | tar xz -C /opt/cov-analysis --strip-components 1
    export PATH=$PATH:/opt/cov-analysis/bin
    cov-configure --config ./coverity.config --template --comptype clangcc --compiler "$CC"
    SCAN_WRAPPER="cov-build --config ./coverity.config --dir cov-int"
fi

# No quotes because I want it to expand to nothing if empty.
# shellcheck disable=SC2086 # No quotes because I want it to expand to nothing if empty.
$SCAN_WRAPPER ninja $NINJA_FLAGS clickhouse-bundle

ls -la ./programs

cache_status

if [ -n "$MAKE_DEB" ]; then
  # No quotes because I want it to expand to nothing if empty.
  # shellcheck disable=SC2086
  DESTDIR=/build/packages/root ninja $NINJA_FLAGS install
  bash -x /build/packages/build
fi

mv ./programs/clickhouse* /output
mv ./src/unit_tests_dbms /output ||: # may not exist for some binary builds
find . -name '*.so' -print -exec mv '{}' /output \;
find . -name '*.so.*' -print -exec mv '{}' /output \;

# Different files for performance test.
if [ "performance" == "$COMBINED_OUTPUT" ]
then
    cp -r ../tests/performance /output
    cp -r ../tests/config/top_level_domains  /output
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
    tar -cv -I pigz -f "$COMBINED_OUTPUT.tgz" /output
    rm -r /output/*
    mv "$COMBINED_OUTPUT.tgz" /output
fi

if [ "coverity" == "$COMBINED_OUTPUT" ]
then
    tar -cv -I pigz -f "coverity-scan.tgz" cov-int
    mv "coverity-scan.tgz" /output
fi

# Also build fuzzers if any sanitizer specified
# if [ -n "$SANITIZER" ]
# then
#   # Currently we are in build/build_docker directory
#   ../docker/packager/other/fuzzer.sh
# fi

cache_status

if [ "${CCACHE_DEBUG:-}" == "1" ]
then
    find . -name '*.ccache-*' -print0 \
        | tar -c -I pixz -f /output/ccache-debug.txz --null -T -
fi

if [ -n "$CCACHE_LOGFILE" ]
then
    # Compress the log as well, or else the CI will try to compress all log
    # files in place, and will fail because this directory is not writable.
    tar -cv -I pixz -f /output/ccache.log.txz "$CCACHE_LOGFILE"
fi

ls -l /output
