#!/usr/bin/env bash
set -x -e

exec &> >(ts)

ccache_status () {
    ccache --show-config ||:
    ccache --show-stats ||:
    SCCACHE_NO_DAEMON=1 sccache --show-stats ||:
}

[ -O /build ] || git config --global --add safe.directory /build

if [ "$EXTRACT_TOOLCHAIN_DARWIN" = "1" ]; then
  mkdir -p /build/cmake/toolchain/darwin-x86_64
  tar xJf /MacOSX11.0.sdk.tar.xz -C /build/cmake/toolchain/darwin-x86_64 --strip-components=1
  ln -sf darwin-x86_64 /build/cmake/toolchain/darwin-aarch64

  if [ "$EXPORT_SOURCES_WITH_SUBMODULES" = "1" ]; then
    cd /build
    tar --exclude-vcs-ignores --exclude-vcs --exclude build --exclude build_docker --exclude debian --exclude .git --exclude .github --exclude .cache --exclude docs --exclude tests/integration -c . | pigz -9 > /output/source_sub.tar.gz
  fi
fi

# Uncomment to debug ccache. Don't put ccache log in /output right away, or it
# will be confusingly packed into the "performance" package.
# export CCACHE_LOGFILE=/build/ccache.log
# export CCACHE_DEBUG=1


mkdir -p /build/build_docker
cd /build/build_docker
rm -f CMakeCache.txt
# Read cmake arguments into array (possibly empty)
read -ra CMAKE_FLAGS <<< "${CMAKE_FLAGS:-}"
env

if [ -n "$MAKE_DEB" ]; then
  rm -rf /build/packages/root
  # NOTE: this is for backward compatibility with previous releases,
  # that does not diagnostics tool (only script).
  if [ -d /build/programs/diagnostics ]; then
    if [ -z "$SANITIZER" ]; then
      # We need to check if clickhouse-diagnostics is fine and build it
      (
        cd /build/programs/diagnostics
        make test-no-docker
        GOARCH="${DEB_ARCH}" CGO_ENABLED=0 make VERSION="$VERSION_STRING" build
        mv clickhouse-diagnostics ..
      )
    else
      echo -e "#!/bin/sh\necho 'Not implemented for this type of package'" > /build/programs/clickhouse-diagnostics
      chmod +x /build/programs/clickhouse-diagnostics
    fi
  fi
fi


ccache_status
# clear cache stats
ccache --zero-stats ||:

if [ "$BUILD_MUSL_KEEPER" == "1" ]
then
    # build keeper with musl separately
    # and without rust bindings
    cmake --debug-trycompile -DENABLE_RUST=OFF -DBUILD_STANDALONE_KEEPER=1 -DENABLE_CLICKHOUSE_KEEPER=1 -DCMAKE_VERBOSE_MAKEFILE=1 -DUSE_MUSL=1 -LA -DCMAKE_TOOLCHAIN_FILE=/build/cmake/linux/toolchain-x86_64-musl.cmake "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" "-DSANITIZE=$SANITIZER" -DENABLE_CHECK_HEAVY_BUILDS=1 "${CMAKE_FLAGS[@]}" ..
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
    cmake --debug-trycompile -DBUILD_STANDALONE_KEEPER=0 -DCREATE_KEEPER_SYMLINK=0 -DCMAKE_VERBOSE_MAKEFILE=1 -LA "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" "-DSANITIZE=$SANITIZER" -DENABLE_CHECK_HEAVY_BUILDS=1 "${CMAKE_FLAGS[@]}" ..
else
    # Build everything
    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" "-DSANITIZE=$SANITIZER" -DENABLE_CHECK_HEAVY_BUILDS=1 "${CMAKE_FLAGS[@]}" ..
fi

if [ "coverity" == "$COMBINED_OUTPUT" ]
then
    mkdir -p /workdir/cov-analysis

    wget --post-data "token=$COVERITY_TOKEN&project=ClickHouse%2FClickHouse" -qO- https://scan.coverity.com/download/linux64 | tar xz -C /workdir/cov-analysis --strip-components 1
    export PATH=$PATH:/workdir/cov-analysis/bin
    cov-configure --config ./coverity.config --template --comptype clangcc --compiler "$CC"
    SCAN_WRAPPER="cov-build --config ./coverity.config --dir cov-int"
fi

# No quotes because I want it to expand to nothing if empty.
# shellcheck disable=SC2086 # No quotes because I want it to expand to nothing if empty.
$SCAN_WRAPPER ninja $NINJA_FLAGS $BUILD_TARGET

ls -la ./programs

ccache_status

if [ -n "$MAKE_DEB" ]; then
  # No quotes because I want it to expand to nothing if empty.
  # shellcheck disable=SC2086
  DESTDIR=/build/packages/root ninja $NINJA_FLAGS programs/install
  cp /build/programs/clickhouse-diagnostics /build/packages/root/usr/bin
  cp /build/programs/clickhouse-diagnostics /output
  bash -x /build/packages/build
fi

mv ./programs/clickhouse* /output
[ -x ./programs/self-extracting/clickhouse ] && mv ./programs/self-extracting/clickhouse /output
mv ./src/unit_tests_dbms /output ||: # may not exist for some binary builds

prepare_combined_output () {
    local OUTPUT
    OUTPUT="$1"

    mkdir -p "$OUTPUT"/config
    cp /build/programs/server/config.xml "$OUTPUT"/config
    cp /build/programs/server/users.xml "$OUTPUT"/config
    cp -r --dereference /build/programs/server/config.d "$OUTPUT"/config
}

# Different files for performance test.
if [ "$WITH_PERFORMANCE" == 1 ]
then
    PERF_OUTPUT=/workdir/performance/output
    mkdir -p "$PERF_OUTPUT"
    cp -r ../tests/performance "$PERF_OUTPUT"
    cp -r ../tests/config/top_level_domains  "$PERF_OUTPUT"
    cp -r ../docker/test/performance-comparison/config "$PERF_OUTPUT" ||:
    for SRC in /output/clickhouse*; do
        # Copy all clickhouse* files except packages and bridges
        [[ "$SRC" != *.* ]] && [[ "$SRC" != *-bridge ]] && \
          cp -d "$SRC" "$PERF_OUTPUT"
    done
    if [ -x "$PERF_OUTPUT"/clickhouse-keeper ]; then
        # Replace standalone keeper by symlink
        ln -sf clickhouse "$PERF_OUTPUT"/clickhouse-keeper
    fi

    cp -r ../docker/test/performance-comparison "$PERF_OUTPUT"/scripts ||:
    prepare_combined_output "$PERF_OUTPUT"

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
    mkdir "$PERF_OUTPUT"/ch
    # Copy .git only, but skip modules, using tar
    tar c -C /build/ --exclude='.git/modules/**' .git | tar x -C "$PERF_OUTPUT"/ch
    # Create branch pr and origin/master to have them for the following performance comparison
    git -C "$PERF_OUTPUT"/ch branch pr
    git -C "$PERF_OUTPUT"/ch fetch --no-tags --no-recurse-submodules --depth 50 origin master:origin/master
    # Clean remote, to not have it stale
    git -C "$PERF_OUTPUT"/ch remote | xargs -n1 git -C "$PERF_OUTPUT"/ch remote remove
    # And clean all tags
    echo "Deleting $(git -C "$PERF_OUTPUT"/ch tag | wc -l) tags"
    git -C "$PERF_OUTPUT"/ch tag | xargs git -C "$PERF_OUTPUT"/ch tag -d >/dev/null
    git -C "$PERF_OUTPUT"/ch reset --soft pr
    git -C "$PERF_OUTPUT"/ch log -5
    (
        cd "$PERF_OUTPUT"/..
        tar -cv --zstd -f /output/performance.tar.zst output
    )
fi

# May be set for performance test.
if [ "" != "$COMBINED_OUTPUT" ]
then
    prepare_combined_output /output
    tar -cv --zstd -f "$COMBINED_OUTPUT.tar.zst" /output
    rm -r /output/*
    mv "$COMBINED_OUTPUT.tar.zst" /output
fi

if [ "coverity" == "$COMBINED_OUTPUT" ]
then
    # Coverity does not understand ZSTD.
    tar -cvz -f "coverity-scan.tar.gz" cov-int
    mv "coverity-scan.tar.gz" /output
fi

ccache_status
ccache --evict-older-than 1d

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
