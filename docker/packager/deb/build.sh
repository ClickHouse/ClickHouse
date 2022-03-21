#!/usr/bin/env bash

set -x -e

# Uncomment to debug ccache.
# export CCACHE_LOGFILE=/build/ccache.log
# export CCACHE_DEBUG=1

ccache --show-config ||:
ccache --show-stats ||:
ccache --zero-stats ||:

read -ra ALIEN_PKGS <<< "${ALIEN_PKGS:-}"
build/release "${ALIEN_PKGS[@]}" | ts '%Y-%m-%d %H:%M:%S'
mv /*.deb /output
mv -- *.changes /output
mv -- *.buildinfo /output
mv /*.rpm /output ||: # if exists
mv /*.tgz /output ||: # if exists

if [ -n "$BINARY_OUTPUT" ] && { [ "$BINARY_OUTPUT" = "programs" ] || [ "$BINARY_OUTPUT" = "tests" ] ;}
then
  echo "Place $BINARY_OUTPUT to output"
  mkdir /output/binary ||: # if exists
  mv /build/obj-*/programs/clickhouse* /output/binary

  if [ "$BINARY_OUTPUT" = "tests" ]
  then
    mv /build/obj-*/src/unit_tests_dbms /output/binary
  fi
fi

# Also build fuzzers if any sanitizer specified
# if [ -n "$SANITIZER" ]
# then
#   # Script is supposed that we are in build directory.
#   mkdir -p build/build_docker
#   cd build/build_docker
#   # Launching build script
#   ../docker/packager/other/fuzzer.sh
#   cd
# fi

ccache --show-config ||:
ccache --show-stats ||:

if [ "${CCACHE_DEBUG:-}" == "1" ]
then
    find /build -name '*.ccache-*' -print0 \
        | tar -c -I pixz -f /output/ccache-debug.txz --null -T -
fi

if [ -n "$CCACHE_LOGFILE" ]
then
    # Compress the log as well, or else the CI will try to compress all log
    # files in place, and will fail because this directory is not writable.
    tar -cv -I pixz -f /output/ccache.log.txz "$CCACHE_LOGFILE"
fi
