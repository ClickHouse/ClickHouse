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

  # Copy all fuzzers
  FUZZER_TARGETS=$(find /build/obj-*/src -name '*_fuzzer' -execdir basename {} ';' | tr '\n' ' ')
  mkdir -p /output/fuzzers ||: # if exists
  for FUZZER_TARGET in $FUZZER_TARGETS
  do
      FUZZER_PATH=$(find /build/obj-*/src -name "$FUZZER_TARGET")
      strip --strip-unneeded "$FUZZER_PATH"
      mv "$FUZZER_PATH" /output/fuzzers ||: # if exists
  done

  tar -zcvf /output/fuzzers.tar.gz /output/fuzzers
  rm -rf /output/fuzzers

  if [ "$BINARY_OUTPUT" = "tests" ]
  then
    mv /build/obj-*/src/unit_tests_dbms /output/binary
  fi
fi

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
