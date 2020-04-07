#!/usr/bin/env bash

set -x -e

ccache --show-stats ||:
ccache --zero-stats ||:
build/release --no-pbuilder $ALIEN_PKGS | ts '%Y-%m-%d %H:%M:%S'
mv /*.deb /output
mv *.changes /output
mv *.buildinfo /output
mv /*.rpm /output ||: # if exists
mv /*.tgz /output ||: # if exists

if [ "binary" == "$BINARY_OUTPUT" ]
then
  mkdir /output/binary
  mv /build/obj-x86_64-linux-gnu/programs/clickhouse* /output/binary
  mv /build/obj-x86_64-linux-gnu/dbms/unit_tests_dbms /output/binary
fi
ccache --show-stats ||:
ln -s /usr/lib/x86_64-linux-gnu/libOpenCL.so.1.0.0 /usr/lib/libOpenCL.so ||:
