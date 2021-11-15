#!/bin/bash

set -e

SRC_DIR=../..
BUILD_DIR=${SRC_DIR}/build

# BTW, .so files are acceptable from any Linux distribution for the last 12 years (at least).
# See https://presentations.clickhouse.com/cpp_russia_2020/ for the details.

mkdir root
pushd root
mkdir lib lib64 etc tmp root
cp ${BUILD_DIR}/programs/clickhouse .
cp /lib/x86_64-linux-gnu/{libc.so.6,libdl.so.2,libm.so.6,libpthread.so.0,librt.so.1,libnss_dns.so.2,libresolv.so.2} lib
cp /lib64/ld-linux-x86-64.so.2 lib64
cp /etc/resolv.conf ./etc
strip clickhouse

# This is needed for chroot but not needed for Docker:

# mkdir proc
# sudo mount --bind /proc proc
