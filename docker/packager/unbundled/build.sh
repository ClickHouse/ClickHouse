#!/usr/bin/env bash

set -x -e

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
curl -O https://clickhouse-builds.s3.yandex.net/utils/1/dpkg-deb \
    && chmod +x dpkg-deb \
    && cp dpkg-deb /usr/bin

ccache --show-stats ||:
ccache --zero-stats ||:
build/release --no-pbuilder $ALIEN_PKGS | ts '%Y-%m-%d %H:%M:%S'
mv /*.deb /output
mv *.changes /output
mv *.buildinfo /output
mv /*.rpm /output ||: # if exists
mv /*.tgz /output ||: # if exists

ccache --show-stats ||:
ln -s /usr/lib/x86_64-linux-gnu/libOpenCL.so.1.0.0 /usr/lib/libOpenCL.so ||:
