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
ccache --show-stats ||:
