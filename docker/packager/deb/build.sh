#!/usr/bin/env bash

set -x -e

ccache --show-stats ||:
ccache --zero-stats ||:
build/release --no-pbuilder
mv /*.deb /output
mv *.changes /output
mv *.buildinfo /output
ccache --show-stats ||:
