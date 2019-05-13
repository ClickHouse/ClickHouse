#!/usr/bin/env bash

set -x -e

ccache -s ||:
build/release --no-pbuilder
mv /*.deb /output
mv *.changes /output
mv *.buildinfo /output
ccache -s ||:
