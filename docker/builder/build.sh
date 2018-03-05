#!/bin/bash

mkdir -p /server/build_docker
cd /server/build_docker
cmake /server -DENABLE_EMBEDDED_COMPILER=1 -DENABLE_TESTS=0
make -j $(nproc || grep -c ^processor /proc/cpuinfo)
#ctest -V -j $(nproc || grep -c ^processor /proc/cpuinfo)
