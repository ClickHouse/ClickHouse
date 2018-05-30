#!/bin/bash

#ccache -s
mkdir -p /server/build_docker
cd /server/build_docker
cmake -G Ninja /server -DENABLE_TESTS=1
cmake --build .
env TEST_OPT="--skip long compile $TEST_OPT" ctest -V -j $(nproc || grep -c ^processor /proc/cpuinfo)
