#!/bin/bash

export THREADS=$(grep -c ^processor /proc/cpuinfo)
export CC=gcc-7
export CXX=g++-7

mkdir -p ${CI_PROJECT_DIR}/build
cmake ${CI_PROJECT_DIR}
make -j $THREADS
make clickhouse