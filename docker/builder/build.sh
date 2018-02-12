#!/bin/bash

export THREADS=$(grep -c ^processor /proc/cpuinfo)
#export CC=gcc-7
#export CXX=g++-7

mkdir -p /server/build
cmake /server -DUSE_EMBEDDED_COMPILER=1
make -j $THREADS
