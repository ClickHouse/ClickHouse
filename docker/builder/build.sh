#!/bin/bash

export THREADS=$(grep -c ^processor /proc/cpuinfo)
export CC=gcc-7
export CXX=g++-7

mkdir -p /server/build
cmake /server
cd /server/build
make -j $THREADS
