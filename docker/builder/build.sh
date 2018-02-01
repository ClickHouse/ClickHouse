#!/bin/bash

export THREADS=$(grep -c ^processor /proc/cpuinfo)
export CC=gcc-7
export CXX=g++-7

mkdir -p /server/build
cd /server/build

cmake /server
make -j $THREADS
