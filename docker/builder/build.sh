#!/bin/bash

export THREADS=$(grep -c ^processor /proc/cpuinfo)
export CC=gcc-7
export CXX=g++-7

cmake /server
cd /server
mkdir -p /server/build
make -j $THREADS
