#!/bin/bash

NPROC=8

mkdir -p /server/build
cd /server/build

CXX=g++-5 CC=gcc-5 cmake /server

make -j $(NPROC)