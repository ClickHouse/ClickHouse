#!/bin/sh

# How to build ClickHouse under freebsd 11+
# [temporary solution before port created]

# install compiler and libs
pkg install -y git cmake gcc6 bash glib mysql57-client icu libltdl unixODBC

# install testing only stuff if you want:
pkg install -y python py27-lxml py27-termcolor

# Checkout ClickHouse sources
git clone https://github.com/yandex/ClickHouse.git

# Build!
mkdir -p ClickHouse/build
cd ClickHouse/build
cmake .. -DCMAKE_CXX_COMPILER=`which g++6` -DCMAKE_C_COMPILER=`which gcc6`
make -j $(nproc || sysctl -n hw.ncpu || echo 2)
cd ..
