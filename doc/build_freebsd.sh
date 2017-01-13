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

# WIP: variant with libs from ports:
# pkg install boost-libs poco google-perftools
# cmake .. -DCMAKE_CXX_COMPILER=`which g++6` -DCMAKE_C_COMPILER=`which gcc6` -DUSE_INTERNAL_BOOST_LIBRARY=0 -DUSE_INTERNAL_POCO_LIBRARY=0 -DUSE_INTERNAL_GPERFTOOLS_LIBRARY=0 -DCXX11_ABI= -DUSE_STATIC_LIBRARIES=0

make -j $(nproc || sysctl -n hw.ncpu || echo 2)
cd ..
