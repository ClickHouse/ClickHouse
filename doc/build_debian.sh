#!/bin/sh

# How to build ClickHouse under debian-based systems (ubuntu)

# apt install -y curl sudo
# curl https://raw.githubusercontent.com/yandex/ClickHouse/master/doc/build_debian.sh | sh

# install compiler and libs
sudo apt install -y git bash cmake gcc-6 g++-6 libicu-dev libreadline-dev libmysqlclient-dev unixodbc-dev libltdl-dev libssl-dev

# install testing only stuff if you want:
sudo apt install -y python python-lxml python-termcolor curl perl

# Checkout ClickHouse sources
git clone https://github.com/yandex/ClickHouse.git

# Build!
mkdir -p ClickHouse/build
cd ClickHouse/build
cmake .. -DCMAKE_CXX_COMPILER=`which g++-6` -DCMAKE_C_COMPILER=`which gcc-6`

make -j $(nproc || sysctl -n hw.ncpu || echo 2)
cd ..
