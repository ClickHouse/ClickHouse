#!/bin/sh

# How to build ClickHouse under freebsd 11+
# [temporary solution before port created]

# pkg install -y curl sudo
# curl https://raw.githubusercontent.com/yandex/ClickHouse/master/doc/build_freebsd.sh | sh

# install compiler and libs
sudo pkg install git cmake bash mysql57-client icu libltdl unixODBC google-perftools

# install testing only stuff if you want:
sudo pkg install python py27-lxml py27-termcolor curl perl5

# Checkout ClickHouse sources
git clone https://github.com/yandex/ClickHouse.git

# Build!
mkdir -p ClickHouse/build
cd ClickHouse/build
cmake .. -DUSE_INTERNAL_GPERFTOOLS_LIBRARY=0
#  WIP: variant with libs from ports:
# sudo pkg install boost-libs
#  Check UNIXODBC option:
# make -C /usr/ports/devel/poco config reinstall
# cmake .. -DUSE_INTERNAL_BOOST_LIBRARY=0 -DUSE_INTERNAL_POCO_LIBRARY=0 -DUSE_INTERNAL_GPERFTOOLS_LIBRARY=0

make -C dbms/src/Server -j $(nproc || sysctl -n hw.ncpu || echo 2)
cd ../..

# run server:
# ClickHouse/build/dbms/src/Server/clickhouse --server --config-file=ClickHouse/dbms/src/Server/config.xml &

# run client:
# ClickHouse/build/dbms/src/Server/clickhouse --client
