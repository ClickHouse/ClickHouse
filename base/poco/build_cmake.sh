#!/bin/bash

# POCO_STATIC=1 - for static build
# POCO_UNBUNDLED - for no built-in version of libs
# CMAKE_INSTALL_PREFIX=path - for install path

rm -rf cmake-build
mkdir cmake-build
cd cmake-build

cmake ../. -DCMAKE_BUILD_TYPE=Debug  $1 $2 $3 $4 $5
make -j3
make install

rm -rf CMakeCache.txt

cmake ../. -DCMAKE_BUILD_TYPE=Release $1 $2 $3 $4 $5
make -j3
make install


cd ..
