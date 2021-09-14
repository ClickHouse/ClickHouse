#!/usr/bin/env bash
set -e

#ccache -s # uncomment to display CCache statistics
mkdir -p /server/build_docker
cd /server/build_docker
cmake -G Ninja /server "-DCMAKE_C_COMPILER=$(command -v clang-11)" "-DCMAKE_CXX_COMPILER=$(command -v clang++-11)"

# Set the number of build jobs to the half of number of virtual CPU cores (rounded up).
# By default, ninja use all virtual CPU cores, that leads to very high memory consumption without much improvement in build time.
# Note that modern x86_64 CPUs use two-way hyper-threading (as of 2018).
# Without this option my laptop with 16 GiB RAM failed to execute build due to full system freeze.
NUM_JOBS=$(( ($(nproc || grep -c ^processor /proc/cpuinfo) + 1) / 2 ))

ninja -j $NUM_JOBS && env TEST_OPT="--skip long compile $TEST_OPT" ctest -V -j $NUM_JOBS
