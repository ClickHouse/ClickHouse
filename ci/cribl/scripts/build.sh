#!/bin/bash
set -ex

CORES=$(nproc)

cmake -S . -B ${BUILD_DIR} -DNO_ARMV81_OR_HIGHER=1 -DCMAKE_BUILD_TYPE=Release -DPARALLEL_LINK_JOBS=${CORES}
(cd build && ninja -j ${CORES} clickhouse)