#!/bin/sh
#
# entry point for oss-fuzz, so that fuzzers
# and build invocation can be changed without having
# to modify the oss-fuzz repo.
#
# invoke it from the git root.

# make sure to exit on problems
set -eux

mkdir -p build
cd build

cmake .. \
-GNinja \
-DCMAKE_BUILD_TYPE=Debug \
-DENABLE_FUZZING=On \
-DMINISELECT_FUZZ_LINKMAIN=off \
-DMINISELECT_FUZZ_LDFLAGS=$LIB_FUZZING_ENGINE

cmake --build . --target all_fuzzers

