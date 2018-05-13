#!/usr/bin/env bash
set -e

source default-config

[[ -d "${WORKSPACE}/sources" ]] || die "Run get-sources.sh first"

mkdir "${WORKSPACE}/build"
pushd "${WORKSPACE}/build"

[[ "$USE_LLVM_LIBRARIES_FROM_SYSTEM" == 0 ]] && CMAKE_FLAGS="$CMAKE_FLAGS -D USE_INTERNAL_LLVM_LIBRARY=1"
[[ "$USE_LLVM_LIBRARIES_FROM_SYSTEM" != 0 ]] && CMAKE_FLAGS="$CMAKE_FLAGS -D USE_INTERNAL_LLVM_LIBRARY=0"

cmake -D CMAKE_BUILD_TYPE=${BUILD_TYPE} $CMAKE_FLAGS ../sources

[[ "$BUILD_TARGETS" != 'all' ]] && BUILD_TARGETS_STRING="--target $BUILD_TARGETS"

cmake --build . $BUILD_TARGETS_STRING -- -j $THREADS

popd
