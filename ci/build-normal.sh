#!/usr/bin/env bash
set -e -x

source default-config

[[ -d "${WORKSPACE}/sources" ]] || die "Run get-sources.sh first"

mkdir -p "${WORKSPACE}/build"
pushd "${WORKSPACE}/build"

if [[ "${ENABLE_EMBEDDED_COMPILER}" == 1 ]]; then
    [[ "$USE_LLVM_LIBRARIES_FROM_SYSTEM" == 0 ]] && CMAKE_FLAGS="$CMAKE_FLAGS -DUSE_INTERNAL_LLVM_LIBRARY=1"
    [[ "$USE_LLVM_LIBRARIES_FROM_SYSTEM" != 0 ]] && CMAKE_FLAGS="$CMAKE_FLAGS -DUSE_INTERNAL_LLVM_LIBRARY=0"
fi

cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DENABLE_EMBEDDED_COMPILER=${ENABLE_EMBEDDED_COMPILER} $CMAKE_FLAGS ../sources

[[ "$BUILD_TARGETS" != 'all' ]] && BUILD_TARGETS_STRING="--target $BUILD_TARGETS"

cmake --build . $BUILD_TARGETS_STRING -- -j $THREADS

popd
