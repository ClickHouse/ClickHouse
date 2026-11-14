#!/usr/bin/env bash
# `EM_COMPILER_WRAPPER` of the wasm64 build: emcc runs every compilation as `emcc_sccache.sh clang ARGS...`.
# sccache refuses `-Xclang -iwithsysroot<dir>`, so each one is passed as the same driver option instead, after
# all other arguments: that keeps it after every `-isystem` in the include search order, as in the `-Xclang` form.
args=()
sysroot_includes=()
while [ $# -gt 0 ]; do
    if [ "$1" = "-Xclang" ] && [ $# -ge 2 ] && [[ "$2" == -iwithsysroot?* ]]; then
        sysroot_includes+=("$2")
        shift 2
        continue
    fi
    if [ "$1" = "-target" ] && [ $# -ge 2 ]; then
        # sccache probes the compiler without these arguments, and emsdk's aarch64 clang has no default target
        export CCC_OVERRIDE_OPTIONS="# ^--target=$2"
    fi
    args+=("$1")
    shift
done
exec sccache "${args[@]}" "${sysroot_includes[@]}"
