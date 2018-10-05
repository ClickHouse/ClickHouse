#!/usr/bin/env bash
set -e -x

source default-config

./install-os-packages.sh libicu-dev
./install-os-packages.sh libreadline-dev

if [[ "$ENABLE_EMBEDDED_COMPILER" == 1 && "$USE_LLVM_LIBRARIES_FROM_SYSTEM" == 1 ]]; then
    ./install-os-packages.sh llvm-libs-5.0
fi
