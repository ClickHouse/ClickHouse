#!/usr/bin/env bash
set -e -x

source default-config

# TODO Non debian systems
# TODO Install from PPA on older Ubuntu

if [ -f '/etc/lsb-release' ]; then
    source /etc/lsb-release
    if [[ "$DISTRIB_ID" == "Ubuntu" ]]; then
        if [[ "$COMPILER" == "gcc" ]]; then
            $SUDO apt-get -y install gcc-${COMPILER_PACKAGE_VERSION} g++-${COMPILER_PACKAGE_VERSION}
            export CC=gcc-${COMPILER_PACKAGE_VERSION}
            export CXX=g++-${COMPILER_PACKAGE_VERSION}
        elif [[ "$COMPILER" == "clang" ]]; then
            [[ $(uname -m) == "x86_64" ]] && LLD="lld-${COMPILER_PACKAGE_VERSION}"
            $SUDO apt-get -y install clang-${COMPILER_PACKAGE_VERSION} "$LLD" libc++-dev libc++abi-dev
            export CC=clang-${COMPILER_PACKAGE_VERSION}
            export CXX=clang++-${COMPILER_PACKAGE_VERSION}
        else
            die "Unknown compiler specified"
        fi
    else
        die "Unknown Linux variant"
    fi
else
    die "Unknown OS"
fi
