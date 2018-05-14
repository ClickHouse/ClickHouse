#!/usr/bin/env bash
set -e -x

source default-config

# TODO Non debian systems
apt-cache search cmake3 | grep -P '^cmake3 ' && $SUDO apt-get -y install cmake3 || $SUDO apt-get -y install cmake

if [[ "$COMPILER_INSTALL_METHOD" == "packages" ]]; then
    . install-compiler-from-packages.sh;
elif [[ "$COMPILER_INSTALL_METHOD" == "sources" ]]; then
    . install-compiler-from-sources.sh
else
    die "Unknown COMPILER_INSTALL_METHOD"
fi
