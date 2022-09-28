#!/bin/sh -e

OS=$(uname -s)
ARCH=$(uname -m)

DIR=

if [ "${OS}" = "Linux" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        DIR="amd64"
    elif [ "${ARCH}" = "aarch64" -o "${ARCH}" = "arm64" ]
    then
        # If the system has >=ARMv8.2 (https://en.wikipedia.org/wiki/AArch64), choose the corresponding build, else fall back to a v8.0
        # compat build. Unfortunately, the ARM ISA level cannot be read directly, we need to guess from the "features" in /proc/cpuinfo.
        # Also, the flags in /proc/cpuinfo are named differently than the flags passed to the compiler (cmake/cpu_features.cmake).
        ARMV82=$(grep -m 1 'Features' /proc/cpuinfo | awk '/asimd/ && /sha1/ && /aes/ && /atomics/ && /lrcpc/')
        if [ "${ARMV82}" ]
        then
            DIR="aarch64"
        else
            DIR="aarch64v80compat"
        fi
    elif [ "${ARCH}" = "powerpc64le" -o "${ARCH}" = "ppc64le" ]
    then
        DIR="powerpc64le"
    fi
elif [ "${OS}" = "FreeBSD" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        DIR="freebsd"
    fi
elif [ "${OS}" = "Darwin" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        DIR="macos"
    elif [ "${ARCH}" = "aarch64" -o "${ARCH}" = "arm64" ]
    then
        DIR="macos-aarch64"
    fi
fi

if [ -z "${DIR}" ]
then
    echo "Operating system '${OS}' / architecture '${ARCH}' is unsupported."
    exit 1
fi

URL="https://builds.clickhouse.com/master/${DIR}/clickhouse"
echo
echo "Will download ${URL}"
echo
curl -O "${URL}" && chmod a+x clickhouse || exit 1
echo
echo "Successfully downloaded the ClickHouse binary, you can run it as:
    ./clickhouse"

if [ "${OS}" = "Linux" ]
then
    echo
    echo "You can also install it:
    sudo ./clickhouse install"
fi
