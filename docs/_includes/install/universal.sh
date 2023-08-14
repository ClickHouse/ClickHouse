#!/bin/sh -e

OS=$(uname -s)
ARCH=$(uname -m)

DIR=

if [ "${OS}" = "Linux" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        # Require at least x86-64 + SSE4.2 (introduced in 2006). On older hardware fall back to plain x86-64 (introduced in 1999) which
        # guarantees at least SSE2. The caveat is that plain x86-64 builds are much less tested than SSE 4.2 builds.
        HAS_SSE42=$(grep sse4_2 /proc/cpuinfo)
        if [ "${HAS_SSE42}" ]
        then
            DIR="amd64"
        else
            DIR="amd64compat"
        fi
    elif [ "${ARCH}" = "aarch64" -o "${ARCH}" = "arm64" ]
    then
        # If the system has >=ARMv8.2 (https://en.wikipedia.org/wiki/AArch64), choose the corresponding build, else fall back to a v8.0
        # compat build. Unfortunately, the ARM ISA level cannot be read directly, we need to guess from the "features" in /proc/cpuinfo.
        # Also, the flags in /proc/cpuinfo are named differently than the flags passed to the compiler (cmake/cpu_features.cmake).
        HAS_ARMV82=$(grep -m 1 'Features' /proc/cpuinfo | awk '/asimd/ && /sha1/ && /aes/ && /atomics/ && /lrcpc/')
        if [ "${HAS_ARMV82}" ]
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

clickhouse_download_filename_prefix="clickhouse"
clickhouse="$clickhouse_download_filename_prefix"

if [ -f "$clickhouse" ]
then
    read -p "ClickHouse binary ${clickhouse} already exists. Overwrite? [y/N] " answer
    if [ "$answer" = "y" -o "$answer" = "Y" ]
    then
        rm -f "$clickhouse"
    else
        i=0
        while [ -f "$clickhouse" ]
        do
            clickhouse="${clickhouse_download_filename_prefix}.${i}"
            i=$(($i+1))
        done
    fi
fi

URL="https://builds.clickhouse.com/master/${DIR}/clickhouse"
echo
echo "Will download ${URL} into ${clickhouse}"
echo
curl "${URL}" -o "${clickhouse}" && chmod a+x "${clickhouse}" || exit 1
echo
echo "Successfully downloaded the ClickHouse binary, you can run it as:
    ./${clickhouse}"

if [ "${OS}" = "Linux" ]
then
    echo
    echo "You can also install it:
    sudo ./${clickhouse} install"
fi
