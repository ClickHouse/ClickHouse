#!/bin/sh -e

OS=$(uname -s)
ARCH=$(uname -m)

DIR=

if [ "${OS}" = "Linux" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        # The default build targets x86-64-v3 which requires AVX2, BMI1, BMI2, FMA, etc.
        # On older hardware, fall back to the compat build (plain x86-64, SSE2 baseline).
        # Check avx2 as a proxy, since every real CPU with AVX2 also has the other v3 features.
        if grep -q avx2 /proc/cpuinfo
        then
            if ldd --version 2>&1 | grep -q musl
            then
                DIR="amd64musl"
            else
                DIR="amd64"
            fi
        else
            DIR="amd64compat"
        fi
    elif [ "${ARCH}" = "aarch64" -o "${ARCH}" = "arm64" ]
    then
        # Dispatch between standard and compatibility builds, see cmake/cpu_features.cmake for details. Unfortunately, (1) the ARM ISA level
        # cannot be read directly, we need to guess from the "features" in /proc/cpuinfo, and (2) the flags in /proc/cpuinfo are named
        # differently than the flags passed to the compiler in cpu_features.cmake.
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
    elif [ "${ARCH}" = "riscv64" ]
    then
        DIR="riscv64"
    elif [ "${ARCH}" = "s390x" ]
    then
        DIR="s390x"
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

echo
echo "You can also install it:
sudo ./${clickhouse} install"
