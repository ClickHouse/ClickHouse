#!/bin/sh -e

OS=$(uname -s)
ARCH=$(uname -m)

DIR=

if [ "${OS}" = "Linux" ]
then
    if [ "${ARCH}" = "x86_64" ]
    then
        DIR="amd64"
    elif [ "${ARCH}" = "aarch64" ]
    then
        DIR="aarch64"
    elif [ "${ARCH}" = "powerpc64le" ] || [ "${ARCH}" = "ppc64le" ]
    then
        DIR="powerpc64le"
    fi
elif [ "${OS}" = "FreeBSD" ]
then
    if [ "${ARCH}" = "x86_64" ]
    then
        DIR="freebsd"
    elif [ "${ARCH}" = "aarch64" ]
    then
        DIR="freebsd-aarch64"
    elif [ "${ARCH}" = "powerpc64le" ] || [ "${ARCH}" = "ppc64le" ]
    then
        DIR="freebsd-powerpc64le"
    fi
elif [ "${OS}" = "Darwin" ]
then
    if [ "${ARCH}" = "x86_64" ]
    then
        DIR="macos"
    elif [ "${ARCH}" = "aarch64" -o "${ARCH}" = "arm64" ]
    then
        DIR="macos-aarch64"
    fi
fi

if [ -z "${DIR}" ]
then
    echo "The '${OS}' operating system with the '${ARCH}' architecture is not supported."
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
