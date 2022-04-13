#!/bin/bash -e

if [[ -n $1 ]]; then
    SCALE=$1
else
    SCALE=100
fi

TABLE="hits_${SCALE}m_obfuscated"
DATASET="${TABLE}_v1.tar.xz"
QUERIES_FILE="queries.sql"
TRIES=3

# Note: on older Ubuntu versions, 'axel' does not support IPv6. If you are using IPv6-only servers on very old Ubuntu, just don't install 'axel'.

FASTER_DOWNLOAD=wget
if command -v axel >/dev/null; then
    FASTER_DOWNLOAD=axel
else
    echo "It's recommended to install 'axel' for faster downloads."
fi

if command -v pixz >/dev/null; then
    TAR_PARAMS='-Ipixz'
else
    echo "It's recommended to install 'pixz' for faster decompression of the dataset."
fi

mkdir -p clickhouse-benchmark-$SCALE
pushd clickhouse-benchmark-$SCALE

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
    elif [ "${ARCH}" = "powerpc64le" ]
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
    elif [ "${ARCH}" = "powerpc64le" ]
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
echo "Successfully downloaded the ClickHouse binary"

chmod a+x clickhouse

if [[ ! -f $QUERIES_FILE ]]; then
    wget "https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/$QUERIES_FILE"
fi

if [[ ! -d data ]]; then
    if [[ ! -f $DATASET ]]; then
        $FASTER_DOWNLOAD "https://datasets.clickhouse.com/hits/partitions/$DATASET"
    fi

    tar $TAR_PARAMS --strip-components=1 --directory=. -x -v -f $DATASET
fi

uptime

echo "Starting clickhouse-server"

./clickhouse server > server.log 2>&1 &
PID=$!

function finish {
    kill $PID
    wait
}
trap finish EXIT

echo "Waiting for clickhouse-server to start"

for i in {1..30}; do
    sleep 1
    ./clickhouse client --query "SELECT 'The dataset size is: ', count() FROM $TABLE" 2>/dev/null && break || echo '.'
    if [[ $i == 30 ]]; then exit 1; fi
done

echo
echo "Will perform benchmark. Results:"
echo

cat "$QUERIES_FILE" | sed "s/{table}/${TABLE}/g" | while read query; do
    sync
    if [ "${OS}" = "Darwin" ] 
    then 
        sudo purge > /dev/null
    else
        echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
    fi

    echo -n "["
    for i in $(seq 1 $TRIES); do
        RES=$(./clickhouse client --max_memory_usage 100G --time --format=Null --query="$query" 2>&1 ||:)
        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done


echo
echo "Benchmark complete. System info:"
echo

if [ "${OS}" = "Darwin" ] 
then 
    echo '----Version, build id-----------'
    ./clickhouse local --query "SELECT format('Version: {}', version())"
    sw_vers | grep BuildVersion
    ./clickhouse local --query "SELECT format('The number of threads is: {}', value) FROM system.settings WHERE name = 'max_threads'" --output-format TSVRaw
    ./clickhouse local --query "SELECT format('Current time: {}', toString(now(), 'UTC'))"
    echo '----CPU-------------------------'
    sysctl hw.model 
    sysctl -a | grep -E 'hw.activecpu|hw.memsize|hw.byteorder|cachesize'
    echo '----Disk Free and Total--------'
    df -h .
    echo '----Memory Free and Total-------'
    vm_stat
    echo '----Physical Memory Amount------'
    ls -l /var/vm
    echo '--------------------------------'
else
    echo '----Version, build id-----------'
    ./clickhouse local --query "SELECT format('Version: {}, build id: {}', version(), buildId())"
    ./clickhouse local --query "SELECT format('The number of threads is: {}', value) FROM system.settings WHERE name = 'max_threads'" --output-format TSVRaw
    ./clickhouse local --query "SELECT format('Current time: {}', toString(now(), 'UTC'))"
    echo '----CPU-------------------------'
    cat /proc/cpuinfo | grep -i -F 'model name' | uniq
    lscpu
    echo '----Block Devices---------------'
    lsblk
    echo '----Disk Free and Total--------'
    df -h .
    echo '----Memory Free and Total-------'
    free -h
    echo '----Physical Memory Amount------'
    cat /proc/meminfo | grep MemTotal
    echo '----RAID Info-------------------'
    cat /proc/mdstat
    #echo '----PCI-------------------------'
    #lspci
    #echo '----All Hardware Info-----------'
    #lshw
    echo '--------------------------------'
fi
echo
