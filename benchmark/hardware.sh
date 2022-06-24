#!/bin/bash -e

TABLE="hits_100m_obfuscated"
QUERIES_FILE="queries.sql"
TRIES=3

mkdir -p clickhouse-benchmark
pushd clickhouse-benchmark

# Download the binary
if [[ ! -x clickhouse ]]; then
    curl https://clickhouse.com/ | sh
fi

if [[ ! -f $QUERIES_FILE ]]; then
    wget "https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/$QUERIES_FILE"
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
    ./clickhouse client --query "SELECT 'Ok.'" 2>/dev/null && break || echo -n '.'
    if [[ $i == 30 ]]; then exit 1; fi
done

echo "Will download the dataset"
./clickhouse client --max_insert_threads $(nproc || 4) --progress --query "
  CREATE OR REPLACE TABLE ${TABLE} ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime)
  AS SELECT * FROM url('https://datasets.clickhouse.com/hits/native/hits_100m_obfuscated_{0..255}.native.zst')"

./clickhouse client --query "SELECT 'The dataset size is: ', count() FROM ${TABLE}"

echo "Will prepare the dataset"
./clickhouse client --query "OPTIMIZE TABLE ${TABLE} FINAL"

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
        RES=$(./clickhouse client --time --format=Null --query="$query" 2>&1 ||:)
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
    echo '--------------------------------'
fi
echo

echo "Instance type from IMDS (if available):"
curl --connect-timeout 1 http://169.254.169.254/latest/meta-data/instance-type
echo
