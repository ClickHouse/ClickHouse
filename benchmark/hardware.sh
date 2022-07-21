#!/bin/bash -e

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

./clickhouse server >/dev/null 2>&1 &
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

if [[ $(./clickhouse client --query "EXISTS hits") == '1' && $(./clickhouse client --query "SELECT count() FROM hits") == '100000000' ]]; then
    echo "Dataset already downloaded"
else
    echo "Will download the dataset"
    if [ "`uname`" = "Darwin" ]
    then
    ./clickhouse client --receive_timeout 1000 --max_insert_threads $(sysctl -n hw.ncpu) --progress --query "
        CREATE OR REPLACE TABLE hits ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime)
        AS SELECT * FROM url('https://datasets.clickhouse.com/hits/native/hits_100m_obfuscated_{0..255}.native.zst')"
    else
    ./clickhouse client --receive_timeout 1000 --max_insert_threads $(nproc || 4) --progress --query "
        CREATE OR REPLACE TABLE hits ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime)
        AS SELECT * FROM url('https://datasets.clickhouse.com/hits/native/hits_100m_obfuscated_{0..255}.native.zst')"
    fi
    ./clickhouse client --query "SELECT 'The dataset size is: ', count() FROM hits"
fi

if [[ $(./clickhouse client --query "SELECT count() FROM system.parts WHERE table = 'hits' AND database = 'default' AND active") == '1' ]]; then
    echo "Dataset already prepared"
else
    echo "Will prepare the dataset"
    ./clickhouse client --receive_timeout 1000 --query "OPTIMIZE TABLE hits FINAL"
fi

echo
echo "Will perform benchmark. Results:"
echo

>result.csv
QUERY_NUM=1

cat "$QUERIES_FILE" | sed "s/{table}/hits/g" | while read query; do
    sync
    if [ "`uname`" = "Darwin" ]
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

        echo "${QUERY_NUM},${i},${RES}" >> result.csv
    done
    echo "],"

    QUERY_NUM=$((QUERY_NUM + 1))
done


echo
echo "Benchmark complete. System info:"
echo

touch {cpu_model,cpu,df,memory,memory_total,blk,mdstat,instance}.txt

if [ "`uname`" = "Darwin" ]
then
    echo '----Version, build id-----------'
    ./clickhouse local --query "SELECT format('Version: {}', version())"
    ./clickhouse local --query "SELECT format('The number of threads is: {}', value) FROM system.settings WHERE name = 'max_threads'" --output-format TSVRaw
    ./clickhouse local --query "SELECT format('Current time: {}', toString(now(), 'UTC'))"
    echo '----CPU-------------------------'
    sysctl hw.model | tee cpu_model.txt
    sysctl -a | grep -E 'hw.activecpu|hw.memsize|hw.byteorder|cachesize' | tee cpu.txt
    echo '----Disk Free and Total--------'
    df -h . | tee df.txt
    echo '----Memory Free and Total-------'
    vm_stat | tee memory.txt
    echo '----Physical Memory Amount------'
    ls -l /var/vm | tee memory_total.txt
    echo '--------------------------------'
else
    echo '----Version, build id-----------'
    ./clickhouse local --query "SELECT format('Version: {}, build id: {}', version(), buildId())"
    ./clickhouse local --query "SELECT format('The number of threads is: {}', value) FROM system.settings WHERE name = 'max_threads'" --output-format TSVRaw
    ./clickhouse local --query "SELECT format('Current time: {}', toString(now(), 'UTC'))"
    echo '----CPU-------------------------'
    cat /proc/cpuinfo | grep -i -F 'model name' | uniq | tee cpu_model.txt
    lscpu | tee cpu.txt
    echo '----Block Devices---------------'
    lsblk | tee blk.txt
    echo '----Disk Free and Total--------'
    df -h . | tee df.txt
    echo '----Memory Free and Total-------'
    free -h | tee memory.txt
    echo '----Physical Memory Amount------'
    cat /proc/meminfo | grep MemTotal | tee memory_total.txt
    echo '----RAID Info-------------------'
    cat /proc/mdstat| tee mdstat.txt
    echo '--------------------------------'
fi
echo

echo "Instance type from IMDS (if available):"
curl -s --connect-timeout 1 'http://169.254.169.254/latest/meta-data/instance-type' | tee instance.txt
echo

echo "Uploading the results (if possible)"

UUID=$(./clickhouse local --query "SELECT generateUUIDv4()")

./clickhouse local --query "
  SELECT
    '${UUID}' AS run_id,
    version() AS version,
    now() AS test_time,
    (SELECT value FROM system.settings WHERE name = 'max_threads') AS threads,
    filesystemCapacity() AS fs_capacity,
    filesystemAvailable() AS fs_available,
    file('cpu_model.txt') AS cpu_model,
    file('cpu.txt') AS cpu,
    file('df.txt') AS df,
    file('memory.txt') AS memory,
    file('memory_total.txt') AS memory_total,
    file('blk.txt') AS blk,
    file('mdstat.txt') AS mdstat,
    file('instance.txt') AS instance
" | tee meta.tsv | ./clickhouse client --host play.clickhouse.com --secure --user benchmark --query "
  INSERT INTO benchmark_runs
  (run_id, version, test_time, threads, fs_capacity, fs_available, cpu_model, cpu, df, memory, memory_total, blk, mdstat, instance)
  FORMAT TSV" || echo "Cannot upload results."

./clickhouse local --query "
  SELECT
    '${UUID}' AS run_id,
    c1 AS query_num,
    c2 AS try_num,
    c3 AS time
  FROM file('result.csv')
" | tee results.tsv | ./clickhouse client --host play.clickhouse.com --secure --user benchmark --query "
  INSERT INTO benchmark_results
  (run_id, query_num, try_num, time)
  FORMAT TSV" || echo "Cannot upload results. Please send the output to feedback@clickhouse.com"

<<////

Server Setup:

CREATE TABLE benchmark_runs
(
    run_id UUID,
    version String,
    test_time DateTime,
    threads String,
    fs_capacity UInt64,
    fs_available UInt64,
    cpu_model String,
    cpu String,
    df String,
    memory String,
    memory_total String,
    blk String,
    mdstat String,
    instance String
) ENGINE = ReplicatedMergeTree ORDER BY run_id;

CREATE TABLE benchmark_results
(
    run_id UUID,
    query_num UInt8,
    try_num UInt8,
    time Decimal32(3)
) ENGINE = ReplicatedMergeTree ORDER BY (run_id, query_num, try_num);

CREATE USER benchmark IDENTIFIED WITH no_password SETTINGS max_rows_to_read = 1, max_result_rows = 1, max_execution_time = 1;

CREATE QUOTA benchmark
KEYED BY ip_address
FOR RANDOMIZED INTERVAL 1 MINUTE MAX query_inserts = 4, written_bytes = 100000,
FOR RANDOMIZED INTERVAL 1 HOUR MAX query_inserts = 10, written_bytes = 500000,
FOR RANDOMIZED INTERVAL 1 DAY MAX query_inserts = 50, written_bytes = 2000000
TO benchmark;

GRANT INSERT ON benchmark_runs TO benchmark;
GRANT INSERT ON benchmark_results TO benchmark;

Example query:

SELECT
    cpu_model,
    threads,
    instance,
    k
FROM
(
    SELECT
        run_id,
        exp(avg(log(adjusted_time / best_time))) AS k
    FROM
    (
        WITH greatest(time, 0.01) AS adjusted_time
        SELECT
            run_id,
            adjusted_time,
            min(adjusted_time) OVER (PARTITION BY query_num, try_num) AS best_time
        FROM benchmark_results
        WHERE try_num > 1
    )
    GROUP BY run_id
    ORDER BY k ASC
) AS t
INNER JOIN benchmark_runs USING (run_id)

////
