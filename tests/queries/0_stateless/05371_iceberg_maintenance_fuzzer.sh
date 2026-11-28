#!/usr/bin/env bash
# Random sequences of writes and maintenance on an Iceberg table, checked after every step against a
# MergeTree table that receives the same writes. Seeds are fixed, so a failure names a reproducible sequence.
# A live file deleted by maintenance shows up as a read error in the comparison (stderr is captured).
#
# Tags: no-fasttest, long
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - long: dozens of commits per run, slow under sanitizers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading format version 3 metadata logs a spurious "violates Iceberg specification" warning,
# https://github.com/ClickHouse/ClickHouse/issues/125333
CH="${CLICKHOUSE_CLIENT} --send_logs_level=error --allow_insert_into_iceberg=1 --allow_iceberg_remove_orphan_files=1 --iceberg_orphan_files_min_retention_seconds=0 --allow_experimental_expire_snapshots=1 --mutations_sync=2"

# A fixed generator rather than $RANDOM, whose sequence depends on the bash version.
next_random()
{
    state=$(( (state * 1103515245 + 12345) % 2147483648 ))
    random=$(( state / 65536 % $1 ))
}

for format_version in 2 3; do
    for seed in 1; do
        state=${seed}
        TABLE="t_${CLICKHOUSE_DATABASE}_v${format_version}_${seed}"
        TABLE_PATH="${USER_FILES_PATH}/${TABLE}"
        $CH --query "
            CREATE TABLE ${TABLE} (id Int64, p Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') PARTITION BY p
            SETTINGS iceberg_format_version = ${format_version}, allow_experimental_iceberg_compaction = 0;
            CREATE TABLE ${TABLE}_expected (id Int64, p Int64) ENGINE = MergeTree ORDER BY id;"

        next_id=0
        steps=""
        for step in $(seq 20); do
            # DELETE and DROP PARTITION are supported only for format version 2.
            next_random $((format_version == 2 ? 7 : 5))
            op=${random}
            case ${op} in
                0|1)
                    next_random 5
                    rows=$((random + 1))
                    values="SELECT number, number % 3 FROM numbers(${next_id}, ${rows})"
                    $CH --query "INSERT INTO ${TABLE} ${values}"
                    $CH --query "INSERT INTO ${TABLE}_expected ${values}"
                    next_id=$((next_id + rows))
                    steps+=" insert:${rows}";;
                2)
                    next_random 3
                    retain=$((random + 1))
                    $CH --query "ALTER TABLE ${TABLE} EXECUTE expire_snapshots(retention_period = '1ms', retain_last = ${retain})" > /dev/null
                    steps+=" expire:${retain}";;
                3)
                    mkdir -p "${TABLE_PATH}/data"
                    echo "junk" > "${TABLE_PATH}/data/junk-${step}.parquet"
                    TZ=UTC touch -t 202001010000 "${TABLE_PATH}/data/junk-${step}.parquet"
                    steps+=" junk";;
                4)
                    $CH --query "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files('$($CH --query "SELECT toString(now())")')" > /dev/null
                    steps+=" remove_orphans";;
                5)
                    next_random 4
                    key=${random}
                    $CH --query "ALTER TABLE ${TABLE} DELETE WHERE id % 4 = ${key}"
                    $CH --query "ALTER TABLE ${TABLE}_expected DELETE WHERE id % 4 = ${key}"
                    steps+=" delete:${key}";;
                6)
                    next_random 3
                    partition=${random}
                    $CH --query "ALTER TABLE ${TABLE} DROP PARTITION ${partition}"
                    $CH --query "ALTER TABLE ${TABLE}_expected DELETE WHERE p = ${partition}"
                    steps+=" drop_partition:${partition}";;
            esac
            actual=$($CH --query "SELECT groupArray((id, p)) FROM (SELECT * FROM ${TABLE} ORDER BY id)" 2>&1)
            expected=$($CH --query "SELECT groupArray((id, p)) FROM (SELECT * FROM ${TABLE}_expected ORDER BY id)")
            if [ "${actual}" != "${expected}" ]; then
                echo "format version ${format_version}, seed ${seed}, steps:${steps}"
                echo "expected ${expected}, got ${actual}"
                break
            fi
        done

        $CH --query "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files('$($CH --query "SELECT toString(now())")')" > /dev/null
        echo "format version ${format_version}, seed ${seed}: $(find "${TABLE_PATH}/data" -name 'junk-*' | wc -l | tr -d ' ') junk files left"
        $CH --query "DROP TABLE ${TABLE}; DROP TABLE ${TABLE}_expected"
        rm -rf "${TABLE_PATH}"
    done
done
