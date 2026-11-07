#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the test reads the global counters of system.events.
# Many concurrent queries miss the same keys of CACHE and SSD_CACHE dictionaries with one update thread and a slow source,
# so the update queue holds several units and they are updated in batches. Every value must be correct.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP DICTIONARY IF EXISTS cache_dict_batch;
DROP DICTIONARY IF EXISTS ssd_cache_dict_batch;
DROP DICTIONARY IF EXISTS complex_cache_dict_batch;
DROP VIEW IF EXISTS slow_source_view;
DROP TABLE IF EXISTS source_table;

CREATE TABLE source_table (id UInt64, value String, key_string String) ENGINE = MergeTree ORDER BY id;
INSERT INTO source_table SELECT number, toString(number * 3), toString(number) FROM numbers(20000);

CREATE VIEW slow_source_view AS SELECT id, value, key_string FROM source_table WHERE ignore(sleep(0.05)) = 0;

CREATE DICTIONARY cache_dict_batch (id UInt64, value String DEFAULT 'missing')
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' DB '${CLICKHOUSE_DATABASE}' TABLE 'slow_source_view'))
LIFETIME(MIN 1 MAX 2)
LAYOUT(CACHE(SIZE_IN_CELLS 65536 MAX_THREADS_FOR_UPDATES 1 ALLOW_READ_EXPIRED_KEYS 0));

-- SSD_CACHE does not accept zero layout values, and ALLOW_READ_EXPIRED_KEYS is 0 by default.
-- The write buffer holds all keys, so the test reads no blocks from the file (see https://github.com/ClickHouse/ClickHouse/issues/117991).
CREATE DICTIONARY ssd_cache_dict_batch (id UInt64, value String DEFAULT 'missing')
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' DB '${CLICKHOUSE_DATABASE}' TABLE 'slow_source_view'))
LIFETIME(MIN 1 MAX 2)
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 33554432 WRITE_BUFFER_SIZE 16777216 PATH '${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_ssd_cache_dict_batch' MAX_THREADS_FOR_UPDATES 1));

CREATE DICTIONARY complex_cache_dict_batch (key_string String, value String DEFAULT 'missing')
PRIMARY KEY key_string
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' DB '${CLICKHOUSE_DATABASE}' TABLE 'slow_source_view'))
LIFETIME(MIN 1 MAX 2)
LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 65536 MAX_THREADS_FOR_UPDATES 1 ALLOW_READ_EXPIRED_KEYS 0));
"

function check_simple()
{
    local dictionary=$1
    # All clients request the same keys in the same iteration, so their units share keys and are updated in batches.
    for iteration in {1..20}; do
        # Keys 20000..20099 do not exist in the source and must return the default value.
        $CLICKHOUSE_CLIENT -q "
            SELECT countIf(v != if(k < 20000, toString(k * 3), 'missing')), countIf(h != (k < 20000))
            FROM (
                SELECT (number * 7919 + $iteration) % 20100 AS k,
                       dictGetString('$dictionary', 'value', k) AS v,
                       dictHas('$dictionary', k) AS h
                FROM numbers(3000)
            )"
    done
}

function check_complex()
{
    local seed=$1
    for _ in {1..10}; do
        $CLICKHOUSE_CLIENT -q "
            SELECT countIf(dictGetString('complex_cache_dict_batch', 'value', tuple(toString(k))) != toString(k * 3))
            FROM (SELECT (number * 104729 + $seed) % 20000 AS k FROM numbers(1000))"
    done
}

function duplicate_keys()
{
    $CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM system.events WHERE event = 'DictCacheKeysRequestedDuplicate'"
}

# Runs the clients of one simple key dictionary. The first line is 1 if some units were updated in a batch with
# shared keys, and each next line is the number of wrong values of one query, which must be 0.
function check_simple_clients()
{
    local dictionary=$1 clients=$2
    local before
    before=$(duplicate_keys)
    for i in $(seq "$clients"); do
        check_simple "$dictionary" > "${CLICKHOUSE_TMP}/batch_${dictionary}_$i.out" 2>&1 &
    done
    wait
    echo $(( $(duplicate_keys) > before ))
    cat "${CLICKHOUSE_TMP}"/batch_"${dictionary}"_*.out | sort | uniq -c | sed -E 's/^ *[0-9]+ //'
}

check_simple_clients cache_dict_batch 8
check_simple_clients ssd_cache_dict_batch 4

for i in {1..2}; do
    check_complex "$i" > "${CLICKHOUSE_TMP}/batch_complex_$i.out" 2>&1 &
done
wait
cat "${CLICKHOUSE_TMP}"/batch_complex_*.out | sort | uniq -c | sed -E 's/^ *[0-9]+ //'

$CLICKHOUSE_CLIENT -m -q "
DROP DICTIONARY cache_dict_batch;
DROP DICTIONARY ssd_cache_dict_batch;
DROP DICTIONARY complex_cache_dict_batch;
DROP VIEW slow_source_view;
DROP TABLE source_table;
"
