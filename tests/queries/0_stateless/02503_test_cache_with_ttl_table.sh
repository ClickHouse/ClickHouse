#!/usr/bin/env bash
# Tags: no-fasttest, no-s3-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ttl_on_columns="
CREATE TABLE ttl (d DateTime, a String ttl d, b String ttl d)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY toSecond(d)
SETTINGS min_bytes_for_wide_part = 0, storage_policy = 's3_cache', merge_with_ttl_timeout = 0;
"
ttl_on_table="
CREATE TABLE ttl (d DateTime, a String, b String)
ENGINE = MergeTree ORDER BY toDate(d) PARTITION BY tuple()
TTL d
SETTINGS remove_empty_parts = 1, storage_policy='s3_cache', merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0;
"

function insert()
{
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
INSERT INTO ttl
    SELECT now(), * FROM
        generateRandom('a String, b String')
    LIMIT 10000
SETTINGS enable_filesystem_cache_on_write_operations = 0;
"
}

function query()
{
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ttl FORMAT Null;"
}


for create_table in "$ttl_on_columns" "$ttl_on_table"; do
    ${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
    SYSTEM DROP FILESYSTEM CACHE;
    DROP TABLE IF EXISTS ttl NO DELAY;
    $create_table
    """

    for _ in {1..5}; do insert & done
    for _ in {1..25}; do query & done

    wait

    for _ in {1..10}; do query & done

    sleep 2

    query

    wait
done

${CLICKHOUSE_CLIENT} -q "DROP TABLE ttl NO DELAY;"
