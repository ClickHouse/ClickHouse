#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
SYSTEM DROP FILESYSTEM CACHE;

DROP TABLE IF EXISTS test_02313;
CREATE TABLE test_02313 (id Int32, val String)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS storage_policy = 's3_cache';

INSERT INTO test_02313
    SELECT * FROM
        generateRandom('id Int32, val String')
    LIMIT 100000
SETTINGS enable_filesystem_cache_on_write_operations = 0;
"""

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
SET filesystem_cache_asynchronous_write=1;
SET max_threads=42;
SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null;
"""

function query()
{
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
SET filesystem_cache_asynchronous_write=1;
SET max_threads=42;
SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null;
"""
}

function drop()
{
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP FILESYSTEM CACHE"
}

query &
query &
query &
drop &
query &
drop &
query &
drop &
query &
drop &
query &

drop &
drop &
drop &
drop &
drop &
drop &
drop &
drop &
drop &
drop &


wait
