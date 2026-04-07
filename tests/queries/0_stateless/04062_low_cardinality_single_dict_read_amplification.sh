#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Regression test for repeated decompression of a single per-part LowCardinality dictionary.
# When low_cardinality_use_single_dictionary_for_part=1, a single dictionary is written for
# the whole part. Previously, any non-continuous read (mark range skip caused by a WHERE filter)
# would re-decompress the dictionary, causing read amplification proportional to the number of
# disjoint mark ranges touched by the query.
# --
# Here we test without low_cardinality_use_single_dictionary_for_part as it requires server
# modification to apply. The smaller dictionary requires a significant number of rows.

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS lc_single_dict_repro;

CREATE TABLE lc_single_dict_repro
(
    id UInt128,
    stack Array(LowCardinality(String)) CODEC(NONE),
    filter UInt8
)
ENGINE = MergeTree
PRIMARY KEY (id, stack)
SETTINGS index_granularity = 32, min_bytes_for_wide_part = 0;

INSERT INTO lc_single_dict_repro
    SELECT
        number,
        -- 8000 long and unique entries fit into a single dictionary.
        arrayMap(i -> repeat('8 bytes|', 16) || toString(number % 8000), range(20)),
        if(intDiv(number, 32) % 2 = 0, 1, 0)
    FROM numbers(128 * 1024);

OPTIMIZE TABLE lc_single_dict_repro FINAL;
"

$CLICKHOUSE_CLIENT -q "
-- Count the distinct offsets, <= 2 means one dict
SELECT countDistinct(stack.dict.mark) <= 2 has_one_dict
FROM mergeTreeIndex(currentDatabase(), lc_single_dict_repro, with_marks = true)
WHERE part_name = (
    SELECT name
    FROM system.parts
    WHERE (database = currentDatabase()) AND (table = 'lc_single_dict_repro') AND active
    ORDER BY name ASC
    LIMIT 1
)
"

query_id="$(random_str 10)"

$CLICKHOUSE_CLIENT --query_id="${query_id}" -q "
-- Correctness: selective query should return the right count and sum.
SELECT
    count(),
    sum(arraySum(frame -> length(frame), stack))
FROM lc_single_dict_repro
PREWHERE filter = 1
"

$CLICKHOUSE_CLIENT -m -q "
-- Read amplification check.
-- We assert: read data size < 8 * data_compressed_bytes.
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ReadCompressedBytes'] < 8 * (
    SELECT sum(data_compressed_bytes)
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'lc_single_dict_repro' AND active
)
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query_id='${query_id}'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE lc_single_dict_repro;
"

$CLICKHOUSE_CLIENT -m -q "
-- Checking correctness for multiple dicts
CREATE TABLE lc_multi_dict
(
    id UInt128,
    stack Array(LowCardinality(String)) CODEC(NONE),
    tuple Tuple(a LowCardinality(String), b LowCardinality(String)) CODEC(NONE),
    nested Nested(c LowCardinality(String), d LowCardinality(String)) CODEC(NONE),
    filter UInt8
)
ENGINE = MergeTree
PRIMARY KEY (id, stack)
SETTINGS index_granularity = 32, min_bytes_for_wide_part = 0;

INSERT INTO lc_multi_dict
    SELECT
        rand64(),
        -- 80000 unique entries do not fit into a single dictionary.
        arrayMap(i -> repeat('8 bytes|', 16) || toString(number % 80000), range(20)),
        -- multiple long dicts for a, single short dict for b
        ('long long looooong value for a' || toString(number % 80000), 'short for b' || toString(number % 80)),
        -- multiple long dicts for c, single short dict for d
        ['long long looooong value for cc' || toString(number % 80000)],
        ['short for dd' || toString(number % 80)],
        if(intDiv(number, 32) % 2 = 0, 1, 0)
    FROM numbers(128 * 1024);

OPTIMIZE TABLE lc_multi_dict FINAL;
"

$CLICKHOUSE_CLIENT -q "
-- Count the distinct offsets, > 2 means multiple dicts
SELECT
    -- you'd expect single dict b and d, but they in fact have many marks
    countDistinct(stack.dict.mark) > 2 AS has_many_dicts,
    countDistinct(\`tuple%2Ea.mark\`) > 2 AS has_many_dicts_a,
    countDistinct(\`tuple%2Eb.mark\`) > 2 AS has_many_dicts_b,
    countDistinct(\`nested%2Ec.mark\`) > 2 AS has_many_dicts_c,
    countDistinct(\`nested%2Ed.mark\`) > 2 AS has_many_dicts_d
FROM mergeTreeIndex(currentDatabase(), lc_multi_dict, with_marks = true)
WHERE part_name = (
    SELECT name
    FROM system.parts
    WHERE (database = currentDatabase()) AND (table = 'lc_multi_dict') AND active
    ORDER BY name ASC
    LIMIT 1
)
"

$CLICKHOUSE_CLIENT -m -q "
SELECT
    count(),
    sum(arraySum(frame -> length(frame), stack)),
    sum(length(tuple.a)),
    sum(length(tuple.b)),
    sum(arraySum(c -> length(c), nested.c)),
    sum(arraySum(d -> length(d), nested.d))
FROM lc_multi_dict
PREWHERE filter = 1;

DROP TABLE lc_multi_dict;
"
