#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline --query "
DROP TABLE IF EXISTS test_map_case_insensitive;

CREATE TABLE test_map_case_insensitive
(
    id UInt64,
    value Map(String, String),
    index value_key_index mapKeys(value) TYPE tokenbf_v1(1024, 4, 0) GRANULARITY 1,
    index value_value_index arrayMap(x -> lower(x), mapValues(value)) TYPE ngrambf_v1(4, 60000, 5, 0) GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_map_case_insensitive
SELECT number, map(toString(number), toString(number))
FROM numbers(1000000);

SELECT count() FROM test_map_case_insensitive WHERE (value['10000']) = lower('123456');

DROP TABLE test_map_case_insensitive;
"

${CLICKHOUSE_CLIENT} --multiline --query "
DROP TABLE IF EXISTS test_map_case_insensitive;

CREATE TABLE test_map_case_insensitive
(
    id UInt64,
    value Map(String, String),
    index value_key_index mapKeys(value) TYPE tokenbf_v1(1024, 4, 0) GRANULARITY 1,
    index value_value_index arrayMap(x -> lower(x), mapValues(value)) TYPE ngrambf_v1(4, 60000, 5, 0) GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_map_case_insensitive
SELECT number, map(toString(number), toString(number))
FROM numbers(1000000);
"

${CLICKHOUSE_CLIENT} --multiline --query "
EXPLAIN indexes = 1
SELECT *
FROM test_map_case_insensitive
WHERE lower(value['10000']) = lower('123456');" 2>&1 | grep -cF 'value_value_index'

${CLICKHOUSE_CLIENT} --multiline --query "
DROP TABLE test_map_case_insensitive;
"
