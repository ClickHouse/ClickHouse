#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Filtered reads (row policy) must not populate count-from-files cache with a reduced
# cardinality that later unrestricted counts would reuse.

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_04656;
DROP ROW POLICY IF EXISTS p_04656 ON ${CLICKHOUSE_DATABASE}.t_04656;

CREATE TABLE ${CLICKHOUSE_DATABASE}.t_04656 (x UInt8) ENGINE = File(TSV);
INSERT INTO ${CLICKHOUSE_DATABASE}.t_04656 SELECT number FROM numbers(10);

CREATE ROW POLICY p_04656 ON ${CLICKHOUSE_DATABASE}.t_04656 USING x < 5 TO ALL;

SELECT 'with_policy', count()
FROM ${CLICKHOUSE_DATABASE}.t_04656
SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1;

DROP ROW POLICY p_04656 ON ${CLICKHOUSE_DATABASE}.t_04656;

SELECT 'after_drop_policy', count()
FROM ${CLICKHOUSE_DATABASE}.t_04656
SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1;

DROP TABLE ${CLICKHOUSE_DATABASE}.t_04656;
"
