#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Filtered reads (row policy) must not populate count-from-files cache with a reduced
# cardinality that later unrestricted counts would reuse.

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_05292;
DROP ROW POLICY IF EXISTS p_05292 ON ${CLICKHOUSE_DATABASE}.t_05292;

CREATE TABLE ${CLICKHOUSE_DATABASE}.t_05292 (x UInt8) ENGINE = File(TSV);
INSERT INTO ${CLICKHOUSE_DATABASE}.t_05292 SELECT number FROM numbers(10);

CREATE ROW POLICY p_05292 ON ${CLICKHOUSE_DATABASE}.t_05292 USING x < 5 TO ALL;

SELECT 'with_policy', count()
FROM ${CLICKHOUSE_DATABASE}.t_05292
SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1;

DROP ROW POLICY p_05292 ON ${CLICKHOUSE_DATABASE}.t_05292;

SELECT 'after_drop_policy', count()
FROM ${CLICKHOUSE_DATABASE}.t_05292
SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1;

DROP TABLE ${CLICKHOUSE_DATABASE}.t_05292;
"
