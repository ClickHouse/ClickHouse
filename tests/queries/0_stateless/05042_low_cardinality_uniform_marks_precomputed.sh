#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS lc_uniform_marks;

CREATE TABLE lc_uniform_marks
(
    id UInt64,
    s LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 32, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO lc_uniform_marks SELECT number, 'value_' || toString(number % 16) FROM numbers(8192);

OPTIMIZE TABLE lc_uniform_marks FINAL;
"

# One mark per column in a Wide part, so the single-dictionary check for the
# LowCardinality column is answered from the count precomputed when the marks were
# built, instead of scanning every mark of the part.
query_id="$(random_str 10)"

$CLICKHOUSE_CLIENT --query_id="${query_id}" -q "
SELECT count(), sum(length(s)) FROM lc_uniform_marks
"

$CLICKHOUSE_CLIENT -m -q "
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['UniformMarksCheckFromPrecomputedCount'] > 0
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query_id = '${query_id}'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE lc_uniform_marks;
"
