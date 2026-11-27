#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: a single `ALTER` of several types is prohibited there, which is exactly what this test needs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The text index transform authorization of an `ALTER` must not reject a statement the executor accepts.

echo '1. An ordinary index added after a mutation segment, with a rename in each metadata segment.'
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE seg (id UInt64, a String, m UInt64 MATERIALIZED 1) ENGINE = MergeTree ORDER BY id;
    ALTER TABLE seg
        RENAME COLUMN a TO b,
        MATERIALIZE COLUMN m,
        RENAME COLUMN b TO c,
        ADD INDEX idx(c) TYPE minmax GRANULARITY 1;
    SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'seg';
"

echo '2. The same with a text index transform, on the local host.'
$CLICKHOUSE_CLIENT -n -q "
    SET enable_full_text_index = 1;
    CREATE TABLE seg_text (id UInt64, a String, m UInt64 MATERIALIZED 1) ENGINE = MergeTree ORDER BY id;
    ALTER TABLE seg_text
        RENAME COLUMN a TO b,
        MATERIALIZE COLUMN m,
        RENAME COLUMN b TO c,
        ADD INDEX idx_text(c) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(c));
    SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'seg_text';
"

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE seg;
    DROP TABLE seg_text;
"
