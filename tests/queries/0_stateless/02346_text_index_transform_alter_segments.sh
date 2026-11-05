#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: ON CLUSTER is not allowed for a Replicated database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The text index transform authorization of an `ALTER` must not reject a statement the executor accepts,
# and an index kept by `IF NOT EXISTS` on the initiator is authorized against the schema the statement produces.

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

$CLICKHOUSE_CLIENT --allow_introspection_functions 1 -n -q "
    CREATE TABLE tab
    (
        id UInt64,
        val String,
        mangled String ALIAS demangle(val),
        lowered String ALIAS lower(val),
        INDEX idx(val) TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id;
"

echo '3. An index kept by IF NOT EXISTS is authorized against the alias the statement renames.'
$CLICKHOUSE_CLIENT --allow_introspection_functions 0 --distributed_ddl_output_mode none -q "
    ALTER TABLE tab ON CLUSTER test_shard_localhost
        RENAME COLUMN mangled TO renamed,
        ADD INDEX IF NOT EXISTS idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = renamed)
" 2>&1 | grep -o -m1 'FUNCTION_NOT_ALLOWED'

echo '4. And it is sent bound to the expansion of that alias.'
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -q "
    ALTER TABLE tab ON CLUSTER test_shard_localhost
        RENAME COLUMN lowered TO renamed_05498,
        ADD INDEX IF NOT EXISTS idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = renamed_05498)
"
$CLICKHOUSE_CLIENT -q "
    SELECT position(query, 'preprocessor = lower(val)') > 0, position(query, 'preprocessor = renamed_05498') > 0
    FROM system.distributed_ddl_queue
    WHERE position(query, '${CLICKHOUSE_DATABASE}') > 0 AND position(query, 'RENAME COLUMN lowered') > 0
    LIMIT 1"

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE seg;
    DROP TABLE seg_text;
    DROP TABLE tab;
"
