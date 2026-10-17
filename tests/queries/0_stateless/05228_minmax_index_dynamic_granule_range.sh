#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel-replicas
# no-ordinary-database: a full-definition ATTACH needs an explicit UUID, which Ordinary rejects.
# no-replicated-database: a Replicated database rejects an explicit UUID in ATTACH.
# no-parallel-replicas: EXPLAIN indexes = 1 gains a per-node Granules block, and
# use_skip_indexes_on_data_read is not supported with parallel replicas.

# A full-definition ATTACH always logs a <Warning> recommending the short form. This test asserts
# counts, not warnings, so keep it off stderr; real errors still reach it.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# CREATE and ALTER refuse a minmax index over Dynamic, so the only way to reach the read side is
# ATTACH, which skips index validation. Startup metadata load takes the same path, which is how
# tables created before the validator grew its nested arm still load today. A full-definition
# ATTACH needs an explicit UUID: generate one per table so the test stays parallel-safe.
UUID_DYN=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
UUID_NULL=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
UUID_ARR=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
UUID_MC=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# Auto statistics can drop a whole part before any skip index is read, which would make the
# assertions below measure something other than the skip index.
#
# Every table pins index_granularity, index_granularity_bytes and min_bytes_for_wide_part in its
# DDL so the granule counts asserted through EXPLAIN are stable under merge-tree settings
# randomization, and orders by tuple() so the primary key never prunes. 64 rows give 16 granules.
#
# The runner redraws secondary_indices_enable_bulk_filtering and use_skip_indexes_on_data_read per
# test run, so every arm that reads the index pins all three skip-index settings; only the row
# labelled 'on data read' asks for use_skip_indexes_on_data_read = 1.
# Both values of secondary_indices_enable_bulk_filtering are asserted because the runner randomizes
# it, not because minmax has a bulk path of its own: it implements none, so the row labelled
# 'default' (that setting's default, 1) reads the index exactly as the row above it does.
$CLICKHOUSE_CLIENT --multiquery -q "
SET use_statistics_for_part_pruning = 0;

ATTACH TABLE t_dyn UUID '${UUID_DYN}' (k UInt64, d Dynamic, INDEX idx d TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_dyn SELECT number, number FROM numbers(64);

ATTACH TABLE t_dyn_null UUID '${UUID_NULL}' (k UInt64, d Dynamic, INDEX idx d TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_dyn_null SELECT number, if(number % 2 = 0, NULL, number) FROM numbers(64);

ATTACH TABLE t_arr UUID '${UUID_ARR}' (k UInt64, d Array(Dynamic), INDEX idx d TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_arr SELECT number, [number::Dynamic] FROM numbers(64);

-- The flagged column is FIRST so a read that skipped its bounds instead of consuming them would take
-- u's bounds out of d's bytes: both bounds of every column go consecutively into one stream.
ATTACH TABLE t_multi UUID '${UUID_MC}'
    (k UInt64, d Dynamic, u UInt64, INDEX idx (d, u) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_multi SELECT number, number, number FROM numbers(64);

CREATE TABLE t_uint (k UInt64, u UInt64, INDEX idx u TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_uint SELECT number, number FROM numbers(64);

CREATE TABLE t_nullable (k UInt64, n Nullable(UInt64), INDEX idx n TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_nullable SELECT number, if(number % 2 = 0, NULL, number) FROM numbers(64);

SELECT '-- 1. Dynamic column, equality atom';
SELECT 'no index      ', count() FROM t_dyn WHERE d = 3 SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_dyn WHERE d = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'default       ', count() FROM t_dyn WHERE d = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 1, use_skip_indexes_on_data_read = 0;
SELECT 'granules 16/16', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dyn WHERE d = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0) WHERE explain LIKE '%Granules: 16/16%';

SELECT '-- 2. Dynamic column, range atom';
SELECT 'no index      ', count() FROM t_dyn WHERE d > 10 SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_dyn WHERE d > 10
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;

SELECT '-- 3. Dynamic column holding NULLs. IS NOT NULL loses rows; IS NULL never did, and must not start';
SELECT 'not null no idx', count() FROM t_dyn_null WHERE d IS NOT NULL SETTINGS use_skip_indexes = 0;
SELECT 'not null granul', count() FROM t_dyn_null WHERE d IS NOT NULL
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'is null no idx ', count() FROM t_dyn_null WHERE d IS NULL SETTINGS use_skip_indexes = 0;
SELECT 'is null granule', count() FROM t_dyn_null WHERE d IS NULL
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;

SELECT '-- 4. Nested Dynamic in an Array';
SELECT 'no index      ', count() FROM t_arr WHERE d >= [0::Dynamic] SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_arr WHERE d >= [0::Dynamic]
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;

-- 'd kept' / 'd granules 16/16' are this arm's defect rows; 'u prunes' / 'u granules 1/16' hold on an
-- unfixed server too and are decode-integrity controls for the stream u's bounds are read from.
SELECT '-- 5. Multi-column index, flagged column first';
SELECT 'no index        ', count() FROM t_multi WHERE u = 3 SETTINGS use_skip_indexes = 0;
SELECT 'u prunes        ', count() FROM t_multi WHERE u = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'u granules 1/16 ', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multi WHERE u = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0) WHERE explain LIKE '%Granules: 1/16%';
SELECT 'd kept          ', count() FROM t_multi WHERE d = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'd granules 16/16', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multi WHERE d = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0) WHERE explain LIKE '%Granules: 16/16%';

SELECT '-- 6. Dynamic column on the data-read path';
SELECT 'no index      ', count() FROM t_dyn WHERE d = 3 SETTINGS use_skip_indexes = 0;
SELECT 'on data read  ', count() FROM t_dyn WHERE d = 3
    SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT '-- 7. Control: UInt64 must still prune';
SELECT 'no index      ', count() FROM t_uint WHERE u = 3 SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_uint WHERE u = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'granules 1/16 ', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_uint WHERE u = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0) WHERE explain LIKE '%Granules: 1/16%';

SELECT '-- 8. Control: Nullable(UInt64) must still prune and still keep NULLs';
SELECT 'no index      ', count() FROM t_nullable WHERE n = 3 SETTINGS use_skip_indexes = 0;
SELECT 'granule       ', count() FROM t_nullable WHERE n = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'granules 1/16 ', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_nullable WHERE n = 3
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0) WHERE explain LIKE '%Granules: 1/16%';
SELECT 'no index null ', count() FROM t_nullable WHERE n IS NULL SETTINGS use_skip_indexes = 0;
SELECT 'granule null  ', count() FROM t_nullable WHERE n IS NULL
    SETTINGS use_skip_indexes = 1, secondary_indices_enable_bulk_filtering = 0, use_skip_indexes_on_data_read = 0;

SELECT '-- 9. Control: DDL still refuses these types';
"

# CREATE must keep refusing what ATTACH lets through, at the top level and through a wrapper.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_reject_dyn (k UInt64, d Dynamic, INDEX idx d TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()" 2>&1 | grep -c -m 1 -F 'is not allowed in minmax index'
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_reject_arr (k UInt64, d Array(Dynamic), INDEX idx d TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()" 2>&1 | grep -c -m 1 -F 'is not allowed in minmax index'
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_reject_var (k UInt64, v Variant(UInt64, String), INDEX idx v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()" 2>&1 | grep -c -m 1 -F 'is not allowed in minmax index'
