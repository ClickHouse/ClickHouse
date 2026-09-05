#!/usr/bin/env bash
# Regression test for the merge memory reservation estimate (see CompactionStatistics).
# A projection can reference a column the merge does not write: a fully expired TTL column with no DDL
# DEFAULT is dropped from the merged part entirely, so a later merge of that part neither reads nor
# writes it. The merging-vs-gathering classification of the certainly-vertical pricing resolved every
# projection-required column against the written columns and threw BAD_ARGUMENTS ("Column or subcolumn
# 'u' is not found") for the expired one, failing the user's OPTIMIZE - originally caught by
# 04492_projection_ttl_default_divergence when a background TTL merge ran before its explicit OPTIMIZE.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_expired_proj_column
    (
        d Date,
        k UInt32,
        u Int32 TTL d + INTERVAL 1 DAY, -- no DDL DEFAULT: once fully expired, the column is dropped from the part
        PROJECTION p (SELECT k, u ORDER BY k)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0,
             enable_vertical_merge_algorithm = 1,
             vertical_merge_algorithm_min_rows_to_activate = 1,
             vertical_merge_algorithm_min_bytes_to_activate = 1;

    INSERT INTO t_expired_proj_column SELECT '2000-01-01', number, 100 + number FROM numbers(1000);

    -- Applies the column TTL: u is fully expired and has no default, so the merged part drops it.
    OPTIMIZE TABLE t_expired_proj_column FINAL;

    -- Re-merge of a part that no longer stores u, while the projection still requires it: the
    -- reservation estimate must price this merge without throwing.
    OPTIMIZE TABLE t_expired_proj_column FINAL;

    SELECT count(), min(u), max(u) FROM t_expired_proj_column;
"
