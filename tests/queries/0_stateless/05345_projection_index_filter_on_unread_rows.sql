-- Tags: no-parallel-replicas

-- A projection index must not fail the query on rows the query does not read.

DROP TABLE IF EXISTS t_proj_index_unread;

CREATE TABLE t_proj_index_unread (id UInt64, a UInt64, x UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

ALTER TABLE t_proj_index_unread ADD PROJECTION p (SELECT a, x, s, _part_offset ORDER BY a);

-- x = 5 and s = 'bad' only for ids 500 .. 599, all below the primary key cut id >= 1024.
INSERT INTO t_proj_index_unread
SELECT number, number % 10, intDiv(number, 100), if(intDiv(number, 100) = 5, 'bad', toString(number)) FROM numbers(10000);

SET optimize_use_projections = 1, min_table_rows_to_use_projection_index = 0;

SELECT count() FROM t_proj_index_unread WHERE id >= 1024 AND a = 1 AND gcd(1, x - 5) = 1 SETTINGS optimize_use_projection_filtering = 1;
SELECT count() FROM t_proj_index_unread WHERE id >= 1024 AND a = 1 AND gcd(1, x - 5) = 1 SETTINGS optimize_use_projection_filtering = 0;
SELECT count() FROM t_proj_index_unread WHERE id >= 1024 AND a = 1 AND toUInt64(s) > 0 SETTINGS optimize_use_projection_filtering = 1;
SELECT count() FROM t_proj_index_unread WHERE id >= 1024 AND a = 1 AND toUInt64(s) > 0 SETTINGS optimize_use_projection_filtering = 0;

-- An error the query must not ignore still fails it.
SELECT count() FROM t_proj_index_unread WHERE id >= 1024 AND a = 1 AND throwIf(x = 5, 'unread row', toInt32(159)) = 0
SETTINGS optimize_use_projection_filtering = 1, allow_custom_error_code_in_throwif = 1; -- { serverError TIMEOUT_EXCEEDED }

DROP TABLE t_proj_index_unread;
