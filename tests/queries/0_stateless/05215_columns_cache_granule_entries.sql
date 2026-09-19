-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- The columns cache holds one entry per stripe of granules of a column (a fixed cut of the part
-- into stripes of about 65536 rows), served granule by granule, so reads that cut the part into
-- different mark ranges find each other's entries: a query with a condition, whose reader skips
-- rows and reads its ranges piecewise, is served from the entries of a full scan and does not
-- displace them, and a read whose ranges the query condition cache prunes on its second run
-- still finds the entries its first run wrote.

SET max_threads = 1;

DROP TABLE IF EXISTS t_cc_granules;

CREATE TABLE t_cc_granules (id UInt64, v UInt64, g UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

-- 13 granules: 12 full ones and one of 1696 rows. `g` is the number of the granule.
INSERT INTO t_cc_granules SELECT number, number % 10, intDiv(number, 8192), toString(number) FROM numbers(100000);

SYSTEM DROP COLUMNS CACHE;
SYSTEM DROP QUERY CONDITION CACHE;

-- A full scan writes the granules of every column it reads.
SELECT sum(v), sum(g), sum(length(s)) FROM t_cc_granules
SETTINGS use_columns_cache = 1, log_comment = 'cc_granules_full';

-- 13 granules of 8192 rows make 2 stripes: one entry per stripe per column.
SELECT count(), uniqExact(row_begin), min(rows), max(rows) FROM system.columns_cache WHERE table = 't_cc_granules' AND database = currentDatabase();

-- A query with a condition reads `s` only for the rows that pass, in pieces of the granules:
-- every piece is served from the entries above, nothing is read from the part.
SELECT sum(length(s)) FROM t_cc_granules WHERE v = 3
SETTINGS use_columns_cache = 1, log_comment = 'cc_granules_condition';

-- ... and the entries are still there for the full scan.
SELECT sum(v), sum(g), sum(length(s)) FROM t_cc_granules
SETTINGS use_columns_cache = 1, log_comment = 'cc_granules_full_again';

-- The query condition cache remembers that only one granule holds `g = 7`, so the second run
-- reads only that granule; it is served from the entry of its stripe too.
SELECT count() FROM t_cc_granules WHERE g = 7
SETTINGS use_columns_cache = 1, use_query_condition_cache = 1, log_comment = 'cc_granules_qcc_1';

SELECT count() FROM t_cc_granules WHERE g = 7
SETTINGS use_columns_cache = 1, use_query_condition_cache = 1, log_comment = 'cc_granules_qcc_2';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] AS hits,
    ProfileEvents['ColumnsCacheMisses'] AS misses,
    ProfileEvents['SelectedMarks'] AS marks
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'cc_granules_%'
ORDER BY event_time_microseconds;

DROP TABLE t_cc_granules;
