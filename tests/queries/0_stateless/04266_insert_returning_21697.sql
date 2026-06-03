-- Issue #21697: INSERT ... RETURNING

SET async_insert = 0;

DROP TABLE IF EXISTS t_insert_returning;
DROP TABLE IF EXISTS t_insert_returning_other;

CREATE TABLE t_insert_returning (id UInt64, name String) ENGINE = Memory;
CREATE TABLE t_insert_returning_other (id UInt64) ENGINE = Memory;

-- INSERT VALUES + RETURNING with row filter
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT * FROM t_insert_returning WHERE id = 1 ORDER BY id) VALUES (1, 'foo');

SELECT 'table after values returning';
SELECT * FROM t_insert_returning ORDER BY id;

-- INSERT SELECT + RETURNING with aggregate
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning SELECT 2 AS id, 'bar' AS name RETURNING (SELECT count() AS c FROM t_insert_returning);

SELECT 'table after select returning';
SELECT * FROM t_insert_returning ORDER BY id;

-- RETURNING can query a table other than the INSERT target
INSERT INTO t_insert_returning_other VALUES (10);
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT id FROM t_insert_returning_other WHERE id = 10) VALUES (3, 'baz');

-- INSERT failure prevents RETURNING SELECT from running
SELECT 'insert failure';
INSERT INTO t_insert_returning (id, bad_col) RETURNING (SELECT 1 AS x) VALUES (1, 'x'); -- { serverError NO_SUCH_COLUMN_IN_TABLE }

SELECT count() AS rows_after_failed_insert FROM t_insert_returning;

-- SETTINGS on the RETURNING subquery apply to the result (max_result_rows)
SELECT 'returning subquery settings';
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT number FROM numbers(100) SETTINGS max_block_size=1, max_result_rows=5, result_overflow_mode='break') VALUES (50, 'limits');

-- RETURNING subquery is planned only after INSERT finishes (consistent with native push inserts)
SELECT 'returning analysis after insert';
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT no_such_col FROM t_insert_returning) VALUES (101, 'late_analysis'); -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() AS inserted_after_bad_returning FROM t_insert_returning WHERE id = 101;

-- RETURNING SETTINGS are applied only after INSERT (invalid setting must not block insert)
SELECT 'returning settings after insert';
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 SETTINGS no_such_setting=1) VALUES (102, 'settings'); -- { serverError UNKNOWN_SETTING }
SELECT count() AS inserted_after_bad_returning_settings FROM t_insert_returning WHERE id = 102;

-- Query-global execution/resource limits in the RETURNING subquery are rejected (cannot be enforced per-phase on
-- the shared query: memory trackers and the query time limit are set once from the INSERT settings), while the
-- INSERT still completes first
SELECT 'returning max_execution_time rejection';
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 SETTINGS max_execution_time=1) VALUES (103, 'timeout'); -- { serverError NOT_IMPLEMENTED }
SELECT count() AS inserted_after_returning_timeout FROM t_insert_returning WHERE id = 103;

SELECT 'returning memory limit rejection';
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 SETTINGS max_memory_usage=1000000) VALUES (104, 'memlimit'); -- { serverError NOT_IMPLEMENTED }
SELECT count() AS inserted_after_returning_memlimit FROM t_insert_returning WHERE id = 104;

-- Temporary-data-on-disk limits in the RETURNING subquery are rejected for the same reason (the disk scope is shared
-- with the INSERT phase), while the INSERT still completes first
SELECT 'returning temp data limit rejection';
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 SETTINGS max_temporary_data_on_disk_size_for_query=1) VALUES (107, 'tempdisk'); -- { serverError NOT_IMPLEMENTED }
SELECT count() AS inserted_after_returning_tempdisk FROM t_insert_returning WHERE id = 107;

-- Network-bandwidth limits in the RETURNING subquery are rejected for the same reason (the user/all-user throttlers
-- are bound once from the INSERT phase), while the INSERT still completes first
SELECT 'returning network bandwidth rejection';
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 SETTINGS max_network_bandwidth_for_user=1) VALUES (109, 'netbw'); -- { serverError NOT_IMPLEMENTED }
SELECT count() AS inserted_after_returning_netbw FROM t_insert_returning WHERE id = 109;

-- Per-query read/write throttler limits in the RETURNING subquery are rejected too (the throttlers are copied from
-- the INSERT context by Context::createCopy), while the INSERT still completes first
SELECT 'returning local read bandwidth rejection';
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 SETTINGS max_local_read_bandwidth=1) VALUES (110, 'localbw'); -- { serverError NOT_IMPLEMENTED }
SELECT count() AS inserted_after_returning_localbw FROM t_insert_returning WHERE id = 110;

-- AST size/depth limits in the RETURNING subquery's SETTINGS are enforced when the subquery is planned (after the
-- INSERT persists), exactly as for a standalone SELECT
SELECT 'returning ast size limit';
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 SETTINGS max_ast_elements=1) VALUES (108, 'astsize'); -- { serverError TOO_BIG_AST }
SELECT count() AS inserted_after_returning_ast_size FROM t_insert_returning WHERE id = 108;

-- A small outer/session AST size limit must not reject a RETURNING subquery that raises the limit in its own SETTINGS:
-- the subquery is detached from the INSERT before the outer checkASTSizeLimits, so the INSERT runs and the subquery is
-- size-checked later with its own settings. Without the fix the combined AST trips the outer max_ast_elements and the
-- row is never inserted.
SELECT 'returning delayed ast size limit';
TRUNCATE TABLE t_insert_returning;
SET max_ast_elements = 20;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 SETTINGS max_ast_elements = 100000) VALUES (111, 'astdelay');
SET max_ast_elements = 0;
SELECT count() AS inserted_after_returning_delayed_ast FROM t_insert_returning WHERE id = 111;

-- A different allow_experimental_analyzer in the RETURNING subquery must not be validated before the INSERT runs
-- (the subquery is an independent query planned only after the INSERT persists)
SELECT 'returning analyzer setting';
TRUNCATE TABLE t_insert_returning;
INSERT INTO t_insert_returning (id, name) SETTINGS allow_experimental_analyzer=1 RETURNING (SELECT 1 SETTINGS allow_experimental_analyzer=0) VALUES (105, 'analyzer');
SELECT count() AS inserted_after_returning_analyzer FROM t_insert_returning WHERE id = 105;

-- The RETURNING subquery must be normalized with its own SETTINGS, not the outer INSERT's: the outer session uses
-- UNION ALL, but the subquery overrides union_default_mode to DISTINCT, so its UNION must collapse to a single row.
SELECT 'returning union_default_mode';
TRUNCATE TABLE t_insert_returning;
SET union_default_mode = 'ALL';
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 UNION SELECT 1 SETTINGS union_default_mode='DISTINCT') VALUES (106, 'union');
SET union_default_mode = '';
SELECT count() AS inserted_after_returning_union FROM t_insert_returning WHERE id = 106;

-- async_insert is rejected
SELECT 'async insert rejection';
SET async_insert = 1;
INSERT INTO t_insert_returning (id, name) RETURNING (SELECT 1 AS x) VALUES (4, 'async'); -- { serverError NOT_IMPLEMENTED }
SET async_insert = 0;

DROP TABLE t_insert_returning_other;
DROP TABLE t_insert_returning;
