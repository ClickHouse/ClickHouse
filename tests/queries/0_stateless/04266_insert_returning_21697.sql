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
