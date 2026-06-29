-- A trailing query-level SETTINGS clause after the RETURNING subquery of an INSERT ... SELECT must be
-- accepted. Previously the source SELECT parser stopped at RETURNING and the trailing SETTINGS was never
-- consumed, producing a SYNTAX_ERROR.

SET async_insert = 0;

DROP TABLE IF EXISTS t_ret_settings;
DROP TABLE IF EXISTS t_ret_settings_src;

CREATE TABLE t_ret_settings (id UInt64) ENGINE = Memory;
CREATE TABLE t_ret_settings_src (id UInt64) ENGINE = Memory;
INSERT INTO t_ret_settings_src VALUES (1), (2), (3);

-- INSERT ... SELECT ... RETURNING (...) SETTINGS ...
SELECT 'select returning trailing settings';
INSERT INTO t_ret_settings SELECT id FROM t_ret_settings_src
RETURNING (SELECT count() FROM t_ret_settings)
SETTINGS parallel_distributed_insert_select = 1;

SELECT 'rows after insert';
SELECT id FROM t_ret_settings ORDER BY id;

-- The trailing SETTINGS is pushed into the source SELECT, just like for a plain INSERT ... SELECT ... SETTINGS.
SELECT 'select returning trailing settings pushed down';
TRUNCATE TABLE t_ret_settings;
INSERT INTO t_ret_settings SELECT number FROM numbers(5)
RETURNING (SELECT count() FROM t_ret_settings)
SETTINGS max_threads = 1;

SELECT count() FROM t_ret_settings;

-- Trailing SETTINGS still apply to the source SELECT / INSERT phase.
SELECT 'trailing settings still apply to source select';
TRUNCATE TABLE t_ret_settings;
INSERT INTO t_ret_settings SELECT number FROM numbers(10)
RETURNING (SELECT count() FROM t_ret_settings)
SETTINGS max_result_rows = 1, result_overflow_mode = 'break';

SELECT count() FROM t_ret_settings;

-- Trailing source SETTINGS must not affect RETURNING SELECT normalization/planning.
-- Session UNION mode is ALL; trailing source settings set DISTINCT only for source phase.
SELECT 'trailing settings do not affect returning planning';
TRUNCATE TABLE t_ret_settings;
SET union_default_mode = 'ALL';
INSERT INTO t_ret_settings SELECT 1
RETURNING (SELECT 1 UNION SELECT 1)
SETTINGS union_default_mode = 'DISTINCT';
SET union_default_mode = '';

SELECT count() FROM t_ret_settings;

-- Trailing SETTINGS after RETURNING apply to the source SELECT only, not to the RETURNING subquery.
SELECT 'trailing settings do not cap returning';
TRUNCATE TABLE t_ret_settings;
INSERT INTO t_ret_settings SELECT number FROM numbers(10)
RETURNING (SELECT number FROM numbers(10) ORDER BY number)
SETTINGS max_result_rows = 1, result_overflow_mode = 'break';

SELECT count() FROM t_ret_settings;

-- INSERT VALUES + RETURNING with a query-level SETTINGS before the data clause still works.
SELECT 'values returning with settings';
TRUNCATE TABLE t_ret_settings;
INSERT INTO t_ret_settings SETTINGS max_threads = 1 RETURNING (SELECT count() FROM t_ret_settings) VALUES (10);

SELECT id FROM t_ret_settings ORDER BY id;

DROP TABLE t_ret_settings_src;
DROP TABLE t_ret_settings;
