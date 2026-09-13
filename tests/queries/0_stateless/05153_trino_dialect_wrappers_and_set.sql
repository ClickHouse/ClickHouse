-- Regression tests for the review findings about statement wrappers around a set
-- operation and about the `SET` forms handled before the Trino translation.

SET allow_experimental_trino_dialect = 1;

DROP TABLE IF EXISTS trino_wrappers_src;
DROP TABLE IF EXISTS trino_wrappers_dst;
CREATE TABLE trino_wrappers_src (x Int64) ENGINE = Memory;
CREATE TABLE trino_wrappers_dst (x Int64) ENGINE = Memory;
INSERT INTO trino_wrappers_src VALUES (1), (2), (3);

SET dialect = 'trino';

-- A trailing ORDER BY/LIMIT after a set operation applies to the whole set operation,
-- also when the set operation is the body of `INSERT ... SELECT`, of `EXPLAIN ...`,
-- or written with the `TABLE t` shorthand.
INSERT INTO trino_wrappers_dst SELECT 30 UNION ALL SELECT 10 UNION ALL SELECT 20 ORDER BY 1 LIMIT 2;
SELECT x FROM trino_wrappers_dst ORDER BY x;

EXPLAIN SYNTAX SELECT 30 UNION ALL SELECT 10 ORDER BY 1 LIMIT 1;

TABLE trino_wrappers_src UNION ALL TABLE trino_wrappers_src ORDER BY x DESC LIMIT 2;

WITH t AS (SELECT 5 AS x) INSERT INTO trino_wrappers_dst SELECT x FROM t UNION ALL SELECT 40 ORDER BY 1 LIMIT 1;
SELECT x FROM trino_wrappers_dst ORDER BY x;

-- `SET ROLE` must reach the role parser instead of being read as an assignment
-- to a setting named `ROLE`.
SET ROLE NONE;
SET ROLE ALL;

-- The Trino `SET SESSION name = value` form is translated into a plain `SET`.
SET SESSION max_block_size = 12345;
SELECT value FROM system.settings WHERE name = 'max_block_size';

-- The parser settings of the standard entrypoints are honored in the dialect too.
SET implicit_select = 1;
1 + 2;
SET implicit_select = 0;

SET allow_settings_after_format_in_insert = 1;
INSERT INTO trino_wrappers_dst FORMAT Values SETTINGS max_threads = 1 (100);
SELECT x FROM trino_wrappers_dst ORDER BY x;

SET dialect = 'clickhouse';
DROP TABLE trino_wrappers_src;
DROP TABLE trino_wrappers_dst;
