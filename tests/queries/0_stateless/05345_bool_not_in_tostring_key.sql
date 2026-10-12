-- Tags: no-random-settings

-- `Bool` renders every nonzero byte as `true`, so a set element pushed through a key transform DAG
-- without a cast (or through a cast that hides behind `UInt8`) could fold distinct bytes together
-- and let `NOT IN`/`has` wrongly skip granules. The set index must treat such conversions as inexact.
-- https://github.com/ClickHouse/ClickHouse/issues/124825

DROP TABLE IF EXISTS t_bool_tostring_key;

CREATE TABLE t_bool_tostring_key (b Bool) ENGINE = MergeTree ORDER BY toString(b);

INSERT INTO t_bool_tostring_key VALUES (true);
INSERT INTO t_bool_tostring_key VALUES (false);

SELECT count() FROM t_bool_tostring_key WHERE b NOT IN (2);
SELECT countIf(b NOT IN (2)) FROM t_bool_tostring_key;
SELECT count() FROM t_bool_tostring_key WHERE NOT has([toUInt8(2)], b);
SELECT countIf(NOT has([toUInt8(2)], b)) FROM t_bool_tostring_key;
SELECT b FROM t_bool_tostring_key WHERE NOT has([toUInt8(2)], b) ORDER BY b;

EXPLAIN indexes = 1 SELECT count() FROM t_bool_tostring_key WHERE b NOT IN (2);

DROP TABLE t_bool_tostring_key;
