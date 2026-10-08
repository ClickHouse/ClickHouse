-- `Array`, `Map` and `JSON` comparisons order a NULL element after every value (`['x'] < [NULL]` is 1),
-- so index analysis must not drop rows for a comparison constant that holds such a NULL.

DROP TABLE IF EXISTS t_arr;
CREATE TABLE t_arr (a Array(String)) ENGINE = MergeTree PARTITION BY a ORDER BY tuple();
INSERT INTO t_arr VALUES (['x']), (['y']), (['x', 'z']);

SELECT a FROM t_arr WHERE NOT (a > [NULL]) ORDER BY a;
SELECT a FROM t_arr WHERE a <= [NULL] ORDER BY a;
SELECT a FROM t_arr WHERE a < ['x', NULL] ORDER BY a;
SELECT count() FROM t_arr WHERE NOT (a > [NULL]);

-- A constant without a NULL still prunes partitions.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_arr WHERE a < ['y']) WHERE explain ILIKE '%Parts: 2/3%';

DROP TABLE IF EXISTS t_map;
CREATE TABLE t_map (m Map(String, String)) ENGINE = MergeTree PARTITION BY m ORDER BY tuple();
INSERT INTO t_map VALUES (map('k', 'x')), (map('k', 'y'));
SELECT m FROM t_map WHERE m <= map('k', NULL) ORDER BY m['k'];

DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_tuple (t Tuple(String, Array(String))) ENGINE = MergeTree PARTITION BY t ORDER BY tuple();
INSERT INTO t_tuple VALUES (('x', ['a'])), (('x', ['b'])), (('y', ['a']));
SELECT t FROM t_tuple WHERE t < ('x', [NULL]) ORDER BY t;

DROP TABLE IF EXISTS t_pk;
CREATE TABLE t_pk (a Array(Nullable(String))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_pk VALUES (['x']), (['y']), (['x', 'z']), ([NULL]);
SELECT a FROM t_pk WHERE a < [NULL] ORDER BY a;

DROP TABLE IF EXISTS t_minmax;
CREATE TABLE t_minmax
(
    k UInt64,
    a Array(Nullable(String)),
    j JSON(x Nullable(String)),
    aj Array(JSON(x Nullable(String))),
    INDEX ia a TYPE minmax GRANULARITY 1,
    INDEX ij j TYPE minmax GRANULARITY 1,
    INDEX iaj aj TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, allow_minmax_index_for_json = 1;
INSERT INTO t_minmax VALUES (1, ['x'], '{"x":"a"}', ['{"x":"a"}']), (2, ['y'], '{"x":"b"}', ['{"x":"b"}']), (3, [NULL], '{"x":null}', ['{"x":null}']);
SELECT k FROM t_minmax WHERE a < [NULL] ORDER BY k;
SELECT k FROM t_minmax WHERE j < '{"x":null}'::JSON(x Nullable(String)) ORDER BY k;
SELECT k FROM t_minmax WHERE aj < ['{"x":null}']::Array(JSON(x Nullable(String))) ORDER BY k;

DROP TABLE t_arr;
DROP TABLE t_map;
DROP TABLE t_tuple;
DROP TABLE t_pk;
DROP TABLE t_minmax;
