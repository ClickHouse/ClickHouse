-- Regression: with optimize_row_order_if_no_order_by = 1, INSERT into a table with ORDER BY ()
-- must not throw NOT_IMPLEMENTED for column types whose estimateCardinalityInPermutedRange
-- relies on a non-implemented getDataAt (e.g. Nullable(Tuple), JSON).

SET allow_experimental_json_type = 1;
SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_nullable_tuple;
DROP TABLE IF EXISTS t_json;

CREATE TABLE t_nullable_tuple (id UInt32, x Nullable(Tuple(a UInt32, b String)))
ENGINE = MergeTree ORDER BY ()
SETTINGS optimize_row_order_if_no_order_by = 1;

INSERT INTO t_nullable_tuple VALUES (3, (3, 'c')), (1, (1, 'a')), (2, NULL), (1, (1, 'a')), (2, (2, 'b'));

SELECT count() FROM t_nullable_tuple;

CREATE TABLE t_json (id UInt32, j JSON)
ENGINE = MergeTree ORDER BY ()
SETTINGS optimize_row_order_if_no_order_by = 1;

INSERT INTO t_json VALUES (1, '{"a": 1}'), (2, '{"b": "x"}'), (1, '{"a": 1}'), (3, '{"c": [1,2,3]}');

SELECT count() FROM t_json;

DROP TABLE t_nullable_tuple;
DROP TABLE t_json;
