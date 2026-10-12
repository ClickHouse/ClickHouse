-- A field access of a lambda argument (`x.id` in `x -> x.id`) refers to the argument, not to a table column `x`.

-- `APPLY (x -> x.id)` in a stored default reads the field of the matched column.
DROP TABLE IF EXISTS t_apply_dotted;
CREATE TABLE t_apply_dotted
(
    a Tuple(id UInt8, v String),
    x UInt64,
    d UInt8 DEFAULT tupleElement(tuple(COLUMNS('^a$') APPLY (x -> x.id)), 1)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_apply_dotted (a, x) VALUES ((7, 'q'), 1000);
SELECT * FROM t_apply_dotted;
DROP TABLE t_apply_dotted;

-- A `MATERIALIZED` expression with `x -> x.id` does not depend on the table column `x`.
DROP TABLE IF EXISTS t_lambda_field;
CREATE TABLE t_lambda_field
(
    x Tuple(id UInt8),
    arr Array(Tuple(id UInt8)),
    m Array(UInt16) MATERIALIZED arrayMap(x -> x.id + 1, arr)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda_field (x, arr) VALUES ((5), [(1), (2)]);
ALTER TABLE t_lambda_field CLEAR COLUMN x SETTINGS mutations_sync = 2;
ALTER TABLE t_lambda_field MODIFY COLUMN x Tuple(id UInt16) SETTINGS mutations_sync = 2;
SELECT x, arr, m FROM t_lambda_field;
DROP TABLE t_lambda_field;
