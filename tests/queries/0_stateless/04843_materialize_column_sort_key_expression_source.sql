-- Rewriting a MATERIALIZED column in place must be refused not only when the column is a bare
-- sort-key item, but also when a sort-key expression reads it (e.g. `ORDER BY m + 1`).

DROP TABLE IF EXISTS t_matkey_expr;

CREATE TABLE t_matkey_expr
(
    a UInt64,
    m UInt64 MATERIALIZED a + 1
) ENGINE = MergeTree ORDER BY m + 1;

INSERT INTO t_matkey_expr SELECT number FROM numbers(10);

-- Explicit MATERIALIZE COLUMN on a source column of the sort-key expression.
ALTER TABLE t_matkey_expr MATERIALIZE COLUMN m; -- { serverError CANNOT_UPDATE_COLUMN }

DROP TABLE t_matkey_expr;

-- The automatic rematerialization queued when a matcher changes the effective expression
-- must take the same guard: here adding `b` changes the expansion of `* EXCEPT m`.
CREATE TABLE t_matkey_expr_auto
(
    a UInt64,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m)
) ENGINE = MergeTree ORDER BY m + 1;

INSERT INTO t_matkey_expr_auto SELECT number FROM numbers(10);

ALTER TABLE t_matkey_expr_auto ADD COLUMN b UInt64 DEFAULT a + 1000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The parts stay ordered by the original key values.
SELECT a FROM t_matkey_expr_auto ORDER BY m + 1;

DROP TABLE t_matkey_expr_auto;
