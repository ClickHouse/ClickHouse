-- A MATERIALIZED column defined over an ALIAS column is recomputed by CLEAR COLUMN: the alias
-- reference is replaced by the expression it stands for, so the recompute stage does not demand a
-- column no part holds. `04869_materialized_over_alias_column_mutation` covers UPDATE and on-fly
-- reads; CLEAR COLUMN reaches the recompute through a different set, so it needs its own case.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_clear_over_alias;

CREATE TABLE t_clear_over_alias
(
    x Int32,
    a Int32 ALIAS x + 100,
    m Int32 MATERIALIZED a + 1
)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple();

INSERT INTO t_clear_over_alias (x) VALUES (5);
SELECT x, m FROM t_clear_over_alias;

-- x becomes its type default, so m has to be recomputed through the alias to (0 + 100) + 1.
ALTER TABLE t_clear_over_alias CLEAR COLUMN x IN PARTITION tuple();
SELECT x, m FROM t_clear_over_alias;

DROP TABLE t_clear_over_alias;

-- The expansion has to keep the alias's declared type. The `+ 256` is what makes the assertion
-- load-bearing: after the clear `x` is 0, so the expansion narrowed to the alias's UInt8 gives
-- `UInt8(0 + 256) = 0`, while substituting the bare `x + 256` at the column's own UInt16 gives 256.
-- Without the offset both spellings would produce 0 and the case could not tell them apart.
DROP TABLE IF EXISTS t_clear_over_narrowing_alias;

CREATE TABLE t_clear_over_narrowing_alias
(
    x UInt16,
    a UInt8 ALIAS x + 256,
    m UInt16 MATERIALIZED a
)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple();

INSERT INTO t_clear_over_narrowing_alias (x) VALUES (300);
SELECT x, m FROM t_clear_over_narrowing_alias;

ALTER TABLE t_clear_over_narrowing_alias CLEAR COLUMN x IN PARTITION tuple();
SELECT x, m FROM t_clear_over_narrowing_alias;

DROP TABLE t_clear_over_narrowing_alias;
