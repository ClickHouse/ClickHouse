-- Tests that ALTER TABLE ... DROP COLUMN works when another column has a DEFAULT/MATERIALIZED
-- expression that defines and later references an inline alias (`expr AS a ... a`).
-- Previously the dependency check for the dropped column resolved each remaining default expression
-- as a standalone node, which did not collect its internal aliases, so it failed with UNKNOWN_IDENTIFIER.

DROP TABLE IF EXISTS t_04502;

CREATE TABLE t_04502
(
    id UInt64,
    src String,
    to_drop String DEFAULT '',
    aliased String DEFAULT concat(substring(upper(src) AS u, 1, 3), '-', u)
)
ENGINE = MergeTree ORDER BY id;

ALTER TABLE t_04502 DROP COLUMN to_drop;

INSERT INTO t_04502 (id, src) VALUES (1, 'hello');
SELECT id, src, aliased FROM t_04502 ORDER BY id;

DROP TABLE t_04502;
