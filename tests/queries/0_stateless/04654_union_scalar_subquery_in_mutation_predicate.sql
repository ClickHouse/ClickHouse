-- A set operation inside a scalar subquery used directly as a mutation predicate must be normalized
-- when the mutation command is re-parsed on execution.
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/81135

DROP TABLE IF EXISTS t_union_scalar_pred;
CREATE TABLE t_union_scalar_pred (c0 Int) ENGINE = MergeTree ORDER BY tuple();

-- `DELETE FROM` overwrites `mutations_sync` from `lightweight_deletes_sync`, so that is the setting
-- which decides whether anything waits for the mutation. The failure surfaced only while the
-- mutation executed on the part, so pin it rather than relying on the default.
SET lightweight_deletes_sync = 1;

-- With mutation validation disabled the mutation was accepted and then failed on the part.
SET validate_mutation_query = 0;
INSERT INTO t_union_scalar_pred VALUES (0), (1), (2);
DELETE FROM t_union_scalar_pred WHERE (SELECT true UNION DISTINCT SELECT true);
SELECT 'delete scalar subquery union distinct', arraySort(groupArray(c0)) FROM t_union_scalar_pred;
TRUNCATE TABLE t_union_scalar_pred;

-- With validation enabled the same query was rejected earlier, at validation time.
SET validate_mutation_query = 1;
INSERT INTO t_union_scalar_pred VALUES (0), (1), (2);
DELETE FROM t_union_scalar_pred WHERE (SELECT true UNION DISTINCT SELECT true);
SELECT 'delete scalar subquery union distinct, validated', arraySort(groupArray(c0)) FROM t_union_scalar_pred;

DROP TABLE t_union_scalar_pred;
