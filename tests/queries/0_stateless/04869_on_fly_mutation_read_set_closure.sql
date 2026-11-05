-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutation this test needs to stay pending

-- The read set of an on-fly read is grown by the surviving commands themselves: a DELETE predicate
-- and an UPDATE expression put the columns they read into it. Those columns have to be closed over
-- in turn, and they have to be recorded under their storage names.

SET alter_sync = 0, mutations_sync = 0;
SET apply_mutations_on_fly = 1;

SELECT 'delete predicate over a materialized chain';

DROP TABLE IF EXISTS t_delete_over_chain;

CREATE TABLE t_delete_over_chain
(
    x Int32,
    y Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_delete_over_chain (x, y) VALUES (10, 1), (100, 2);

SYSTEM STOP MERGES t_delete_over_chain;

ALTER TABLE t_delete_over_chain UPDATE x = x * 10 WHERE 1;
-- The predicate reads `m2`, which is recomputed from the pending `x`, so both rows go. Reading `y`
-- alone must not see a different table than reading `y` next to the chain.
ALTER TABLE t_delete_over_chain DELETE WHERE m2 > 100;

SELECT groupArray(y) FROM t_delete_over_chain;
SELECT groupArray((y, m2)) FROM t_delete_over_chain;

SYSTEM START MERGES t_delete_over_chain;
-- Mutations are materialized in order, so waiting for a later one drains the pending ones.
ALTER TABLE t_delete_over_chain UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;
SELECT groupArray((y, m2)) FROM t_delete_over_chain;

SELECT 'update expression over a materialized chain';

DROP TABLE IF EXISTS t_update_over_chain;

CREATE TABLE t_update_over_chain
(
    x Int32,
    y Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_update_over_chain (x, y) VALUES (10, 0);

SYSTEM STOP MERGES t_update_over_chain;

ALTER TABLE t_update_over_chain UPDATE x = 20 WHERE 1;
-- `y` is assigned from `m2`, so the chain behind `m2` has to be pulled in even though the query
-- selects neither it nor `x`.
ALTER TABLE t_update_over_chain UPDATE y = m2 WHERE 1;

SELECT y FROM t_update_over_chain;

SYSTEM START MERGES t_update_over_chain;
ALTER TABLE t_update_over_chain UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT y FROM t_update_over_chain;

SELECT 'subcolumn identifiers in commands';

-- The read set is keyed on storage columns while identifiers collected from a command are as
-- written, so `t.a` has to be recorded under `t` or every read of the table fails while the
-- mutation is pending. Once for a predicate, once for an assignment expression.
--
-- One command per table on purpose: a command reading a subcolumn of a column an earlier command
-- writes gets the pre-update value even when materialized, which is a defect of the mutation chain
-- rather than of the read set.

DROP TABLE IF EXISTS t_subcolumn_in_predicate;

CREATE TABLE t_subcolumn_in_predicate (t Tuple(a Int32, b Int32), y Int32)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_subcolumn_in_predicate VALUES ((10, 0), 1), ((100, 0), 2);

SYSTEM STOP MERGES t_subcolumn_in_predicate;

ALTER TABLE t_subcolumn_in_predicate DELETE WHERE t.a > 15;

SELECT groupArray(y) FROM t_subcolumn_in_predicate;
SELECT groupArray((y, t)) FROM t_subcolumn_in_predicate;

SYSTEM START MERGES t_subcolumn_in_predicate;
ALTER TABLE t_subcolumn_in_predicate UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;
SELECT groupArray((y, t)) FROM t_subcolumn_in_predicate;

DROP TABLE IF EXISTS t_subcolumn_in_assignment;

CREATE TABLE t_subcolumn_in_assignment (t Tuple(a Int32, b Int32), y Int32)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_subcolumn_in_assignment VALUES ((10, 0), 1), ((100, 0), 2);

SYSTEM STOP MERGES t_subcolumn_in_assignment;

ALTER TABLE t_subcolumn_in_assignment UPDATE t = (t.a * 10, 0) WHERE 1;

SELECT groupArray(y) FROM t_subcolumn_in_assignment;
SELECT groupArray((y, t)) FROM t_subcolumn_in_assignment;

SYSTEM START MERGES t_subcolumn_in_assignment;
ALTER TABLE t_subcolumn_in_assignment UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;
SELECT groupArray((y, t)) FROM t_subcolumn_in_assignment;

SELECT 'nested column in a predicate';

DROP TABLE IF EXISTS t_nested_in_predicate;

CREATE TABLE t_nested_in_predicate (n Nested(a Int32), y Int32)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_nested_in_predicate VALUES ([1], 1), ([5], 2);

SYSTEM STOP MERGES t_nested_in_predicate;

-- `n.a` is a physical column, not a subcolumn, so normalising the read set must leave it alone.
ALTER TABLE t_nested_in_predicate UPDATE y = y + 10 WHERE 1;
ALTER TABLE t_nested_in_predicate DELETE WHERE n.a[1] > 3;

SELECT groupArray(y) FROM t_nested_in_predicate;

SYSTEM START MERGES t_nested_in_predicate;

DROP TABLE t_delete_over_chain;
DROP TABLE t_update_over_chain;
DROP TABLE t_subcolumn_in_predicate;
DROP TABLE t_subcolumn_in_assignment;
DROP TABLE t_nested_in_predicate;
