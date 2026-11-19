-- A normal projection can have a table ALIAS column in ORDER BY that is not in SELECT:
-- the projection stores the columns that the ALIAS column reads.

-- Case 1: ALIAS column only in ORDER BY
DROP TABLE IF EXISTS t_04048_alias_proj;
CREATE TABLE t_04048_alias_proj
(
    id    UInt64,
    a     UInt32,
    b     UInt32,
    ab_sum UInt64 ALIAS a + b,
    PROJECTION p1 (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_04048_alias_proj (id, a, b) VALUES (1, 10, 5), (2, 1, 1);
SELECT ab_sum FROM t_04048_alias_proj ORDER BY ab_sum;

-- Case 2: ALIAS column in SELECT (not just ORDER BY) works fine
DROP TABLE IF EXISTS t_04048_alias_proj;
CREATE TABLE t_04048_alias_proj
(
    id    UInt64,
    a     UInt32,
    b     UInt32,
    ab_sum UInt64 ALIAS a + b,
    PROJECTION p1 (SELECT ab_sum ORDER BY a)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_04048_alias_proj (id, a, b) VALUES (1, 10, 5), (2, 1, 1);
SELECT ab_sum FROM t_04048_alias_proj ORDER BY ab_sum;

DROP TABLE t_04048_alias_proj;

-- Case 3: ADD PROJECTION over an ALIAS column with optimize_respect_aliases = 0.
-- Building a projection is DDL, so its required columns must not depend on a
-- query-time optimization setting.
SET optimize_respect_aliases = 0;
DROP TABLE IF EXISTS t_04048_alias_proj;
CREATE TABLE t_04048_alias_proj
(
    id    UInt64,
    a     UInt32,
    b     UInt32,
    ab_sum UInt64 ALIAS a + b
)
ENGINE = MergeTree ORDER BY id
-- The INSERT below must build the projection, otherwise it never reaches the bug.
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_04048_alias_proj ADD PROJECTION p1 (SELECT ab_sum ORDER BY a);

INSERT INTO t_04048_alias_proj (id, a, b) VALUES (1, 10, 5), (2, 1, 1);
SELECT ab_sum FROM t_04048_alias_proj ORDER BY ab_sum;

DROP TABLE t_04048_alias_proj;

-- Case 4: same, but the ALIAS column itself is added by ALTER.
DROP TABLE IF EXISTS t_04048_alias_proj;
CREATE TABLE t_04048_alias_proj
(
    id    UInt64,
    a     UInt32,
    b     UInt32
)
ENGINE = MergeTree ORDER BY id
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_04048_alias_proj ADD COLUMN ab_sum UInt64 ALIAS a + b;
ALTER TABLE t_04048_alias_proj ADD PROJECTION p1 (SELECT ab_sum ORDER BY a);

INSERT INTO t_04048_alias_proj (id, a, b) VALUES (1, 10, 5), (2, 1, 1);
SELECT ab_sum FROM t_04048_alias_proj ORDER BY ab_sum;

DROP TABLE t_04048_alias_proj;
