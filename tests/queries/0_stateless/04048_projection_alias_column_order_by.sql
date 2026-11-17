-- Bug test for PR #91352 (Correct metadata of normal projections).
-- Creating a normal projection with a table ALIAS column in ORDER BY (but not in SELECT)
-- fails with UNKNOWN_IDENTIFIER because FetchColumns cannot resolve ALIAS columns
-- appended by cloneToASTSelect when they are not in the projection SELECT list.

-- Case 1: ALIAS column only in ORDER BY → CREATE TABLE throws UNKNOWN_IDENTIFIER
DROP TABLE IF EXISTS t_04048_alias_proj;
CREATE TABLE t_04048_alias_proj
(
    id    UInt64,
    a     UInt32,
    b     UInt32,
    ab_sum UInt64 ALIAS a + b,
    PROJECTION p1 (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id; -- { serverError UNKNOWN_IDENTIFIER }

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
