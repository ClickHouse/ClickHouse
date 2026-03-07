-- Tags: no-random-settings

-- Test for the bug where convertOuterJoinToInnerJoin optimization crashes with BAD_GET
-- when the WHERE filter expression evaluates to a non-UInt8 type (e.g., Float64, Int8).
-- The isFilterAlwaysFalseForDefaultValueInputs function assumed the filter result was
-- always UInt8, but expressions like radians(), sin(), sign() return Float64 or Int8.

DROP TABLE IF EXISTS left_t;
DROP TABLE IF EXISTS right_t;

CREATE TABLE left_t (c0 Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right_t (c0 Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO left_t VALUES (1), (2);
INSERT INTO right_t VALUES (1), (2), (3);

-- These queries should not throw BAD_GET exception.
-- Float64 filter (radians returns Float64):
SELECT left_t.c0 FROM left_t RIGHT OUTER JOIN right_t ON left_t.c0 = right_t.c0 WHERE radians(left_t.c0) ORDER BY left_t.c0;

-- Int8 filter (sign returns Int8):
SELECT left_t.c0 FROM left_t RIGHT OUTER JOIN right_t ON left_t.c0 = right_t.c0 WHERE sign(left_t.c0) ORDER BY left_t.c0;

-- FULL OUTER JOIN with Float64 filter:
SELECT left_t.c0 FROM left_t FULL OUTER JOIN right_t ON left_t.c0 = right_t.c0 WHERE sin(left_t.c0) ORDER BY left_t.c0;

-- Verify that the optimization still works correctly (no extra rows):
SELECT count() FROM left_t RIGHT OUTER JOIN right_t ON left_t.c0 = right_t.c0 WHERE radians(left_t.c0);
SELECT count() FROM left_t FULL OUTER JOIN right_t ON left_t.c0 = right_t.c0 WHERE sin(left_t.c0);

DROP TABLE left_t;
DROP TABLE right_t;
