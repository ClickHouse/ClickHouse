-- Tests for MutationsDateTimeLiteralVisitor — reversed comparison and IN clause.
--
-- When session_timezone is set, ALTER TABLE ... DELETE/UPDATE wraps string
-- literals compared to DateTime/DateTime64 columns so the background mutation
-- thread evaluates the predicate in the correct timezone.
-- (MutationsDateTimeLiteralVisitor.cpp, rewriteDateTimeLiteralsWithTimezone)
--
-- Previously uncovered paths:
--
--   1. REVERSED COMPARISON (lines 83-93): column on the RIGHT, string literal
--      on the LEFT: WHERE '2000-01-01 02:00:00' <= time
--      The existing test 04056 only exercises the normal form (column >= 'literal').
--
--   2. IN CLAUSE (lines 101-138, 193-197): column as left argument of IN:
--        WHERE time IN tuple('2000-01-01 01:00:00', '2000-01-01 05:00:00')
--      The IN-function dispatch branch (lines 193-197) and the entire
--      tryWrapInLiterals function (lines 101-138) were zero in the nightly
--      coverage run. Using tuple() keeps the literals as ASTFunction children
--      (not a pre-evaluated Tuple literal) so tryWrapInLiterals can walk and
--      wrap each string literal individually.

-- America/Denver (UTC-7 in winter): a non-server timezone, so if the visitor
-- stops wrapping literals, they parse in the server timezone (UTC) and the
-- predicate shifts by 7 hours — the surviving rows below then change.
SET session_timezone = 'America/Denver';
SET mutations_sync = 1;

-- === Scenario 1: reversed comparison ('literal' <= column) ===
-- MutationsDateTimeLiteralVisitor.cpp lines 83-93

DROP TABLE IF EXISTS t_mut_rev;
CREATE TABLE t_mut_rev (id UInt32, time DateTime)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_mut_rev VALUES (1, '2000-01-01 01:00:00'), (2, '2000-01-01 03:00:00'), (3, '2000-01-01 05:00:00');

-- Literal on LEFT, column on RIGHT.
-- Equivalent to time >= '2000-01-01 02:00:00'; deletes rows 2 and 3.
ALTER TABLE t_mut_rev DELETE WHERE '2000-01-01 02:00:00' <= time;

SELECT id, time FROM t_mut_rev ORDER BY id;

-- Same with DateTime64 (no explicit timezone).
DROP TABLE IF EXISTS t_mut_rev64;
CREATE TABLE t_mut_rev64 (id UInt32, time DateTime64(3))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_mut_rev64 VALUES (1, '2000-01-01 01:00:00.000'), (2, '2000-01-01 03:00:00.000'), (3, '2000-01-01 05:00:00.000');

ALTER TABLE t_mut_rev64 DELETE WHERE '2000-01-01 02:00:00' <= time;

SELECT id, time FROM t_mut_rev64 ORDER BY id;

DROP TABLE t_mut_rev;
DROP TABLE t_mut_rev64;

-- === Scenario 2: IN clause with explicit tuple() of string literals ===
-- MutationsDateTimeLiteralVisitor.cpp lines 101-138 and lines 193-197

DROP TABLE IF EXISTS t_mut_in;
CREATE TABLE t_mut_in (id UInt32, time DateTime)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_mut_in VALUES (1, '2000-01-01 01:00:00'), (2, '2000-01-01 03:00:00'), (3, '2000-01-01 05:00:00');

-- Deletes rows 1 and 3; row 2 (03:00) remains.
ALTER TABLE t_mut_in DELETE WHERE time IN tuple('2000-01-01 01:00:00', '2000-01-01 05:00:00');

SELECT id, time FROM t_mut_in ORDER BY id;

-- notIn is also in in_functions — verify it is dispatched to tryWrapInLiterals.
-- After the previous delete only row 2 (03:00) exists.
-- Insert two more rows; delete any row NOT in the set (03:00, 07:00).
-- Row 5 (09:00) is not in the set and gets deleted; rows 2 and 4 survive.
INSERT INTO t_mut_in VALUES (4, '2000-01-01 07:00:00'), (5, '2000-01-01 09:00:00');

ALTER TABLE t_mut_in DELETE WHERE time NOT IN tuple('2000-01-01 03:00:00', '2000-01-01 07:00:00');

SELECT id, time FROM t_mut_in ORDER BY id;

DROP TABLE t_mut_in;

-- === Scenario 3: DateTime64 with IN ===
DROP TABLE IF EXISTS t_mut_in64;
CREATE TABLE t_mut_in64 (id UInt32, time DateTime64(3))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_mut_in64 VALUES (1, '2000-01-01 01:00:00.000'), (2, '2000-01-01 03:00:00.000'), (3, '2000-01-01 05:00:00.000');

-- Deletes rows 1 and 3; row 2 remains.
ALTER TABLE t_mut_in64 DELETE WHERE time IN tuple('2000-01-01 01:00:00', '2000-01-01 05:00:00');

SELECT id, time FROM t_mut_in64 ORDER BY id;

DROP TABLE t_mut_in64;
