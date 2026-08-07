-- Cross-type values on the right-hand side of `IN`: the constant `Set` path converts each
-- element to the left-hand side type (skipping unrepresentable values), while the row-wise
-- rewrite for a non-constant right-hand side compares through the least common supertype.
-- The row-wise results below match the analyzer behavior for these queries in previous
-- versions of ClickHouse; the old analyzer rejected them with `BAD_ARGUMENTS`.
-- This test pins the agreement between both analyzers for the row-wise rewrite.

-- { echoOn }

SET enable_analyzer = 0;

-- Constant `Set` contract: `DateTime` is truncated to the `Date` key type.
SELECT toDate('2020-01-01') IN (toDateTime('2020-01-01 12:34:56'));
-- Row-wise rewrite compares through the `DateTime` supertype.
SELECT toDate('2020-01-01') IN (materialize(toDateTime('2020-01-01 12:34:56')));
SELECT toDate('2020-01-01') NOT IN (materialize(toDateTime('2020-01-01 12:34:56')));
SELECT toDate('2020-01-01') IN (materialize(toDateTime('2020-01-01 12:34:56')), materialize(toDateTime('2020-01-02 00:00:00')));

-- Constant `Set` contract: the unrepresentable `'z'` is skipped.
SELECT CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)') IN ('a', 'z');
SELECT CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)') IN (materialize('a'), materialize('z'));
SELECT CAST('c', 'Enum(\'a\' = 1, \'b\' = 2, \'c\' = 3)') IN (materialize('a'), materialize('z'));

SELECT toIPv4('1.2.3.4') IN (toUInt32(16909060));
SELECT toIPv4('1.2.3.4') IN (materialize(toUInt32(16909060)));
SELECT toIPv4('1.2.3.4') IN (materialize(toUInt32(16909060)), materialize(toUInt32(0)));

SET enable_analyzer = 1;

SELECT toDate('2020-01-01') IN (toDateTime('2020-01-01 12:34:56'));
SELECT toDate('2020-01-01') IN (materialize(toDateTime('2020-01-01 12:34:56')));
SELECT toDate('2020-01-01') NOT IN (materialize(toDateTime('2020-01-01 12:34:56')));
SELECT toDate('2020-01-01') IN (materialize(toDateTime('2020-01-01 12:34:56')), materialize(toDateTime('2020-01-02 00:00:00')));

SELECT CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)') IN ('a', 'z');
SELECT CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)') IN (materialize('a'), materialize('z'));
SELECT CAST('c', 'Enum(\'a\' = 1, \'b\' = 2, \'c\' = 3)') IN (materialize('a'), materialize('z'));

SELECT toIPv4('1.2.3.4') IN (toUInt32(16909060));
SELECT toIPv4('1.2.3.4') IN (materialize(toUInt32(16909060)));
SELECT toIPv4('1.2.3.4') IN (materialize(toUInt32(16909060)), materialize(toUInt32(0)));
