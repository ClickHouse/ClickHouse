-- `f | g` has no equivalent call syntax, so it stays an operator where other operators are written as calls.
EXPLAIN SYNTAX CREATE TABLE t (x UInt64 CODEC(ZSTD(a | b))) ENGINE = Memory;
EXPLAIN SYNTAX CREATE TABLE t (x Array(UInt64) DEFAULT arrayMap(plus(_, 1) | multiply(_, 2), [1])) ENGINE = Memory;
SELECT formatQuerySingleLine('CREATE TABLE t (x UInt64 STATISTICS(tdigest(a | b))) ENGINE = Memory');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY COLUMN x UInt64 CODEC(ZSTD((a | b) | c, a | (b | c)))');
-- `(NOT x) | f` keeps its grouping where operators are formatted as calls.
EXPLAIN SYNTAX CREATE TABLE t (x UInt64 CODEC(ZSTD((NOT a) | b, a | ((NOT b) | c)))) ENGINE = Memory;
WITH 'ALTER TABLE t MODIFY COLUMN x UInt64 CODEC(ZSTD((NOT a) | b))' AS q
SELECT formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q));
WITH 'ALTER TABLE t MODIFY COLUMN x UInt64 CODEC(ZSTD(a | ((NOT b) | c)))' AS q
SELECT formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q));
