-- A sparse column's default rows share one stored value, and the shortcut for a single sparse argument
-- executes the function once for that value and stamps the result onto every default row. For `rand`
-- and friends - which take an argument precisely to defeat common subexpression elimination - that
-- collapsed tens of thousands of independent draws into one. The `LowCardinality` dictionary shortcut
-- did the same per dictionary entry.

DROP TABLE IF EXISTS t_sparse_nondeterministic;

CREATE TABLE t_sparse_nondeterministic (id UInt64, s UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_sparse_nondeterministic SELECT number, if(number % 100 = 0, number + 1, 0) FROM numbers(100000);

SELECT 'the column is stored sparse';
SELECT countIf(serialization_kind = 'Sparse') > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse_nondeterministic' AND column = 's' AND active;

SELECT 'a draw per row';
SELECT uniqExact(rand(s)) > 99000 FROM t_sparse_nondeterministic;
SELECT uniqExact(generateUUIDv4(s)) > 99000 FROM t_sparse_nondeterministic;
SELECT abs(count() - 50000) < 2000 FROM t_sparse_nondeterministic WHERE rand(s) % 2 = 0;

SELECT 'the same for a LowCardinality argument';
SELECT uniqExact(rand(toLowCardinality(if(number % 100 = 0, number + 1, 0)))) > 99000 FROM numbers(100000);

SELECT 'a deterministic function still reads the sparse column once per value';
SELECT sum(s + 1) FROM t_sparse_nondeterministic;
SELECT uniqExact(toString(s)) FROM t_sparse_nondeterministic;

DROP TABLE t_sparse_nondeterministic;
