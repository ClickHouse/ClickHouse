-- Random settings limits: optimize_inverse_dictionary_lookup=(1, None)
-- `optimize_inverse_dictionary_lookup` must rewrite every reference to a `GROUP BY` alias the same way, also when the
-- rewritten comparison contains a nested `dictGet` or a comparison that folds to a constant.

DROP DICTIONARY IF EXISTS d1;
DROP DICTIONARY IF EXISTS d2;
DROP DICTIONARY IF EXISTS d3;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS src1;
DROP TABLE IF EXISTS src2;
DROP TABLE IF EXISTS src3;

CREATE TABLE t2 (s String, n UInt64) ENGINE = MergeTree ORDER BY s;
INSERT INTO t2 SELECT toString(number % 3), number FROM numbers(12);
CREATE TABLE src2 (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src2 SELECT 1, 7;
CREATE TABLE src1 (id UInt64, a String) ENGINE = MergeTree ORDER BY id;
INSERT INTO src1 SELECT 7, 'x';
CREATE TABLE src3 (id UInt64, flag UInt8 DEFAULT 5) ENGINE = MergeTree ORDER BY id;
INSERT INTO src3 VALUES (1, 0), (2, 1);
CREATE DICTIONARY d2 (id UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src2')) LAYOUT(HASHED()) LIFETIME(0);
CREATE DICTIONARY d1 (id UInt64, a String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src1')) LAYOUT(HASHED()) LIFETIME(0);
CREATE DICTIONARY d3 (id UInt64, flag UInt8 DEFAULT 5) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src3')) LAYOUT(HASHED()) LIFETIME(0);

-- Nested `dictGet` chain.
SELECT NOT (dictGet('d1', 'a', dictGet('d2', 'b', n)) = 'x') AS r FROM t2 GROUP BY r ORDER BY r;
SELECT NOT (dictGet('d1', 'a', dictGet('d2', 'b', n)) = 'x') AS r, count() FROM t2 GROUP BY r HAVING r OR NOT r ORDER BY r;
SELECT NOT (dictGet('d1', 'a', dictGet('d2', 'b', n)) = 'x') AS r, count() FROM t2 GROUP BY r WITH ROLLUP ORDER BY r, count();
SELECT dictGet('d1', 'a', dictGet('d2', 'b', n)) = 'x' AS r, count() FROM t2 GROUP BY r WITH ROLLUP ORDER BY r SETTINGS group_by_use_nulls = 1;

-- The inner comparison folds to a constant, which makes the outer one rewritable.
SELECT dictGet('d3', 'flag', n) = (dictGet('d1', 'a', n) = 'zzz') AS r, count() FROM t2 GROUP BY r ORDER BY r;

-- Two different keys must stay different after the rewrite, or `ROLLUP` loses a grouping level.
SELECT NOT (dictGet('d1', 'a', dictGet('d2', 'b', n)) = 'x') AS r1, NOT (dictGet('d2', 'b', n) = 7) AS r2, grouping(r1, r2) AS g, count()
FROM t2 GROUP BY ROLLUP(r1, r2) ORDER BY g, r1, r2;

DROP DICTIONARY d1;
DROP DICTIONARY d2;
DROP DICTIONARY d3;
DROP TABLE t2;
DROP TABLE src1;
DROP TABLE src2;
DROP TABLE src3;
