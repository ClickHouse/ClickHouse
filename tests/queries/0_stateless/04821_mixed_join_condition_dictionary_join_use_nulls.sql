-- With `join_use_nulls = 1`, a mixed (cross-side non-equi) JOIN ON condition over a dictionary
-- fell back from a direct join to a hash join whose stored right block wraps the attribute into
-- Nullable, while the mixed condition was built against the plain input type. The residual filter
-- then read the column through the wrong type and matched arbitrary rows (issue #113931).
SET enable_analyzer = 1;
SET allow_experimental_join_condition = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS t3;
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS dsrc;

CREATE TABLE t3 (key UInt64, a UInt32) ENGINE = Memory;
INSERT INTO t3 VALUES (1, 1), (1, 2), (2, 3), (3, 4);
CREATE TABLE dsrc (key UInt64, a UInt32) ENGINE = Memory;
INSERT INTO dsrc VALUES (1, 30), (2, 20);
CREATE DICTIONARY dict (key UInt64, a UInt32) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'dsrc')) LIFETIME(0) LAYOUT(FLAT());

-- Of the key-matched pairs, only (1, 1) and (1, 2) satisfy the residual (10 < 30, 20 < 30;
-- for (2, 3) the residual 30 < 20 is false), so sum(d.a) = 60 and the other rows are NULL-extended.
SELECT 'LEFT ANY hash', count(), sum(d.a) FROM t3 LEFT ANY JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a)
SETTINGS join_algorithm = 'hash';
SELECT 'LEFT ANY parallel_hash', count(), sum(d.a) FROM t3 LEFT ANY JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a)
SETTINGS join_algorithm = 'parallel_hash';
SELECT 'LEFT ALL hash', count(), sum(d.a) FROM t3 LEFT JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a)
SETTINGS join_algorithm = 'hash';
SELECT 'INNER hash', count(), sum(d.a) FROM t3 INNER JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a)
SETTINGS join_algorithm = 'hash';
SELECT 'LEFT SEMI hash', count() FROM t3 LEFT SEMI JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a)
SETTINGS join_algorithm = 'hash';
SELECT 'LEFT ANTI hash', count() FROM t3 LEFT ANTI JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a)
SETTINGS join_algorithm = 'hash';

-- The unmatched rows are NULL-extended, not filled with the attribute default.
SELECT t3.key, t3.a, d.a FROM t3 LEFT JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a)
ORDER BY t3.key, t3.a
SETTINGS join_algorithm = 'hash';

DROP DICTIONARY dict;
DROP TABLE dsrc;
DROP TABLE t3;
