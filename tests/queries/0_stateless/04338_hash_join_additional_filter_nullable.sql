-- Regression test for a LOGICAL_ERROR (IColumn::assertTypeEquality) in
-- HashJoin::buildAdditionalFilter. A non-equi ON conjunct (l.k2 != r.k2) becomes a
-- residual (mixed) filter whose required columns reference the raw, non-Nullable right
-- inputs, while under join_use_nulls = 1 the right side of a key-value-storage join
-- (JoinStepLogicalLookup) reaches the saved join block already promoted to Nullable. A
-- dictionary is a key-value entity, so it reproduces the crash without EmbeddedRocksDB and
-- runs in normal/fasttest builds. join_algorithm = 'hash' forces the HashJoin path.

DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS dict_src;
DROP TABLE IF EXISTS l;

CREATE TABLE dict_src (k1 String, k2 String, val UInt32) ENGINE = Memory;
INSERT INTO dict_src VALUES ('foo', 'bar', 10), ('foo', 'baz', 20), ('xyz', 'abc', 30);

CREATE DICTIONARY dict (k1 String, k2 String, val UInt32)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'dict_src'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0);

CREATE TABLE l (k1 String, k2 String, val UInt32) ENGINE = Memory;
INSERT INTO l VALUES ('foo', 'bar', 1), ('foo', 'baz', 2), ('qux', 'bar', 3);

SET join_algorithm = 'hash';

SELECT l.k1, l.k2, r.k1, r.k2 FROM l AS l
LEFT JOIN dict AS r ON l.k1 = r.k1 AND l.k2 != r.k2
ORDER BY l.k1, l.k2 SETTINGS join_use_nulls = 1;

SELECT COUNT(DISTINCT *) FROM l AS l
LEFT JOIN dict AS r ON l.k1 = r.k1 AND l.k2 != r.k2 SETTINGS join_use_nulls = 1;

DROP DICTIONARY dict;
DROP TABLE dict_src;
DROP TABLE l;
