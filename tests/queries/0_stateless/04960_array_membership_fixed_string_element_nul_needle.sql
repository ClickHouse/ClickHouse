-- Each row prints the function result next to an `arrayExists(x -> x = needle, v)` oracle.
-- The two columns must be equal, except where a comment says otherwise.
SET optimize_rewrite_array_exists_to_has = 0;

DROP TABLE IF EXISTS fs;
CREATE TABLE fs (v Array(FixedString(4))) ENGINE = Memory;
INSERT INTO fs SELECT [CAST('a', 'FixedString(4)')];

SELECT has(v, unhex('6100')), arrayExists(x -> x = unhex('6100'), v) FROM fs;
SELECT has(v, materialize(unhex('6100'))), arrayExists(x -> x = materialize(unhex('6100')), v) FROM fs;
SELECT indexOf(v, unhex('6100')), arrayExists(x -> x = unhex('6100'), v) FROM fs;
SELECT countEqual(v, unhex('6100')), arrayExists(x -> x = unhex('6100'), v) FROM fs;
SELECT indexOfAssumeSorted(v, unhex('6100')), arrayExists(x -> x = unhex('6100'), v) FROM fs;
SELECT notHas(v, unhex('6100')), NOT arrayExists(x -> x = unhex('6100'), v) FROM fs;
SELECT has(v, toNullable(unhex('6100'))), arrayExists(x -> x = toNullable(unhex('6100')), v) FROM fs;

-- A needle whose tail is not a zero byte is a different value and must not match.
SELECT has(v, 'abcd'), arrayExists(x -> x = 'abcd', v) FROM fs;
SELECT has(v, unhex('6100000001')), arrayExists(x -> x = unhex('6100000001'), v) FROM fs;

-- Needles that already matched before, both narrower and wider than the element.
SELECT has(v, 'a'), arrayExists(x -> x = 'a', v) FROM fs;
SELECT has(v, toFixedString('a', 4)), arrayExists(x -> x = toFixedString('a', 4), v) FROM fs;
SELECT has(v, materialize(toFixedString('a', 2))), arrayExists(x -> x = materialize(toFixedString('a', 2)), v) FROM fs;
SELECT has(v, toFixedString('a', 6)), arrayExists(x -> x = toFixedString('a', 6), v) FROM fs;

-- The reported position and the count must be the real ones, not just non-zero.
DROP TABLE IF EXISTS fs_repeat;
CREATE TABLE fs_repeat (v Array(FixedString(4))) ENGINE = Memory;
INSERT INTO fs_repeat SELECT [CAST('z', 'FixedString(4)'), CAST('a', 'FixedString(4)'), CAST('a', 'FixedString(4)')];
SELECT indexOf(v, unhex('6100')), 2 FROM fs_repeat;
SELECT countEqual(v, unhex('6100')), 2 FROM fs_repeat;

-- indexOfAssumeSorted over an array sorted by its stored bytes.
DROP TABLE IF EXISTS fs_sorted;
CREATE TABLE fs_sorted (v Array(FixedString(4))) ENGINE = Memory;
INSERT INTO fs_sorted SELECT [CAST('aa', 'FixedString(4)'), CAST('ab', 'FixedString(4)'), CAST('b', 'FixedString(4)')];
SELECT indexOfAssumeSorted(v, unhex('6161')), indexOf(v, unhex('6161')) FROM fs_sorted;
SELECT indexOfAssumeSorted(v, unhex('6162')), indexOf(v, unhex('6162')) FROM fs_sorted;
SELECT indexOfAssumeSorted(v, unhex('62')), indexOf(v, unhex('62')) FROM fs_sorted;

-- A needle read from a column, and a per-row needle that matches in one row only.
DROP TABLE IF EXISTS fs_needle_col;
CREATE TABLE fs_needle_col (v Array(FixedString(4)), s String, f FixedString(2)) ENGINE = Memory;
INSERT INTO fs_needle_col SELECT [CAST('a', 'FixedString(4)')], unhex('6100'), toFixedString('a', 2);
INSERT INTO fs_needle_col SELECT [CAST('b', 'FixedString(4)')], unhex('6100'), toFixedString('a', 2);
SELECT has(v, s), arrayExists(x -> x = s, v) FROM fs_needle_col ORDER BY v[1];
SELECT has(v, f), arrayExists(x -> x = f, v) FROM fs_needle_col ORDER BY v[1];

-- Elements that are all zero bytes, or carry a zero byte in the middle.
DROP TABLE IF EXISTS fs_zero;
CREATE TABLE fs_zero (v Array(FixedString(4))) ENGINE = Memory;
INSERT INTO fs_zero SELECT [CAST('', 'FixedString(4)')];
SELECT has(v, unhex('00')), arrayExists(x -> x = unhex('00'), v) FROM fs_zero;
SELECT has(v, ''), arrayExists(x -> x = '', v) FROM fs_zero;

DROP TABLE IF EXISTS fs_interior;
CREATE TABLE fs_interior (v Array(FixedString(4))) ENGINE = Memory;
INSERT INTO fs_interior SELECT [CAST(unhex('61006200'), 'FixedString(4)')];
SELECT has(v, unhex('61006200')), arrayExists(x -> x = unhex('61006200'), v) FROM fs_interior;
SELECT has(v, unhex('610062')), arrayExists(x -> x = unhex('610062'), v) FROM fs_interior;
SELECT has(v, unhex('6100620000')), arrayExists(x -> x = unhex('6100620000'), v) FROM fs_interior;

-- Widths other than 4.
DROP TABLE IF EXISTS fs3;
CREATE TABLE fs3 (v Array(FixedString(3))) ENGINE = Memory;
INSERT INTO fs3 SELECT [CAST('foo', 'FixedString(3)')];
SELECT has(v, 'foo'), arrayExists(x -> x = 'foo', v) FROM fs3;
SELECT has(v, toFixedString('foo', 5)), arrayExists(x -> x = toFixedString('foo', 5), v) FROM fs3;
SELECT has(v, unhex('666f6f00')), arrayExists(x -> x = unhex('666f6f00'), v) FROM fs3;

-- LowCardinality element, in both needle spellings.
DROP TABLE IF EXISTS fs_lc;
CREATE TABLE fs_lc (v Array(LowCardinality(FixedString(4)))) ENGINE = Memory;
INSERT INTO fs_lc SELECT [CAST('a', 'FixedString(4)')];
SELECT has(v, unhex('6100')), arrayExists(x -> x = unhex('6100'), v) FROM fs_lc;
SELECT has(v, materialize(unhex('6100'))), arrayExists(x -> x = materialize(unhex('6100')), v) FROM fs_lc;

-- Nullable element.
DROP TABLE IF EXISTS fs_null;
CREATE TABLE fs_null (v Array(Nullable(FixedString(4)))) ENGINE = Memory;
INSERT INTO fs_null SELECT [NULL, CAST('a', 'FixedString(4)')];
SELECT has(v, unhex('6100')), arrayExists(x -> x = unhex('6100'), v) FROM fs_null;
SELECT indexOf(v, unhex('6100')), 2 FROM fs_null;

-- A NULL needle matches only a NULL element, never the element that is all zero bytes.
DROP TABLE IF EXISTS fs_null_needle;
CREATE TABLE fs_null_needle (v Array(FixedString(4)), n Nullable(String)) ENGINE = Memory;
INSERT INTO fs_null_needle SELECT [CAST('', 'FixedString(4)')], NULL;
SELECT has(v, n), arrayExists(x -> x = n, v) FROM fs_null_needle;
SELECT indexOf(v, n), 0 FROM fs_null_needle;
SELECT countEqual(v, n), 0 FROM fs_null_needle;

DROP TABLE IF EXISTS fs_null_both;
CREATE TABLE fs_null_both (v Array(Nullable(FixedString(4))), n Nullable(String)) ENGINE = Memory;
INSERT INTO fs_null_both SELECT [CAST('', 'FixedString(4)')], NULL;
INSERT INTO fs_null_both SELECT [NULL], NULL;
SELECT has(v, n), 0 FROM fs_null_both WHERE isNotNull(v[1]);
SELECT has(v, n), 1 FROM fs_null_both WHERE isNull(v[1]);

-- Map keys and values reach the same comparison.
DROP TABLE IF EXISTS fs_map_key;
CREATE TABLE fs_map_key (m Map(FixedString(4), UInt8)) ENGINE = Memory;
INSERT INTO fs_map_key SELECT map(CAST('a', 'FixedString(4)'), 1);
SELECT mapContainsKey(m, unhex('6100')), arrayExists(x -> x = unhex('6100'), mapKeys(m)) FROM fs_map_key;
SELECT has(m, unhex('6100')), arrayExists(x -> x = unhex('6100'), mapKeys(m)) FROM fs_map_key;

DROP TABLE IF EXISTS fs_map_value;
CREATE TABLE fs_map_value (m Map(UInt8, FixedString(4))) ENGINE = Memory;
INSERT INTO fs_map_value SELECT map(1, CAST('a', 'FixedString(4)'));
SELECT mapContainsValue(m, unhex('6100')), arrayExists(x -> x = unhex('6100'), mapValues(m)) FROM fs_map_value;

-- A skip index must select the same rows the query returns without it.
DROP TABLE IF EXISTS fs_index;
CREATE TABLE fs_index (id UInt8, v Array(FixedString(4)), INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO fs_index SELECT 1, [CAST('a', 'FixedString(4)')];
INSERT INTO fs_index SELECT 2, [CAST('b', 'FixedString(4)')];
INSERT INTO fs_index SELECT 3, [CAST('c', 'FixedString(4)')];
SELECT count() FROM fs_index WHERE has(v, materialize(unhex('6100'))) SETTINGS use_skip_indexes = 1;
SELECT count() FROM fs_index WHERE has(v, materialize(unhex('6100'))) SETTINGS use_skip_indexes = 0;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM fs_index WHERE has(v, unhex('6100')))
WHERE explain ILIKE '%Granules: 1/3%';

DROP TABLE IF EXISTS fs;
DROP TABLE IF EXISTS fs_repeat;
DROP TABLE IF EXISTS fs_sorted;
DROP TABLE IF EXISTS fs_needle_col;
DROP TABLE IF EXISTS fs_zero;
DROP TABLE IF EXISTS fs_interior;
DROP TABLE IF EXISTS fs3;
DROP TABLE IF EXISTS fs_lc;
DROP TABLE IF EXISTS fs_null;
DROP TABLE IF EXISTS fs_null_needle;
DROP TABLE IF EXISTS fs_null_both;
DROP TABLE IF EXISTS fs_map_key;
DROP TABLE IF EXISTS fs_map_value;
DROP TABLE IF EXISTS fs_index;
