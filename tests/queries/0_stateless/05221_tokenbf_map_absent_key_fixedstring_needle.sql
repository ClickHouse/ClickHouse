-- `m['missing'] = CAST('', 'FixedString(N)')` matches every row without the key: `arrayElement` returns '' for
-- an absent key, and `String = FixedString(N)` ignores the zero padding of the constant. The `mapKeys` bloom
-- filter index must not be used for such a comparison, otherwise every granule is pruned and the rows are lost.
-- The default value of `FixedString(N)` is reported as '', while the constant holds N zero bytes, so the guard
-- has to compare the constant without its padding.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_tokenbf;
DROP TABLE IF EXISTS t_ngrambf;

CREATE TABLE t_tokenbf (id UInt64, attrs Map(String, String), INDEX idx mapKeys(attrs) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_tokenbf SELECT number, map(if(number = 500, 'entity', 'other'), 'v') FROM numbers(1024);

CREATE TABLE t_ngrambf (id UInt64, attrs Map(String, String), INDEX idx mapKeys(attrs) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_ngrambf SELECT number, map(if(number = 500, 'entityword', 'otherword'), 'v') FROM numbers(1024);

SELECT '-- absent key, empty FixedString needle: all rows match';
SET optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_tokenbf WHERE attrs['missing'] = CAST('', 'FixedString(3)');
SELECT count() FROM t_tokenbf WHERE attrs['missing'] = CAST('', 'LowCardinality(FixedString(3))');
SELECT count() FROM t_tokenbf WHERE attrs['missing'] = CAST('', 'Nullable(FixedString(3))');
SELECT count() FROM t_ngrambf WHERE attrs['missing'] = CAST('', 'FixedString(3)');
SET optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_tokenbf WHERE attrs['missing'] = CAST('', 'FixedString(3)');
SELECT count() FROM t_tokenbf WHERE attrs['missing'] = CAST('', 'LowCardinality(FixedString(3))');
SELECT count() FROM t_tokenbf WHERE attrs['missing'] = CAST('', 'Nullable(FixedString(3))');
SELECT count() FROM t_ngrambf WHERE attrs['missing'] = CAST('', 'FixedString(3)');

SELECT '-- present key, padded FixedString needle: the index is used and the row is found';
SELECT count(), min(id) FROM t_tokenbf WHERE attrs['entity'] = CAST('v', 'FixedString(3)');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE attrs['entity'] = CAST('v', 'FixedString(3)')) WHERE explain ILIKE '%Granules: 1/128%';
SET optimize_functions_to_subcolumns = 0;
SELECT count(), min(id) FROM t_tokenbf WHERE attrs['entity'] = CAST('v', 'FixedString(3)');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE attrs['entity'] = CAST('v', 'FixedString(3)')) WHERE explain ILIKE '%Granules: 1/128%';

DROP TABLE t_tokenbf;
DROP TABLE t_ngrambf;
