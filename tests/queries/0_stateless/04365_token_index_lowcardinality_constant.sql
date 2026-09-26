-- Token-search skip indexes (text, tokenbf_v1, ngrambf_v1) must prune granules when the
-- constant needle is wrapped in LowCardinality, e.g. hasToken(s, toLowCardinality('rare')).
-- Before the fix the LowCardinality-typed constant failed the string-type gate, no index
-- condition was built, and the query silently degraded to a full scan (all granules read),
-- while a plain-String needle pruned to a single granule. Each assertion below checks that
-- the LowCardinality variant reads the SAME single granule as the plain-String variant.

SET enable_analyzer = 1;
-- A bare count() over a text-index predicate is answered from the posting lists
-- (ReadFromTextIndexCount), leaving no ReadFromMergeTree node whose granule count to assert.
SET query_plan_optimize_count_from_text_index = 0;

DROP TABLE IF EXISTS t_text;
DROP TABLE IF EXISTS t_tokenbf;
DROP TABLE IF EXISTS t_ngrambf;

CREATE TABLE t_text (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_text SELECT number, concat('common ', if(number = 500, 'rare', 'x'), ' tail') FROM numbers(1024);

CREATE TABLE t_tokenbf (id UInt64, s String, INDEX idx s TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_tokenbf SELECT number, concat('common ', if(number = 500, 'rare', 'x'), ' tail') FROM numbers(1024);

CREATE TABLE t_ngrambf (id UInt64, s String, INDEX idx s TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_ngrambf SELECT number, concat('common ', if(number = 500, 'rareword', 'xxxxword'), ' tail') FROM numbers(1024);

SELECT 'text hasToken String';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE hasToken(s, 'rare')) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'text hasToken LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE hasToken(s, toLowCardinality('rare'))) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'text equals LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE s = toLowCardinality('common rare tail')) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'text hasToken LowCardinality(Nullable)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE hasToken(s, CAST('rare', 'LowCardinality(Nullable(String))'))) WHERE explain ILIKE '%Granules: 1/128%';

SELECT 'tokenbf hasToken String';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE hasToken(s, 'rare')) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'tokenbf hasToken LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE hasToken(s, toLowCardinality('rare'))) WHERE explain ILIKE '%Granules: 1/128%';

SELECT 'ngrambf hasToken String';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE hasToken(s, 'rareword')) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'ngrambf hasToken LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE hasToken(s, toLowCardinality('rareword'))) WHERE explain ILIKE '%Granules: 1/128%';

-- Results must stay correct: the LowCardinality needle finds exactly the seeded row.
SELECT 'correctness';
SELECT count(), min(id) FROM t_text WHERE hasToken(s, toLowCardinality('rare'));

-- Map-key lookup on a `mapKeys(...)` text index: `attrs[key] = ...` / `attrs[key] IN (...)`.
-- The map-element key path also gated the constant key with a raw-type string check, so a
-- LowCardinality key degraded to a full scan there too. Disable subcolumn folding so the
-- `arrayElement` map-key branch is exercised directly. Each LowCardinality variant must prune
-- to the SAME single granule as the plain-String key.
SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_map;
CREATE TABLE t_map (id UInt64, attrs Map(String, String), INDEX idx mapKeys(attrs) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_map SELECT number, map(if(number = 500, 'entity', 'other'), 'v') FROM numbers(1024);

SELECT 'mapkey equals String';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map WHERE attrs['entity'] = 'v') WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'mapkey equals LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map WHERE attrs[toLowCardinality('entity')] = 'v') WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'mapkey in LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map WHERE attrs[toLowCardinality('entity')] IN ('v')) WHERE explain ILIKE '%Granules: 1/128%';

SELECT 'mapkey correctness';
SELECT count(), min(id) FROM t_map WHERE attrs[toLowCardinality('entity')] = 'v';

-- Bloom-filter sibling (MergeTreeIndexBloomFilterText.cpp) of the map-key path: a
-- `mapKeys(...)` tokenbf_v1/ngrambf_v1 index. tryGetConstant strips only an outer Nullable,
-- so a LowCardinality(Nullable(String)) key survived as Nullable(String) after
-- removeLowCardinality, failed the raw string-type gate and degraded to a full scan. The key
-- must be stripped of LowCardinality then Nullable (non-null only) before the gate. Each
-- variant must prune to the SAME single granule as the plain-String key.
DROP TABLE IF EXISTS t_map_tokenbf;
CREATE TABLE t_map_tokenbf (id UInt64, attrs Map(String, String), INDEX idx mapKeys(attrs) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_map_tokenbf SELECT number, map(if(number = 500, 'entity', 'other'), 'v') FROM numbers(1024);

SELECT 'mapkey tokenbf equals String';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_tokenbf WHERE attrs['entity'] = 'v') WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'mapkey tokenbf equals LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_tokenbf WHERE attrs[toLowCardinality('entity')] = 'v') WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'mapkey tokenbf equals LowCardinality(Nullable)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_tokenbf WHERE attrs[CAST('entity', 'LowCardinality(Nullable(String))')] = 'v') WHERE explain ILIKE '%Granules: 1/128%';

DROP TABLE IF EXISTS t_map_ngrambf;
CREATE TABLE t_map_ngrambf (id UInt64, attrs Map(String, String), INDEX idx mapKeys(attrs) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_map_ngrambf SELECT number, map(if(number = 500, 'entityword', 'otherword'), 'v') FROM numbers(1024);

SELECT 'mapkey ngrambf equals LowCardinality(Nullable)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_ngrambf WHERE attrs[CAST('entityword', 'LowCardinality(Nullable(String))')] = 'v') WHERE explain ILIKE '%Granules: 1/128%';

SELECT 'mapkey tokenbf correctness';
SELECT count(), min(id) FROM t_map_tokenbf WHERE attrs[CAST('entity', 'LowCardinality(Nullable(String))')] = 'v';

-- Absent-key lookup with an empty LowCardinality(Nullable(String)) needle. arrayElement returns
-- the map default ('') for a missing key, so every row matches. The absent-key guard must compare
-- against the UNWRAPPED default (''), not the raw LC(Nullable) default (NULL); otherwise the guard
-- does not fire, a granule-skipping bloom condition is built, and rows are wrongly lost (false
-- negative). Expect all 1024 rows.
SELECT 'mapkey tokenbf absent-key empty LC(Nullable)';
SELECT count() FROM t_map_tokenbf WHERE attrs['missing'] = CAST('', 'LowCardinality(Nullable(String))');

-- The same absent-key lookup reaches a SECOND, separate guard once subcolumn folding is on:
-- `attrs['missing']` is rewritten to a `map.key_*` subcolumn reference, whose branch carries its
-- own default comparison. Expect all 1024 rows.
SET optimize_functions_to_subcolumns = 1;
SELECT 'mapkey tokenbf absent-key empty LC(Nullable), subcolumn';
SELECT count() FROM t_map_tokenbf WHERE attrs['missing'] = CAST('', 'LowCardinality(Nullable(String))');

-- A FixedString needle carries NUL padding that string equality ignores but a tokenizer does not,
-- so a LowCardinality(FixedString) constant must stay wrapped and be declined here rather than
-- reaching the index condition. Results must be complete whether or not an index is consulted.
CREATE TABLE t_fixed_ngrambf (id UInt64, s String, INDEX idx s TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_fixed_ngrambf SELECT number, if(number = 500, 'rareword', 'xxxxword') FROM numbers(1024);
SELECT 'ngrambf equals LC(FixedString)';
SELECT count(), min(id) FROM t_fixed_ngrambf WHERE s = toLowCardinality(toFixedString('rareword', 12));

CREATE TABLE t_fixed_text (id UInt64, s String, INDEX idx s TYPE text(tokenizer = ngrams(3)) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_fixed_text SELECT number, if(number = 500, 'rareword', 'xxxxword') FROM numbers(1024);
SELECT 'text equals LC(FixedString)';
SELECT count(), min(id) FROM t_fixed_text WHERE s = toLowCardinality(toFixedString('rareword', 12));

-- Both absent-key guards must likewise stay unreachable for that needle, in either folding mode.
SELECT 'mapkey tokenbf absent-key empty LC(FixedString), subcolumn';
SELECT count() FROM t_map_tokenbf WHERE attrs['missing'] = CAST('', 'LowCardinality(FixedString(3))');
SET optimize_functions_to_subcolumns = 0;
SELECT 'mapkey tokenbf absent-key empty LC(FixedString)';
SELECT count() FROM t_map_tokenbf WHERE attrs['missing'] = CAST('', 'LowCardinality(FixedString(3))');

-- Bloom-filter value gate with a non-null LowCardinality(Nullable(String)) needle. The map sections
-- above put the wrapper on the KEY, which a different gate decides, so assert pruning here too.
SELECT 'tokenbf hasToken LowCardinality(Nullable)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE hasToken(s, CAST('rare', 'LowCardinality(Nullable(String))'))) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'ngrambf hasToken LowCardinality(Nullable)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE hasToken(s, CAST('rareword', 'LowCardinality(Nullable(String))'))) WHERE explain ILIKE '%Granules: 1/128%';

-- `hasToken` rejects a needle holding a token separator, and unwrapping must not extend the index to
-- such a needle: pruning the granule that owes the exception would turn the error into an empty result.
-- A plain-String needle is not unwrapped, so it keeps pruning the granule away.
SELECT 'tokenbf hasToken separator needle: String prunes, LowCardinality raises';
SELECT count() FROM t_tokenbf WHERE hasToken(s, 'bad needle');
SELECT count() FROM t_tokenbf WHERE hasToken(s, toLowCardinality('bad needle')); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_fixed_ngrambf;
DROP TABLE t_fixed_text;
DROP TABLE t_text;
DROP TABLE t_tokenbf;
DROP TABLE t_ngrambf;
DROP TABLE t_map;
DROP TABLE t_map_tokenbf;
DROP TABLE t_map_ngrambf;
