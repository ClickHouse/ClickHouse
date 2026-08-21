SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_text;
DROP TABLE IF EXISTS t_tokenbf;
DROP TABLE IF EXISTS t_ngrambf;
DROP TABLE IF EXISTS t_null_set;
DROP TABLE IF EXISTS t_free_set;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_normalized;
DROP TABLE IF EXISTS t_preprocessed_wrapped;
DROP TABLE IF EXISTS t_preprocessed_plain;
DROP TABLE IF EXISTS t_preprocessed_map;
DROP TABLE IF EXISTS t_preprocessed_folded;
DROP TABLE IF EXISTS t_map_fixed;
DROP TABLE IF EXISTS t_map_nullable;

CREATE TABLE t_text (x Nullable(String), INDEX i x TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_text SELECT if(number % 100 = 7, NULL, 'word' || toString(number)) FROM numbers(1000);

CREATE TABLE t_tokenbf (a Nullable(String), b String, INDEX i b TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_tokenbf SELECT if(number % 100 = 7, NULL, 'a' || toString(number)), 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_ngrambf (a Nullable(String), b String, INDEX i b TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_ngrambf SELECT if(number % 100 = 7, NULL, 'a' || toString(number)), 'word' || toString(number) FROM numbers(1000);

SELECT 'text', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'text global', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1 global', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'ngrambf_v1', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'ngrambf_v1 global', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE b GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';

-- Pruning the wrong granules would keep the counts above but change the rows, so compare the two
-- settings on each family.
SELECT 'text rows', (SELECT count() FROM t_text WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_text WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);
SELECT 'tokenbf_v1 rows', (SELECT count() FROM t_tokenbf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tokenbf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);
SELECT 'ngrambf_v1 rows', (SELECT count() FROM t_ngrambf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_ngrambf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);

-- `globalIn` was admitted but never mapped to an RPN function, so it did not prune even at
-- transform_null_in = 0.
SELECT 'tokenbf_v1 global, transform_null_in = 0', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%';

-- All four `NOT IN` spellings now reach the index. A bare `NOT IN` cannot prune on its own, since
-- `FUNCTION_NOT_IN` is always allowed to be true, so assert it is used rather than that it prunes,
-- and assert pruning under the negation that cancels it. `NOT (x NOT IN ...)` folds to `nullIn`, so
-- the `globalNotNullIn` spelling is the one that exercises the negated branch.
SELECT 'tokenbf_v1 not null in used', count() FROM t_tokenbf WHERE b NOT IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'tokenbf_v1 global not in used', count() FROM t_tokenbf WHERE b GLOBAL NOT IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 0;
SELECT 'tokenbf_v1 global not null in used', count() FROM t_tokenbf WHERE b GLOBAL NOT IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'ngrambf_v1 not null in used', count() FROM t_ngrambf WHERE b NOT IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'tokenbf_v1 negated global not null in', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE NOT (b GLOBAL NOT IN ('word5', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1 not in rows', (SELECT count() FROM t_tokenbf WHERE b NOT IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tokenbf WHERE b NOT IN ('word5', 'word500') SETTINGS transform_null_in = 1);
SELECT 'tokenbf_v1 negated not in rows', (SELECT count() FROM t_tokenbf WHERE NOT (b GLOBAL NOT IN ('word5', 'word500')) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_tokenbf WHERE NOT (b GLOBAL NOT IN ('word5', 'word500')) SETTINGS transform_null_in = 1, use_skip_indexes = 0);

-- A NULL in the set matches the column's NULL rows, which the indexes cannot express, so the index
-- must be refused and all NULL rows returned.
SELECT count() FROM t_text WHERE x IN ('word5', NULL) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1; -- { serverError INDEX_NOT_USED }
SELECT 'text null in set', count() FROM t_text WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 1;
SELECT 'text null in set, transform_null_in = 0', count() FROM t_text WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 0;

-- The token bloom filter family rejects a `Nullable` index column, so its per-row refusal is
-- reached through a subquery set, whose element type keeps the wrapper.
CREATE TABLE t_null_set (v Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null_set VALUES ('word5'), (NULL);

SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_null_set) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1; -- { serverError INDEX_NOT_USED }
SELECT 'tokenbf_v1 null in subquery set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1 null in subquery set rows', (SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'ngrambf_v1 null in subquery set rows', (SELECT count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);

-- A null-free subquery set over the same `Nullable` element type must still prune.
CREATE TABLE t_free_set (v Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_free_set VALUES ('word5'), ('word500');
SELECT 'tokenbf_v1 null-free subquery set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_free_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';

-- A `FixedString` element arrives padded with NUL bytes. An n-gram tokenizer emits windows over
-- them, so the element requires trigrams no `String` granule stored and the index must be refused.
CREATE TABLE t_fixed_set (v FixedString(12)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_fixed_set VALUES (toFixedString('word5', 12));

SELECT count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1; -- { serverError INDEX_NOT_USED }
SELECT 'ngrambf_v1 fixed set rows', (SELECT count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'ngrambf_v1 fixed set count', count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1;
SELECT 'ngrambf_v1 fixed set rows, transform_null_in = 0', (SELECT count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0, use_skip_indexes = 0);

-- `splitByNonAlpha` already dropped the padding as a separator, so this pairing pruned correctly
-- before and must keep doing so.
SELECT 'tokenbf_v1 fixed set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1 fixed set rows', (SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);

-- The same holds for the text index over an n-gram tokenizer.
CREATE TABLE t_text_ngrams (x String, INDEX i x TYPE text(tokenizer = ngrams(3))) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_text_ngrams SELECT 'word' || toString(number) FROM numbers(1000);

SELECT count() FROM t_text_ngrams WHERE x IN (SELECT v FROM t_fixed_set) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1; -- { serverError INDEX_NOT_USED }
SELECT 'text fixed set rows', (SELECT count() FROM t_text_ngrams WHERE x IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_text_ngrams WHERE x IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'text fixed set count', count() FROM t_text_ngrams WHERE x IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1;
SELECT 'text fixed set rows, transform_null_in = 0', (SELECT count() FROM t_text_ngrams WHERE x IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_text_ngrams WHERE x IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0, use_skip_indexes = 0);

-- An equal `FixedString` width is the same representation, so it still prunes; an unequal one pads
-- differently and deliberately falls back to a full read.
CREATE TABLE t_fixed_key (b FixedString(12), INDEX i b TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_fixed_key SELECT toFixedString('word' || toString(number), 12) FROM numbers(1000);
SELECT 'ngrambf_v1 fixed key, fixed set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fixed_key WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'ngrambf_v1 fixed key, fixed set rows', (SELECT count() FROM t_fixed_key WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_fixed_key WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);

CREATE TABLE t_fixed_key_narrow (b FixedString(6), INDEX i b TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_fixed_key_narrow SELECT toFixedString('wor' || toString(number), 6) FROM numbers(1000);
CREATE TABLE t_fixed_set_narrow (v FixedString(12)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_fixed_set_narrow VALUES (toFixedString('wor5', 12));
SELECT 'ngrambf_v1 narrow key, wide set rows', (SELECT count() FROM t_fixed_key_narrow WHERE b IN (SELECT v FROM t_fixed_set_narrow) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_fixed_key_narrow WHERE b IN (SELECT v FROM t_fixed_set_narrow) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'ngrambf_v1 narrow key, wide set count', count() FROM t_fixed_key_narrow WHERE b IN (SELECT v FROM t_fixed_set_narrow) SETTINGS transform_null_in = 1;

-- `IPv6` is stored in its binary form, which shares no tokens with the textual spelling a `String`
-- set carries, so the index must be refused rather than matched across representations.
CREATE TABLE t_ipv6 (b IPv6, INDEX i b TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_ipv6 SELECT toIPv6('2001:db8::' || hex(number)) FROM numbers(1000);
CREATE TABLE t_ipv6_text_set (v String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ipv6_text_set VALUES ('2001:db8::5');
CREATE TABLE t_ipv6_set (v IPv6) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ipv6_set VALUES (toIPv6('2001:db8::5'));

SELECT 'ipv6 text set rows', (SELECT count() FROM t_ipv6 WHERE b IN (SELECT v FROM t_ipv6_text_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_ipv6 WHERE b IN (SELECT v FROM t_ipv6_text_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'ipv6 text set count', count() FROM t_ipv6 WHERE b IN (SELECT v FROM t_ipv6_text_set) SETTINGS transform_null_in = 1;
SELECT 'ipv6 text set rows, transform_null_in = 0', (SELECT count() FROM t_ipv6 WHERE b IN (SELECT v FROM t_ipv6_text_set) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_ipv6 WHERE b IN (SELECT v FROM t_ipv6_text_set) SETTINGS transform_null_in = 0, use_skip_indexes = 0);
SELECT count() FROM t_ipv6 WHERE b IN (SELECT v FROM t_ipv6_text_set) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1; -- { serverError INDEX_NOT_USED }
SELECT 'ipv6 own set rows', (SELECT count() FROM t_ipv6 WHERE b IN (SELECT v FROM t_ipv6_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_ipv6 WHERE b IN (SELECT v FROM t_ipv6_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);

-- `array` emits the whole value as one token, so the padding is part of it and the two textual
-- representations are not interchangeable there. Every pairing must agree with the unindexed read.
CREATE TABLE t_array_fixed (b FixedString(12), INDEX i b TYPE text(tokenizer = array)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_array_fixed SELECT toFixedString('word' || toString(number), 12) FROM numbers(1000);
CREATE TABLE t_array_string (b String, INDEX i b TYPE text(tokenizer = array)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_array_string SELECT 'word' || toString(number) FROM numbers(1000);
CREATE TABLE t_string_set (v String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_string_set VALUES ('word5');

SELECT 'array fixed key, fixed set rows', (SELECT count() FROM t_array_fixed WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_array_fixed WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'array fixed key, fixed set count', count() FROM t_array_fixed WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1;
SELECT 'array fixed key, string set rows', (SELECT count() FROM t_array_fixed WHERE b IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_array_fixed WHERE b IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'array fixed key, string set count', count() FROM t_array_fixed WHERE b IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 1;
SELECT 'array fixed key, string set rows, transform_null_in = 0', (SELECT count() FROM t_array_fixed WHERE b IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_array_fixed WHERE b IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 0, use_skip_indexes = 0);
SELECT 'array string key, fixed set rows', (SELECT count() FROM t_array_string WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_array_string WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'array string key, fixed set count', count() FROM t_array_string WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1;
SELECT 'array string key, fixed set rows, transform_null_in = 0', (SELECT count() FROM t_array_string WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_array_string WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0, use_skip_indexes = 0);
SELECT 'array string key, string set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_array_string WHERE b IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';

-- A `tuple(...)` subquery yields one Tuple set column that is unpacked by position, so the element
-- types must be unpacked with it or the wrong position is compared against the index.
CREATE TABLE t_tuple_key (id UInt64, s String, INDEX i s TYPE text(tokenizer = array)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_tuple_key SELECT number, 'word' || toString(number) FROM numbers(1000);

SELECT 'tuple subquery set used', count() FROM t_tuple_key WHERE (id, s) IN (SELECT tuple(number, 'word5') FROM numbers(1000)) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'tuple subquery set rows', (SELECT count() FROM t_tuple_key WHERE (id, s) IN (SELECT tuple(number, 'word5') FROM numbers(1000)) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_tuple_key WHERE (id, s) IN (SELECT tuple(number, 'word5') FROM numbers(1000)) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'tuple subquery set unpacked rows', (SELECT count() FROM t_tuple_key WHERE (id, s) IN (SELECT number, 'word5' FROM numbers(1000)) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_tuple_key WHERE (id, s) IN (SELECT number, 'word5' FROM numbers(1000)) SETTINGS transform_null_in = 1, use_skip_indexes = 0);

-- A preprocessor runs before tokenization and can map the padding onto ordinary token bytes, so it
-- leaves no representation interchangeable even for `splitByNonAlpha`.
CREATE TABLE t_preprocessed (b String, INDEX i b TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = hex(b))) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_preprocessed SELECT if(number = 5, 'word5', 'zz' || toString(number)) FROM numbers(1000);

SELECT 'preprocessor fixed set rows', (SELECT count() FROM t_preprocessed WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_preprocessed WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'preprocessor fixed set count', count() FROM t_preprocessed WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1;
SELECT 'preprocessor fixed set rows, transform_null_in = 0', (SELECT count() FROM t_preprocessed WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_preprocessed WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0, use_skip_indexes = 0);
SELECT 'preprocessor string set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_preprocessed WHERE b IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';

-- A map-element carrier is indexed through `mapValues(map)`, so its stored type comes from that
-- header column and the same rule applies to it.
CREATE TABLE t_map (m Map(String, String), INDEX i mapValues(m) TYPE text(tokenizer = ngrams(3))) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_map SELECT map('k', if(number = 5, 'word5', 'zz' || toString(number))) FROM numbers(1000);

SELECT 'map fixed set rows', (SELECT count() FROM t_map WHERE m['k'] IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_map WHERE m['k'] IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'map fixed set count', count() FROM t_map WHERE m['k'] IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1;
SELECT 'map fixed set rows, transform_null_in = 0', (SELECT count() FROM t_map WHERE m['k'] IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_map WHERE m['k'] IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 0, use_skip_indexes = 0);
SELECT 'map string set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map WHERE m['k'] IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';

-- A preprocessor is applied to the index column under the index's declared type but to a set
-- element under `String`, so a wrapper the two do not share changes the tokens it produces.
CREATE TABLE t_preprocessed_wrapped (x Nullable(String), INDEX i x TYPE text(tokenizer = ngrams(3), preprocessor = concat(toTypeName(x), ':', x))) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_preprocessed_wrapped SELECT if(number = 5, 'word5', 'zz' || toString(number)) FROM numbers(1000);
CREATE TABLE t_preprocessed_plain (x String, INDEX i x TYPE text(tokenizer = ngrams(3), preprocessor = concat(toTypeName(x), ':', x))) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_preprocessed_plain SELECT if(number = 5, 'word5', 'zz' || toString(number)) FROM numbers(1000);

SELECT 'preprocessor wrapped key rows', (SELECT count() FROM t_preprocessed_wrapped WHERE x IN ('word5') SETTINGS transform_null_in = 1) = (SELECT count() FROM t_preprocessed_wrapped WHERE x IN ('word5') SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'preprocessor wrapped key count', count() FROM t_preprocessed_wrapped WHERE x IN ('word5') SETTINGS transform_null_in = 1;
SELECT 'preprocessor wrapped key rows, transform_null_in = 0', (SELECT count() FROM t_preprocessed_wrapped WHERE x IN ('word5') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_preprocessed_wrapped WHERE x IN ('word5') SETTINGS transform_null_in = 0, use_skip_indexes = 0);
-- An `Array` carrier's preprocessor is rewritten through `arrayMap`, so it receives the element
-- type and a `String` element keeps the two applications equal.
CREATE TABLE t_preprocessed_map (m Map(String, String), INDEX i mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(mapValues(m)))) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_preprocessed_map SELECT map('k', if(number = 5, 'word5', 'zz' || toString(number))) FROM numbers(1000);

SELECT 'preprocessor map carrier still prunes', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_preprocessed_map WHERE m['k'] IN ('word5') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'preprocessor map carrier rows', (SELECT count() FROM t_preprocessed_map WHERE m['k'] IN ('word5') SETTINGS transform_null_in = 1) = (SELECT count() FROM t_preprocessed_map WHERE m['k'] IN ('word5') SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'preprocessor plain key still prunes', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_preprocessed_plain WHERE x IN ('word5') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
-- Case folding maps bytes without consulting their type, so both applications yield the same tokens
-- whatever wrappers the carrier has. `force_data_skipping_indices` is the oracle here: a declined
-- index throws INDEX_NOT_USED, which returning the right rows cannot reveal.
CREATE TABLE t_preprocessed_folded (x LowCardinality(Nullable(String)), INDEX i x TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(x))) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_preprocessed_folded SELECT if(number = 5, 'word5', 'zz' || toString(number)) FROM numbers(1000);

SELECT 'preprocessor folded key used', count() FROM t_preprocessed_folded WHERE x IN ('word5') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'preprocessor folded key rows', (SELECT count() FROM t_preprocessed_folded WHERE x IN ('word5') SETTINGS transform_null_in = 1) = (SELECT count() FROM t_preprocessed_folded WHERE x IN ('word5') SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'preprocessor folded key mixed case rows', (SELECT count() FROM t_preprocessed_folded WHERE x IN ('WORD5') SETTINGS transform_null_in = 1) = (SELECT count() FROM t_preprocessed_folded WHERE x IN ('WORD5') SETTINGS transform_null_in = 1, use_skip_indexes = 0);
-- A row whose map lacks the key reads the value type's default, which `mapValues` never stored, so
-- a set holding that default must keep every granule. An all-NUL `FixedString` default is nonempty,
-- so the empty-element refusal above does not cover it.
CREATE TABLE t_map_fixed (m Map(String, FixedString(4)), INDEX i mapValues(m) TYPE text(tokenizer = array)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_map_fixed SELECT map('k', toFixedString('v' || toString(number % 7), 4)) FROM numbers(1000);

SELECT 'map fixed default rows', (SELECT count() FROM t_map_fixed WHERE m['absent'] IN (toFixedString('', 4)) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_map_fixed WHERE m['absent'] IN (toFixedString('', 4)) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'map fixed default rows, transform_null_in = 0', (SELECT count() FROM t_map_fixed WHERE m['absent'] IN (toFixedString('', 4)) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_map_fixed WHERE m['absent'] IN (toFixedString('', 4)) SETTINGS transform_null_in = 0, use_skip_indexes = 0);
SELECT 'map fixed stored value still prunes', count() FROM t_map_fixed WHERE m['k'] IN (toFixedString('v3', 4)) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
-- A `Nullable` map value defaults to NULL, which holds no bytes to compare against a set element.
CREATE TABLE t_map_nullable (m Map(String, Nullable(String)), INDEX i mapValues(m) TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_map_nullable SELECT map('k', 'v' || toString(number % 7)) FROM numbers(1000);

SELECT 'map nullable value used', count() FROM t_map_nullable WHERE m['k'] IN ('v3') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'map nullable value rows', (SELECT count() FROM t_map_nullable WHERE m['k'] IN ('v3') SETTINGS transform_null_in = 1) = (SELECT count() FROM t_map_nullable WHERE m['k'] IN ('v3') SETTINGS transform_null_in = 1, use_skip_indexes = 0);

-- A JSON subcolumn carrier is indexed through `JSONAllValues(json)`, so its stored type comes from
-- that header column.
SET enable_json_type = 1;
CREATE TABLE t_json (j JSON, INDEX i JSONAllValues(j) TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_json SELECT toJSONString(map('k', if(number = 5, 'word5', 'zz' || toString(number))))::JSON FROM numbers(1000);

SELECT 'json string set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json WHERE j.k::String IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'json fixed set rows', (SELECT count() FROM t_json WHERE j.k::String IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_json WHERE j.k::String IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'json fixed set count', count() FROM t_json WHERE j.k::String IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1;

-- An index expression is matched under its `optimize_empty_string_comparisons` rewrite, so the
-- stored type of a carrier matched that way comes from the index header rather than the node.
CREATE TABLE t_normalized (s String, a String ALIAS if(s = '', 'zzz', s), INDEX i a TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_normalized SELECT if(number = 5, 'word5', 'zz' || toString(number)) FROM numbers(1000);

SELECT 'normalized string set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_normalized WHERE a IN (SELECT v FROM t_string_set) SETTINGS transform_null_in = 1, optimize_empty_string_comparisons = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'normalized fixed set rows', (SELECT count() FROM t_normalized WHERE a IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, optimize_empty_string_comparisons = 1) = (SELECT count() FROM t_normalized WHERE a IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, optimize_empty_string_comparisons = 1, use_skip_indexes = 0);

-- `sparse_grams` shares the modified condition class with its own tokenizer.
CREATE TABLE t_sparse (b String, INDEX i b TYPE sparse_grams(3, 100, 512, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_sparse SELECT 'word' || toString(number) FROM numbers(1000);
SELECT 'sparse_grams', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE b IN (SELECT v FROM t_free_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'sparse_grams rows', (SELECT count() FROM t_sparse WHERE b IN (SELECT v FROM t_free_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_sparse WHERE b IN (SELECT v FROM t_free_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'sparse_grams fixed set rows', (SELECT count() FROM t_sparse WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_sparse WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'sparse_grams fixed set count', count() FROM t_sparse WHERE b IN (SELECT v FROM t_fixed_set) SETTINGS transform_null_in = 1;

DROP TABLE t_text;
DROP TABLE t_tokenbf;
DROP TABLE t_ngrambf;
DROP TABLE t_null_set;
DROP TABLE t_free_set;
DROP TABLE t_fixed_set;
DROP TABLE t_fixed_key;
DROP TABLE t_fixed_key_narrow;
DROP TABLE t_fixed_set_narrow;
DROP TABLE t_ipv6;
DROP TABLE t_ipv6_text_set;
DROP TABLE t_ipv6_set;
DROP TABLE t_sparse;
DROP TABLE t_array_fixed;
DROP TABLE t_array_string;
DROP TABLE t_string_set;
DROP TABLE t_tuple_key;
DROP TABLE t_preprocessed;
DROP TABLE t_map;
DROP TABLE t_text_ngrams;
DROP TABLE t_json;
DROP TABLE t_normalized;
DROP TABLE t_preprocessed_wrapped;
DROP TABLE t_preprocessed_plain;
DROP TABLE t_preprocessed_map;
DROP TABLE t_preprocessed_folded;
DROP TABLE t_map_fixed;
DROP TABLE t_map_nullable;
