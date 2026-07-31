-- Under transform_null_in = 1 the analyzer rewrites `x IN (...)` to `nullIn(x, ...)` (and
-- `globalIn` to `globalNullIn`). The text and token-bloom index conditions did not recognise
-- those names, so the atom stayed unknown and the index was dropped. For a set without NULL
-- `nullIn` selects the same rows as `in`, so pruning is sound; a set containing NULL must keep
-- the original predicate, because `nullIn` also matches the column's NULL rows.
--
-- Every table uses ORDER BY tuple() so primary-key pruning cannot contribute to the readings.
-- Pruning is asserted with a magic-constant-free read < total comparison, and every pruning cell
-- is paired with its row count against the transform_null_in = 0 arm, so the test pins both the
-- optimization and correctness.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_txt;
DROP TABLE IF EXISTS t_txt_lc;
DROP TABLE IF EXISTS t_txt_null;
DROP TABLE IF EXISTS t_txt_lcnull;
DROP TABLE IF EXISTS t_txt_tuple;
DROP TABLE IF EXISTS t_txt_tuple_null;
DROP TABLE IF EXISTS t_txt_pos1;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_tok;
DROP TABLE IF EXISTS t_tok_lc;
DROP TABLE IF EXISTS t_tok_tuple;
DROP TABLE IF EXISTS t_tok_tuple_null;
DROP TABLE IF EXISTS t_ngram;
DROP TABLE IF EXISTS t_ngram_lc;
DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_txt_fixed;
DROP TABLE IF EXISTS t_txt_arr;
DROP TABLE IF EXISTS t_txt_mixed;
DROP TABLE IF EXISTS t_tok_mixed;
DROP TABLE IF EXISTS t_ngram_mixed;

CREATE TABLE t_txt (x String, INDEX i x TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt SELECT 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_txt_lc (x LowCardinality(String), INDEX i x TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt_lc SELECT 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_txt_null (x Nullable(String), INDEX i x TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt_null SELECT if(number % 100 = 7, NULL, 'word' || toString(number)) FROM numbers(1000);

CREATE TABLE t_txt_lcnull (x LowCardinality(Nullable(String)), INDEX i x TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt_lcnull SELECT if(number % 100 = 7, NULL, 'word' || toString(number)) FROM numbers(1000);

CREATE TABLE t_txt_tuple (a String, b String, INDEX i b TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt_tuple SELECT 'a' || toString(number), 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_txt_tuple_null (a String, b Nullable(String), INDEX i b TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt_tuple_null SELECT 'a' || toString(number), if(number % 100 = 7, NULL, 'word' || toString(number)) FROM numbers(1000);

-- The indexed component sits at tuple position 1, the shape 04063_text_index_in_tuple pins.
CREATE TABLE t_txt_pos1 (id UInt64, str String, INDEX idx_str str TYPE text(tokenizer = 'array', preprocessor = lower(str))) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt_pos1 SELECT number, concat('Hello', toString(number)) FROM numbers(100);

CREATE TABLE t_map (m Map(String, String), INDEX i mapValues(m) TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_map SELECT map('k', 'word' || toString(number)) FROM numbers(1000);

CREATE TABLE t_json (j JSON, INDEX i JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_json SELECT toJSONString(map('a', 'word' || toString(number))) FROM numbers(1000) SETTINGS enable_json_type = 1;

CREATE TABLE t_tok (x String, INDEX i x TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_tok SELECT 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_tok_lc (x LowCardinality(String), INDEX i x TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_tok_lc SELECT 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_tok_tuple (a String, b String, INDEX i b TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_tok_tuple SELECT 'a' || toString(number), 'word' || toString(number) FROM numbers(1000);

-- Only `b` is indexed, so a Nullable `a` is constructible here. Tolerating the Nullable wrapper in
-- the all-components type rule is what makes this a carrier.
CREATE TABLE t_tok_tuple_null (a Nullable(String), b String, INDEX i b TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_tok_tuple_null SELECT if(number % 100 = 7, NULL, 'a' || toString(number)), 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_ngram (x String, INDEX i x TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_ngram SELECT 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_ngram_lc (x LowCardinality(String), INDEX i x TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_ngram_lc SELECT 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_sparse (x String, INDEX i x TYPE sparse_grams(3, 5, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_sparse SELECT 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_txt_fixed (x FixedString(12), INDEX i x TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt_fixed SELECT 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_txt_arr (x Array(String), INDEX i x TYPE text(tokenizer = 'array')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt_arr SELECT ['word' || toString(number)] FROM numbers(1000);

CREATE TABLE t_txt_mixed (id UInt64, str String, INDEX i str TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_txt_mixed SELECT number, 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_tok_mixed (id UInt64, str String, INDEX i str TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_tok_mixed SELECT number, 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_ngram_mixed (id UInt64, str String, INDEX i str TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_ngram_mixed SELECT number, 'word' || toString(number) FROM numbers(1000);

SELECT '--- 1. gains: the index prunes at transform_null_in = 1, as it already did at 0 ---';

SELECT 'text String', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text String control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text String rows equal', (SELECT count() FROM t_txt WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

SELECT 'text LowCardinality(String)', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text LowCardinality(String) control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text LowCardinality(String) rows equal', (SELECT count() FROM t_txt_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

-- A Nullable key keeps the Nullable wrapper on its set elements at transform_null_in = 1 even when
-- no element is NULL (Set::getElementTypes only strips it when the setting is off), so this cell
-- exercises the wrapper tolerance rather than the name test.
SELECT 'text Nullable(String) null-free set', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_null WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text Nullable(String) control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_null WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text Nullable(String) rows equal', (SELECT count() FROM t_txt_null WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt_null WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

SELECT 'text LowCardinality(Nullable(String)) null-free set', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_lcnull WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text LowCardinality(Nullable(String)) control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_lcnull WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text LowCardinality(Nullable(String)) rows equal', (SELECT count() FROM t_txt_lcnull WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt_lcnull WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

SELECT 'text FixedString(12)', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_fixed WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text FixedString(12) control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_fixed WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text FixedString(12) rows equal', (SELECT count() FROM t_txt_fixed WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt_fixed WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

SELECT 'text tuple set', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_tuple WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text tuple set control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_tuple WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text tuple set rows equal', (SELECT count() FROM t_txt_tuple WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt_tuple WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);

SELECT 'text tuple set Nullable indexed component null-free', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_tuple_null WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text tuple set Nullable indexed component control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_tuple_null WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text tuple set Nullable indexed component rows equal', (SELECT count() FROM t_txt_tuple_null WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt_tuple_null WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);

-- Indexed component at tuple position 1. A guard keyed on set position 0 would refuse this shape.
SELECT 'text tuple set indexed at position 1', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_pos1 WHERE (id, str) IN (SELECT number, 'Hello10' FROM numbers(100)) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text tuple set indexed at position 1 control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_pos1 WHERE (id, str) IN (SELECT number, 'Hello10' FROM numbers(100)) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text tuple set indexed at position 1 rows equal', (SELECT count() FROM t_txt_pos1 WHERE (id, str) IN (SELECT number, 'Hello10' FROM numbers(100)) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt_pos1 WHERE (id, str) IN (SELECT number, 'Hello10' FROM numbers(100)) SETTINGS transform_null_in = 1);

-- The index expression type (Array(String)) differs from the predicate type (String), so a fix
-- comparing the two would refuse the map and JSON carriers outright.
SELECT 'text map element value', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map WHERE m['k'] IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text map element value control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map WHERE m['k'] IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text map element value rows equal', (SELECT count() FROM t_map WHERE m['k'] IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_map WHERE m['k'] IN ('word5', 'word500') SETTINGS transform_null_in = 1);

SELECT 'text JSON subcolumn', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json WHERE j.a::String IN ('word5', 'word500') SETTINGS transform_null_in = 1, enable_json_type = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text JSON subcolumn control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json WHERE j.a::String IN ('word5', 'word500') SETTINGS transform_null_in = 0, enable_json_type = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text JSON subcolumn rows equal', (SELECT count() FROM t_json WHERE j.a::String IN ('word5', 'word500') SETTINGS transform_null_in = 0, enable_json_type = 1) = (SELECT count() FROM t_json WHERE j.a::String IN ('word5', 'word500') SETTINGS transform_null_in = 1, enable_json_type = 1);

SELECT 'tokenbf_v1 String', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 String control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 String rows equal', (SELECT count() FROM t_tok WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tok WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

SELECT 'tokenbf_v1 LowCardinality(String)', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 LowCardinality(String) control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 LowCardinality(String) rows equal', (SELECT count() FROM t_tok_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tok_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

SELECT 'tokenbf_v1 tuple set', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_tuple WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 tuple set control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_tuple WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 tuple set rows equal', (SELECT count() FROM t_tok_tuple WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tok_tuple WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);

SELECT 'ngrambf_v1 String', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'ngrambf_v1 String control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'ngrambf_v1 String rows equal', (SELECT count() FROM t_ngram WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_ngram WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

SELECT 'ngrambf_v1 LowCardinality(String)', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'ngrambf_v1 LowCardinality(String) control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'ngrambf_v1 LowCardinality(String) rows equal', (SELECT count() FROM t_ngram_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_ngram_lc WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

-- sparse_grams shares the condition class of tokenbf_v1 and ngrambf_v1, so one hunk covers it.
SELECT 'sparse_grams String', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'sparse_grams String control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'sparse_grams String rows equal', (SELECT count() FROM t_sparse WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_sparse WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);

SELECT '--- 2. force_data_skipping_indices: a working query stopped being an error ---';

SELECT 'text force_data_skipping_indices', count() FROM t_txt_pos1 WHERE (id, str) IN (SELECT number, 'Hello10' FROM numbers(100)) SETTINGS force_data_skipping_indices = 'idx_str', transform_null_in = 1;
SELECT 'tokenbf_v1 force_data_skipping_indices', count() FROM t_tok WHERE x IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'ngrambf_v1 force_data_skipping_indices', count() FROM t_ngram WHERE x IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'sparse_grams force_data_skipping_indices', count() FROM t_sparse WHERE x IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;

SELECT '--- 3. globalNullIn, and the pre-existing globalIn gap on the token-bloom family ---';

SELECT 'text globalNullIn', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 globalNullIn', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'ngrambf_v1 globalNullIn', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'sparse_grams globalNullIn', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));

-- The token-bloom class admitted globalIn but never assigned an RPN function for it, so GLOBAL IN
-- never pruned there even at transform_null_in = 0. Pinned separately so the pre-existing half is
-- attributable on its own.
SELECT 'tokenbf_v1 globalIn tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'ngrambf_v1 globalIn tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'sparse_grams globalIn tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text globalIn tnin=0 unchanged', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));

SELECT '--- 4. a set containing NULL must NOT be pruned, and must return the NULL rows ---';

SELECT 'text Nullable NULL-in-set not pruned', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_null WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text Nullable NULL-in-set rows', count() FROM t_txt_null WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 1;
SELECT 'text Nullable NULL-in-set rows tnin=0', count() FROM t_txt_null WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 0;

SELECT 'text LowCardinality(Nullable) NULL-in-set not pruned', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_lcnull WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text LowCardinality(Nullable) NULL-in-set rows', count() FROM t_txt_lcnull WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 1;

-- NULL at the indexed tuple position: the same hazard one level down.
SELECT 'text tuple NULL at indexed position not pruned', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_tuple_null WHERE (a, b) IN (('a7', 'word7'), ('a107', NULL)) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text tuple NULL at indexed position rows', count() FROM t_txt_tuple_null WHERE (a, b) IN (('a7', 'word7'), ('a107', NULL)) SETTINGS transform_null_in = 1;
SELECT 'text tuple NULL at indexed position rows tnin=0', count() FROM t_txt_tuple_null WHERE (a, b) IN (('a7', 'word7'), ('a107', NULL)) SETTINGS transform_null_in = 0;

SELECT '--- 5. errors preserved ---';

-- A type-incompatible set must keep throwing rather than being pruned away. The throw happens while
-- converting the set to the key type, so the index never sees this predicate; the two settings
-- differ here because `nullIn` converts strictly while `in` yields an empty set, which is
-- pre-existing behaviour unrelated to the index (it reproduces with no index at all).
SELECT count() FROM t_txt WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }
SELECT 'text type-incompatible set tnin=0', count() FROM t_txt WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 0;
SELECT count() FROM t_tok WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }
SELECT 'tokenbf_v1 type-incompatible set tnin=0', count() FROM t_tok WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 0;

-- A scalar left-hand side against a two-column set. The token must be PRESENT in the data so a
-- granule survives and the filter actually executes: the throw comes from Set::execute, which is
-- only reached for a block that was not pruned away.
SELECT count() FROM t_txt WHERE x IN (SELECT 'word5', 'word500') SETTINGS transform_null_in = 1; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT count() FROM t_txt WHERE x IN (SELECT 'word5', 'word500') SETTINGS transform_null_in = 0; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT count() FROM t_tok WHERE x IN (SELECT 'word5', 'word500') SETTINGS transform_null_in = 1; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT count() FROM t_tok WHERE x IN (SELECT 'word5', 'word500') SETTINGS transform_null_in = 0; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

-- Pruning every granule also skips the execution-time errors those rows would have raised. That is
-- a pre-existing property of pruning in ClickHouse, observable on the primary key alone, and it
-- already applied to `in` on these index types. These rows assert the two spellings AGREE, which is
-- the property this change is about: `nullIn` now behaves like the `in` it mirrors.
SELECT 'text absent-token arity-invalid tnin=1', count() FROM t_txt WHERE x IN (SELECT 'zzzznotpresent', 'other') SETTINGS transform_null_in = 1;
SELECT 'text absent-token arity-invalid tnin=0', count() FROM t_txt WHERE x IN (SELECT 'zzzznotpresent', 'other') SETTINGS transform_null_in = 0;
SELECT 'text absent-token arms equal', (SELECT count() FROM t_txt WHERE x IN (SELECT 'zzzznotpresent', 'other') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt WHERE x IN (SELECT 'zzzznotpresent', 'other') SETTINGS transform_null_in = 1);
SELECT 'tokenbf_v1 absent-token arity-invalid tnin=1', count() FROM t_tok WHERE x IN (SELECT 'zzzznotpresent', 'other') SETTINGS transform_null_in = 1;
SELECT 'tokenbf_v1 absent-token arity-invalid tnin=0', count() FROM t_tok WHERE x IN (SELECT 'zzzznotpresent', 'other') SETTINGS transform_null_in = 0;
SELECT 'tokenbf_v1 absent-token arms equal', (SELECT count() FROM t_tok WHERE x IN (SELECT 'zzzznotpresent', 'other') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tok WHERE x IN (SELECT 'zzzznotpresent', 'other') SETTINGS transform_null_in = 1);

-- Companions for the bound check added in the token-bloom helper: a left-hand tuple against a
-- shorter right-hand side must still be rejected, on both families.
SELECT count() FROM t_txt_pos1 WHERE (id, str) IN (SELECT number FROM numbers(100)) SETTINGS transform_null_in = 1; -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM t_txt_pos1 WHERE (id, str) IN (SELECT tuple(number) FROM numbers(100)) SETTINGS transform_null_in = 1; -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM t_tok_mixed WHERE (id, str) IN (SELECT number FROM numbers(100)) SETTINGS transform_null_in = 1; -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM t_tok_mixed WHERE (id, str) IN (SELECT tuple(number) FROM numbers(100)) SETTINGS transform_null_in = 1; -- { serverError TYPE_MISMATCH, NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- 6. existing bail-outs still bail out ---';

SELECT 'text empty-string element not pruned', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt WHERE x IN ('word5', '') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text empty-string element rows equal', (SELECT count() FROM t_txt WHERE x IN ('word5', '') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt WHERE x IN ('word5', '') SETTINGS transform_null_in = 1);
SELECT 'text tokenizes-to-nothing element not pruned', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt WHERE x IN ('word5', '...') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text tokenizes-to-nothing element rows equal', (SELECT count() FROM t_txt WHERE x IN ('word5', '...') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt WHERE x IN ('word5', '...') SETTINGS transform_null_in = 1);

SELECT '--- 7. NOT IN / notNullIn stay unmapped ---';

SELECT 'text NOT IN not pruned tnin=1', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt WHERE x NOT IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text NOT IN not pruned tnin=0', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt WHERE x NOT IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'text NOT IN rows equal', (SELECT count() FROM t_txt WHERE x NOT IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt WHERE x NOT IN ('word5', 'word500') SETTINGS transform_null_in = 1);
SELECT 'tokenbf_v1 NOT IN not pruned tnin=1', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok WHERE x NOT IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 NOT IN not pruned tnin=0', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok WHERE x NOT IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));

SELECT '--- 8. non-carriers, asserted as unchanged ---';

-- Array(String) does not prune at transform_null_in = 0 either, so no gain may be claimed for it.
SELECT 'Array(String) IN not pruned tnin=0', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_arr WHERE x IN (['word5'], ['word500']) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'Array(String) IN not pruned tnin=1', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_arr WHERE x IN (['word5'], ['word500']) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'Array(String) IN rows equal', (SELECT count() FROM t_txt_arr WHERE x IN (['word5'], ['word500']) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt_arr WHERE x IN (['word5'], ['word500']) SETTINGS transform_null_in = 1);
SELECT 'Array(String) has() prunes tnin=1', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_arr WHERE has(x, 'word5') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_json WHERE j.a IN ('word5', 'word500') SETTINGS transform_null_in = 1, enable_json_type = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM t_json WHERE j.a IN ('word5', 'word500') SETTINGS transform_null_in = 0, enable_json_type = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- 9. Nullable UNINDEXED tuple component on the token-bloom family ---';

SELECT 'tokenbf_v1 Nullable unindexed component null-free', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_tuple_null WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 Nullable unindexed component control tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_tuple_null WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 Nullable unindexed component rows equal', (SELECT count() FROM t_tok_tuple_null WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tok_tuple_null WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);

-- The NULL sits in a component the index never tokenizes, so pruning proceeds and the extra row
-- nullIn selects is still returned. This is what proves the refusal is scoped to the tokenized
-- position rather than applied to the whole set.
SELECT 'tokenbf_v1 NULL in unindexed component prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_tuple_null WHERE (a, b) IN ((NULL, 'word7'), ('a5', 'word5')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'tokenbf_v1 NULL in unindexed component rows tnin=1', count() FROM t_tok_tuple_null WHERE (a, b) IN ((NULL, 'word7'), ('a5', 'word5')) SETTINGS transform_null_in = 1;
SELECT 'tokenbf_v1 NULL in unindexed component rows tnin=0', count() FROM t_tok_tuple_null WHERE (a, b) IN ((NULL, 'word7'), ('a5', 'word5')) SETTINGS transform_null_in = 0;

SELECT '--- 10. mixed-type tuple asymmetry between the two families, both directions ---';

-- The token-bloom class requires EVERY set component to be a string; the text class checks only the
-- tokenized position. That difference predates this change and must survive it, so a future
-- relaxation of the all-components rule cannot slip through unnoticed.
SELECT 'mixed tuple text prunes tnin=0', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'mixed tuple text prunes tnin=1', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'mixed tuple tokenbf_v1 not pruned tnin=0', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'mixed tuple tokenbf_v1 not pruned tnin=1', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'mixed tuple ngrambf_v1 not pruned tnin=0', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'mixed tuple ngrambf_v1 not pruned tnin=1', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'mixed tuple rows equal text', (SELECT count() FROM t_txt_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_txt_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 1);
SELECT 'mixed tuple rows equal tokenbf_v1', (SELECT count() FROM t_tok_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tok_mixed WHERE (id, str) IN ((5, 'word5'), (500, 'word500')) SETTINGS transform_null_in = 1);

DROP TABLE t_txt;
DROP TABLE t_txt_lc;
DROP TABLE t_txt_null;
DROP TABLE t_txt_lcnull;
DROP TABLE t_txt_tuple;
DROP TABLE t_txt_tuple_null;
DROP TABLE t_txt_pos1;
DROP TABLE t_map;
DROP TABLE t_json;
DROP TABLE t_tok;
DROP TABLE t_tok_lc;
DROP TABLE t_tok_tuple;
DROP TABLE t_tok_tuple_null;
DROP TABLE t_ngram;
DROP TABLE t_ngram_lc;
DROP TABLE t_sparse;
DROP TABLE t_txt_fixed;
DROP TABLE t_txt_arr;
DROP TABLE t_txt_mixed;
DROP TABLE t_tok_mixed;
DROP TABLE t_ngram_mixed;
