-- Tags: no-parallel-replicas

-- text_index_build_threads spreads the merge-time text index rebuild over several builders that
-- partition the vocabulary by token hash, so the index a merge produces must not depend on the
-- number of builders. Every case below merges the same rows with several builder counts and
-- compares the merged parts by checksum (which covers the index streams), and the token and phrase
-- queries are compared against a table holding the same rows without an index.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET text_index_hint_max_selectivity = 1.;

SELECT 'Same index for any number of builders';

DROP TABLE IF EXISTS ref;
DROP TABLE IF EXISTS b1;
DROP TABLE IF EXISTS b2;
DROP TABLE IF EXISTS b4;
DROP TABLE IF EXISTS b8;
DROP TABLE IF EXISTS b_clamped;

-- 12000 rows, 8 tokens per row from a 400 word vocabulary plus a token every row has.
-- Deterministic: derived from `number` only.
CREATE TABLE ref (id UInt64, message String)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO ref SELECT number,
    arrayStringConcat(arrayMap(x -> toString(cityHash64(number, x) % 400), range(8)), ' ') || ' marker'
FROM numbers(12000);

CREATE TABLE b1 (id UInt64, message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;

CREATE TABLE b2 AS b1 SETTINGS text_index_build_threads = 2;
CREATE TABLE b4 AS b1 SETTINGS text_index_build_threads = 4;
CREATE TABLE b8 AS b1 SETTINGS text_index_build_threads = 8;
-- Clamped to max_build_text_index_thread_pool_size. The per-builder flush budgets must not become 0,
-- which would flush a temporary segment per block.
CREATE TABLE b_clamped AS b1 SETTINGS text_index_build_threads = 1000000;

-- Three parts each, holding exactly the rows of `ref`.
INSERT INTO b1 SELECT * FROM ref WHERE id < 4000;
INSERT INTO b1 SELECT * FROM ref WHERE id BETWEEN 4000 AND 7999;
INSERT INTO b1 SELECT * FROM ref WHERE id >= 8000;
INSERT INTO b2 SELECT * FROM ref WHERE id < 4000;
INSERT INTO b2 SELECT * FROM ref WHERE id BETWEEN 4000 AND 7999;
INSERT INTO b2 SELECT * FROM ref WHERE id >= 8000;
INSERT INTO b4 SELECT * FROM ref WHERE id < 4000;
INSERT INTO b4 SELECT * FROM ref WHERE id BETWEEN 4000 AND 7999;
INSERT INTO b4 SELECT * FROM ref WHERE id >= 8000;
INSERT INTO b8 SELECT * FROM ref WHERE id < 4000;
INSERT INTO b8 SELECT * FROM ref WHERE id BETWEEN 4000 AND 7999;
INSERT INTO b8 SELECT * FROM ref WHERE id >= 8000;
INSERT INTO b_clamped SELECT * FROM ref WHERE id < 4000;
INSERT INTO b_clamped SELECT * FROM ref WHERE id BETWEEN 4000 AND 7999;
INSERT INTO b_clamped SELECT * FROM ref WHERE id >= 8000;

OPTIMIZE TABLE b1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE b2 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE b4 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE b8 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE b_clamped FINAL SETTINGS optimize_throw_if_noop = 1;

-- One merged part each, and one checksum shared by all builder counts.
SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active
  AND table IN ('b1', 'b2', 'b4', 'b8', 'b_clamped');

SELECT 'Token search matches the same rows without an index';

SELECT token, with_index, without_index, with_index = without_index AS same FROM
(
    SELECT 'marker' AS token,
        (SELECT count() FROM b4 WHERE hasToken(message, 'marker')) AS with_index,
        (SELECT count() FROM ref WHERE hasToken(message, 'marker')) AS without_index
    UNION ALL SELECT '0',
        (SELECT count() FROM b4 WHERE hasToken(message, '0')),
        (SELECT count() FROM ref WHERE hasToken(message, '0'))
    UNION ALL SELECT '42',
        (SELECT count() FROM b4 WHERE hasToken(message, '42')),
        (SELECT count() FROM ref WHERE hasToken(message, '42'))
    UNION ALL SELECT '399',
        (SELECT count() FROM b4 WHERE hasToken(message, '399')),
        (SELECT count() FROM ref WHERE hasToken(message, '399'))
    UNION ALL SELECT 'absent',
        (SELECT count() FROM b4 WHERE hasToken(message, 'absent')),
        (SELECT count() FROM ref WHERE hasToken(message, 'absent'))
)
ORDER BY token;

SELECT 'Phrase search over positions built by several builders';

DROP TABLE IF EXISTS pref;
DROP TABLE IF EXISTS p1;
DROP TABLE IF EXISTS p4;

-- Repeated tokens in a row-dependent rotation, so a phrase is not implied by token presence.
CREATE TABLE pref (id UInt64, message String)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO pref SELECT number,
    arrayStringConcat(arrayMap(x -> ['alpha', 'beta', 'gamma', 'delta'][1 + ((number + x) % 4)], range(10)), ' ')
    || ' ' || ['alpha beta gamma', 'gamma beta alpha', 'beta beta beta'][1 + (number % 3)]
FROM numbers(9000);

CREATE TABLE p1 (id UInt64, message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1,
                                 posting_list_block_size = 256))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1, min_bytes_for_wide_part = 0,
         text_index_build_threads = 1;

CREATE TABLE p4 AS p1 SETTINGS text_index_build_threads = 4;

INSERT INTO p1 SELECT * FROM pref WHERE id < 3000;
INSERT INTO p1 SELECT * FROM pref WHERE id BETWEEN 3000 AND 5999;
INSERT INTO p1 SELECT * FROM pref WHERE id >= 6000;
INSERT INTO p4 SELECT * FROM pref WHERE id < 3000;
INSERT INTO p4 SELECT * FROM pref WHERE id BETWEEN 3000 AND 5999;
INSERT INTO p4 SELECT * FROM pref WHERE id >= 6000;

OPTIMIZE TABLE p1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE p4 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('p1', 'p4');

SELECT phrase, with_index, without_index, with_index = without_index AS same FROM
(
    SELECT 'alpha beta' AS phrase,
        (SELECT count() FROM p4 WHERE hasPhrase(message, 'alpha beta')) AS with_index,
        (SELECT count() FROM pref WHERE hasPhrase(message, 'alpha beta')) AS without_index
    UNION ALL SELECT 'gamma beta alpha',
        (SELECT count() FROM p4 WHERE hasPhrase(message, 'gamma beta alpha')),
        (SELECT count() FROM pref WHERE hasPhrase(message, 'gamma beta alpha'))
    UNION ALL SELECT 'beta beta beta',
        (SELECT count() FROM p4 WHERE hasPhrase(message, 'beta beta beta')),
        (SELECT count() FROM pref WHERE hasPhrase(message, 'beta beta beta'))
    UNION ALL SELECT 'zeta eta',
        (SELECT count() FROM p4 WHERE hasPhrase(message, 'zeta eta')),
        (SELECT count() FROM pref WHERE hasPhrase(message, 'zeta eta'))
)
ORDER BY phrase;

SELECT 'A builder that keeps no token writes no segment';

DROP TABLE IF EXISTS nulls1;
DROP TABLE IF EXISTS nulls8;
DROP TABLE IF EXISTS one1;
DROP TABLE IF EXISTS one8;

-- An all NULL column leaves every builder with an empty share of the vocabulary.
CREATE TABLE nulls1 (id UInt64, message Nullable(String),
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE nulls8 AS nulls1 SETTINGS text_index_build_threads = 8;

-- A single distinct token gives fewer tokens than builders.
CREATE TABLE one1 (id UInt64, message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE one8 AS one1 SETTINGS text_index_build_threads = 8;

INSERT INTO nulls1 SELECT number, NULL FROM numbers(1000);
INSERT INTO nulls1 SELECT number + 1000, NULL FROM numbers(1000);
INSERT INTO nulls8 SELECT number, NULL FROM numbers(1000);
INSERT INTO nulls8 SELECT number + 1000, NULL FROM numbers(1000);
INSERT INTO one1 SELECT number, 'sametoken' FROM numbers(1000);
INSERT INTO one1 SELECT number + 1000, 'sametoken' FROM numbers(1000);
INSERT INTO one8 SELECT number, 'sametoken' FROM numbers(1000);
INSERT INTO one8 SELECT number + 1000, 'sametoken' FROM numbers(1000);

OPTIMIZE TABLE nulls1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE nulls8 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE one1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE one8 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('nulls1', 'nulls8');
SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('one1', 'one8');
SELECT count() FROM nulls8 WHERE hasToken(assumeNotNull(message), 'anything');
SELECT count() FROM one8 WHERE hasToken(message, 'sametoken');

SELECT 'Several flushes per builder';

DROP TABLE IF EXISTS flush1;
DROP TABLE IF EXISTS flush4;

-- A small token budget forces several temporary segments per builder, so the segment numbering
-- interleaves across builders.
CREATE TABLE flush1 (id UInt64, message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1,
         text_index_max_processed_tokens_before_flush = 1000;
CREATE TABLE flush4 AS flush1 SETTINGS text_index_build_threads = 4;

INSERT INTO flush1 SELECT * FROM ref WHERE id < 6000;
INSERT INTO flush1 SELECT * FROM ref WHERE id >= 6000;
INSERT INTO flush4 SELECT * FROM ref WHERE id < 6000;
INSERT INTO flush4 SELECT * FROM ref WHERE id >= 6000;

OPTIMIZE TABLE flush1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE flush4 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('flush1', 'flush4');
SELECT count() FROM flush4 WHERE hasToken(message, 'marker');

SELECT 'Arrays, maps, a token filter and a posting list codec';

DROP TABLE IF EXISTS arr1;
DROP TABLE IF EXISTS arr4;
DROP TABLE IF EXISTS map1;
DROP TABLE IF EXISTS map4;
DROP TABLE IF EXISTS filt1;
DROP TABLE IF EXISTS filt4;
DROP TABLE IF EXISTS codec1;
DROP TABLE IF EXISTS codec4;

CREATE TABLE arr1 (id UInt64, message Array(String),
    INDEX idx(message) TYPE text(tokenizer = array))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE arr4 AS arr1 SETTINGS text_index_build_threads = 4;

CREATE TABLE map1 (id UInt64, message Map(String, String),
    INDEX idx(message) TYPE text(tokenizer = keyValuePairs))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE map4 AS map1 SETTINGS text_index_build_threads = 4;

-- The IN/NOT IN postprocessor takes a separate path in the builder, with a pre-seeded keep set.
CREATE TABLE filt1 (id UInt64, message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha,
                                 postprocessor = if(message IN ('1', '2', '3'), '', message)))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE filt4 AS filt1 SETTINGS text_index_build_threads = 4;

-- The NOT IN spelling pre-seeds the whole keep set into every builder's map.
DROP TABLE IF EXISTS keep1;
DROP TABLE IF EXISTS keep4;
CREATE TABLE keep1 (id UInt64, message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha,
                                 postprocessor = if(message NOT IN ('1', '2', '3', 'marker'), '', message)))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE keep4 AS keep1 SETTINGS text_index_build_threads = 4;

CREATE TABLE codec1 (id UInt64, message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking'))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE codec4 AS codec1 SETTINGS text_index_build_threads = 4;

INSERT INTO arr1 SELECT id, splitByNonAlpha(message) FROM ref WHERE id < 6000;
INSERT INTO arr1 SELECT id, splitByNonAlpha(message) FROM ref WHERE id >= 6000;
INSERT INTO arr4 SELECT id, splitByNonAlpha(message) FROM ref WHERE id < 6000;
INSERT INTO arr4 SELECT id, splitByNonAlpha(message) FROM ref WHERE id >= 6000;

INSERT INTO map1 SELECT id, map('k' || toString(id % 7), toString(id % 400)) FROM ref WHERE id < 6000;
INSERT INTO map1 SELECT id, map('k' || toString(id % 7), toString(id % 400)) FROM ref WHERE id >= 6000;
INSERT INTO map4 SELECT id, map('k' || toString(id % 7), toString(id % 400)) FROM ref WHERE id < 6000;
INSERT INTO map4 SELECT id, map('k' || toString(id % 7), toString(id % 400)) FROM ref WHERE id >= 6000;

INSERT INTO filt1 SELECT * FROM ref WHERE id < 6000;
INSERT INTO filt1 SELECT * FROM ref WHERE id >= 6000;
INSERT INTO filt4 SELECT * FROM ref WHERE id < 6000;
INSERT INTO filt4 SELECT * FROM ref WHERE id >= 6000;

INSERT INTO keep1 SELECT * FROM ref WHERE id < 6000;
INSERT INTO keep1 SELECT * FROM ref WHERE id >= 6000;
INSERT INTO keep4 SELECT * FROM ref WHERE id < 6000;
INSERT INTO keep4 SELECT * FROM ref WHERE id >= 6000;

INSERT INTO codec1 SELECT * FROM ref WHERE id < 6000;
INSERT INTO codec1 SELECT * FROM ref WHERE id >= 6000;
INSERT INTO codec4 SELECT * FROM ref WHERE id < 6000;
INSERT INTO codec4 SELECT * FROM ref WHERE id >= 6000;

OPTIMIZE TABLE arr1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE arr4 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE map1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE map4 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE filt1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE filt4 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE keep1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE keep4 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE codec1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE codec4 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('arr1', 'arr4');
SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('map1', 'map4');
SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('filt1', 'filt4');
SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('keep1', 'keep4');
SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('codec1', 'codec4');
SELECT count() FROM arr4 WHERE has(message, 'marker');
SELECT count() FROM filt4 WHERE hasToken(message, 'marker');
SELECT count() FROM filt4 WHERE hasToken(message, '1');
SELECT count() FROM keep4 WHERE hasToken(message, 'marker');
SELECT count() FROM keep4 WHERE hasToken(message, '0');
SELECT count() FROM codec4 WHERE hasToken(message, 'marker');

SELECT 'Other merges that rebuild the index, and a mutation';

DROP TABLE IF EXISTS coll1;
DROP TABLE IF EXISTS coll4;
DROP TABLE IF EXISTS ttl1;
DROP TABLE IF EXISTS ttl4;
DROP TABLE IF EXISTS mut1;
DROP TABLE IF EXISTS mut4;

CREATE TABLE coll1 (id UInt64, message String, sign Int8,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE coll4 AS coll1 SETTINGS text_index_build_threads = 4;

CREATE TABLE ttl1 (id UInt64, message String, d Date,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE ttl4 AS ttl1 SETTINGS text_index_build_threads = 4;

CREATE TABLE mut1 (id UInt64, message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE mut4 AS mut1 SETTINGS text_index_build_threads = 4;

INSERT INTO coll1 SELECT id, message, 1 FROM ref;
INSERT INTO coll1 SELECT id, message, -1 FROM ref WHERE id % 3 = 0;
INSERT INTO coll4 SELECT id, message, 1 FROM ref;
INSERT INTO coll4 SELECT id, message, -1 FROM ref WHERE id % 3 = 0;

-- Each part keeps some live rows: a part whose rows all expire takes the TTLDrop short-circuit,
-- which does not build the index transform at all (fixed separately in #113385).
INSERT INTO ttl1 SELECT id, message, if(id % 2 = 0, today() - 10, today() + 10) FROM ref WHERE id < 6000;
INSERT INTO ttl1 SELECT id, message, if(id % 2 = 0, today() - 10, today() + 10) FROM ref WHERE id >= 6000;
INSERT INTO ttl4 SELECT id, message, if(id % 2 = 0, today() - 10, today() + 10) FROM ref WHERE id < 6000;
INSERT INTO ttl4 SELECT id, message, if(id % 2 = 0, today() - 10, today() + 10) FROM ref WHERE id >= 6000;

INSERT INTO mut1 SELECT * FROM ref;
INSERT INTO mut4 SELECT * FROM ref;

OPTIMIZE TABLE coll1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE coll4 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE ttl1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE ttl4 FINAL SETTINGS optimize_throw_if_noop = 1;
ALTER TABLE mut1 DELETE WHERE id % 5 = 0 SETTINGS mutations_sync = 2;
ALTER TABLE mut4 DELETE WHERE id % 5 = 0 SETTINGS mutations_sync = 2;

SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('coll1', 'coll4');
SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('mut1', 'mut4');
SELECT count() FROM coll4 WHERE hasToken(message, 'marker');
SELECT count() FROM mut4 WHERE hasToken(message, 'marker');

-- The TTL tables are compared by index content, not by part checksum: the background scheduler can
-- merge a part holding expired rows again after the explicit merge, and the data columns of the
-- rewritten part need not compress to the same bytes.
SELECT token, one_builder, four_builders, without_index,
       one_builder = four_builders AND four_builders = without_index AS same FROM
(
    SELECT 'rows' AS token,
        (SELECT count() FROM ttl1) AS one_builder,
        (SELECT count() FROM ttl4) AS four_builders,
        (SELECT count() FROM ref WHERE id % 2 = 1) AS without_index
    UNION ALL SELECT 'marker',
        (SELECT count() FROM ttl1 WHERE hasToken(message, 'marker')),
        (SELECT count() FROM ttl4 WHERE hasToken(message, 'marker')),
        (SELECT count() FROM ref WHERE id % 2 = 1 AND hasToken(message, 'marker'))
    UNION ALL SELECT '42',
        (SELECT count() FROM ttl1 WHERE hasToken(message, '42')),
        (SELECT count() FROM ttl4 WHERE hasToken(message, '42')),
        (SELECT count() FROM ref WHERE id % 2 = 1 AND hasToken(message, '42'))
    UNION ALL SELECT '399',
        (SELECT count() FROM ttl1 WHERE hasToken(message, '399')),
        (SELECT count() FROM ttl4 WHERE hasToken(message, '399')),
        (SELECT count() FROM ref WHERE id % 2 = 1 AND hasToken(message, '399'))
)
ORDER BY token;

SELECT 'A plain MergeTree merge does not rebuild the index';

DROP TABLE IF EXISTS plain1;
DROP TABLE IF EXISTS plain4;

CREATE TABLE plain1 (id UInt64, message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, text_index_build_threads = 1;
CREATE TABLE plain4 AS plain1 SETTINGS text_index_build_threads = 4;

INSERT INTO plain1 SELECT * FROM ref WHERE id < 6000;
INSERT INTO plain1 SELECT * FROM ref WHERE id >= 6000;
INSERT INTO plain4 SELECT * FROM ref WHERE id < 6000;
INSERT INTO plain4 SELECT * FROM ref WHERE id >= 6000;

OPTIMIZE TABLE plain1 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE plain4 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT count(), uniqExact(hash_of_all_files) FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('plain1', 'plain4');
SELECT count() FROM plain4 WHERE hasToken(message, 'marker');

DROP TABLE ref;
DROP TABLE b1;
DROP TABLE b2;
DROP TABLE b4;
DROP TABLE b8;
DROP TABLE b_clamped;
DROP TABLE pref;
DROP TABLE p1;
DROP TABLE p4;
DROP TABLE nulls1;
DROP TABLE nulls8;
DROP TABLE one1;
DROP TABLE one8;
DROP TABLE flush1;
DROP TABLE flush4;
DROP TABLE arr1;
DROP TABLE arr4;
DROP TABLE map1;
DROP TABLE map4;
DROP TABLE filt1;
DROP TABLE filt4;
DROP TABLE keep1;
DROP TABLE keep4;
DROP TABLE codec1;
DROP TABLE codec4;
DROP TABLE coll1;
DROP TABLE coll4;
DROP TABLE ttl1;
DROP TABLE ttl4;
DROP TABLE mut1;
DROP TABLE mut4;
DROP TABLE plain1;
DROP TABLE plain4;
