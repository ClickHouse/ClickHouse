-- Every table forces default_compression_codec = 'ZSTD(3)'. Without it the part default is
-- size-aware, so a small fresh part is already LZ4 and every size assertion below would hold
-- vacuously.
--
-- The size assertions compare two tables inside one run, so both arms see the same randomized
-- merge-tree settings and the comparison stays valid under randomization. That is deliberate:
-- it is why this test needs no no-random-merge-tree-settings tag. Verified over 100 runs with
-- randomization enabled.

DROP TABLE IF EXISTS t_dict_inherit;
DROP TABLE IF EXISTS t_dict_lz4;
DROP TABLE IF EXISTS t_dict_mixed;
DROP TABLE IF EXISTS t_dict_control;
DROP TABLE IF EXISTS t_dict_noindex;
DROP TABLE IF EXISTS t_peridx;
DROP TABLE IF EXISTS t_peridx_twin;
DROP TABLE IF EXISTS t_prec_arg;
DROP TABLE IF EXISTS t_prec_set;
DROP TABLE IF EXISTS t_prec_base;

-- (a) The codec reaches both write paths: the insert path and the merge path.

CREATE TABLE t_dict_inherit (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'ZSTD(3)', text_index_dictionary_compression_codec = '';

CREATE TABLE t_dict_lz4 (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'ZSTD(3)', text_index_dictionary_compression_codec = 'LZ4';

-- Keep the two source parts of each table unmerged until the insert-path assertion below; otherwise
-- a background merge makes that assertion measure the merge path, and the OPTIMIZE FINAL after it
-- becomes a no-op on an already single part.
SYSTEM STOP MERGES t_dict_inherit;
SYSTEM STOP MERGES t_dict_lz4;

INSERT INTO t_dict_inherit SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000);
INSERT INTO t_dict_inherit SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000, 100000);
INSERT INTO t_dict_lz4     SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000);
INSERT INTO t_dict_lz4     SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000, 100000);

SELECT 'insert path';
SELECT
    uncompressed_inherit = uncompressed_lz4 AS same_content,
    compressed_lz4 > compressed_inherit AS lz4_dictionary_is_larger
FROM
(
    SELECT
        sumIf(data_uncompressed_bytes, table = 't_dict_inherit') AS uncompressed_inherit,
        sumIf(data_uncompressed_bytes, table = 't_dict_lz4') AS uncompressed_lz4,
        sumIf(data_compressed_bytes, table = 't_dict_inherit') AS compressed_inherit,
        sumIf(data_compressed_bytes, table = 't_dict_lz4') AS compressed_lz4
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table IN ('t_dict_inherit', 't_dict_lz4')
);

SYSTEM START MERGES t_dict_inherit;
SYSTEM START MERGES t_dict_lz4;

OPTIMIZE TABLE t_dict_inherit FINAL;
OPTIMIZE TABLE t_dict_lz4 FINAL;

-- A merged text index is written by a different code path than an inserted one, so this is the
-- assertion that catches a change which only covers the insert path.
SELECT 'merge path';
SELECT
    uncompressed_inherit = uncompressed_lz4 AS same_content,
    compressed_lz4 > compressed_inherit AS lz4_dictionary_is_larger
FROM
(
    SELECT
        sumIf(data_uncompressed_bytes, table = 't_dict_inherit') AS uncompressed_inherit,
        sumIf(data_uncompressed_bytes, table = 't_dict_lz4') AS uncompressed_lz4,
        sumIf(data_compressed_bytes, table = 't_dict_inherit') AS compressed_inherit,
        sumIf(data_compressed_bytes, table = 't_dict_lz4') AS compressed_lz4
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table IN ('t_dict_inherit', 't_dict_lz4')
);

-- (b) A part written with one dictionary codec merging with a part written with another.

CREATE TABLE t_dict_mixed (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'ZSTD(3)', text_index_dictionary_compression_codec = '';

CREATE TABLE t_dict_control (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'ZSTD(3)', text_index_dictionary_compression_codec = '';

CREATE TABLE t_dict_noindex (s String)
ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'ZSTD(3)';

SYSTEM STOP MERGES t_dict_mixed;
SYSTEM STOP MERGES t_dict_control;

INSERT INTO t_dict_mixed   SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000);
INSERT INTO t_dict_control SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000);

ALTER TABLE t_dict_mixed MODIFY SETTING text_index_dictionary_compression_codec = 'LZ4';

INSERT INTO t_dict_mixed   SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000, 100000);
INSERT INTO t_dict_control SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000, 100000);
INSERT INTO t_dict_noindex SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(200000);

-- Non-vacuity: the second part really did change codec, while holding its content equal.
SELECT 'second part changed';
SELECT
    mixed_compressed > control_compressed AS second_part_is_larger,
    mixed_uncompressed = control_uncompressed AS same_content
FROM
(
    SELECT
        sumIf(secondary_indices_compressed_bytes, table = 't_dict_mixed') AS mixed_compressed,
        sumIf(secondary_indices_compressed_bytes, table = 't_dict_control') AS control_compressed,
        sumIf(secondary_indices_uncompressed_bytes, table = 't_dict_mixed') AS mixed_uncompressed,
        sumIf(secondary_indices_uncompressed_bytes, table = 't_dict_control') AS control_uncompressed
    FROM system.parts
    WHERE database = currentDatabase() AND table IN ('t_dict_mixed', 't_dict_control')
      AND active AND name LIKE 'all_2_2%'
);

SYSTEM START MERGES t_dict_mixed;
OPTIMIZE TABLE t_dict_mixed FINAL;

SELECT 'mixed codec merge reads correctly';
SELECT
    (SELECT count() FROM t_dict_mixed) = (SELECT count() FROM t_dict_noindex),
    (SELECT count() FROM t_dict_mixed WHERE hasToken(s, 'tok123')) = (SELECT count() FROM t_dict_noindex WHERE hasToken(s, 'tok123')),
    (SELECT count() FROM t_dict_mixed WHERE hasAllTokens(s, ['quick', 'fox'])) = (SELECT count() FROM t_dict_noindex WHERE hasAllTokens(s, ['quick', 'fox'])),
    (SELECT count() FROM t_dict_mixed WHERE s LIKE 'tok1%') = (SELECT count() FROM t_dict_noindex WHERE s LIKE 'tok1%'),
    (SELECT count() FROM t_dict_mixed WHERE s LIKE '%tok12345%') = (SELECT count() FROM t_dict_noindex WHERE s LIKE '%tok12345%'),
    (SELECT count() FROM t_dict_mixed WHERE s ILIKE '%TOK12345%') = (SELECT count() FROM t_dict_noindex WHERE s ILIKE '%TOK12345%');

-- (c) The dictionary reader that does not accept mixed codecs within one stream, plus a
-- consistency check of the parts themselves.
SELECT 'dictionary reads equal';
SELECT
    (SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_dict_mixed, idx))
        = (SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_dict_lz4, idx)),
    (SELECT countDistinct(token) FROM mergeTreeTextIndex(currentDatabase(), t_dict_mixed, idx))
        = (SELECT countDistinct(token) FROM mergeTreeTextIndex(currentDatabase(), t_dict_lz4, idx));

SELECT 'check table';
CHECK TABLE t_dict_mixed SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_dict_lz4 SETTINGS check_query_single_value_result = 1;

-- (d) The codec applies to exactly the index that names it. Both tables keep the table setting
-- empty, so an argument is the only thing that can make a dictionary differ; `a` and `b` carry
-- different content so the sibling assertion cannot pass by the two indexes being identical.

CREATE TABLE t_peridx
(
    n UInt64, a String, b String,
    INDEX idx_a a TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'LZ4'),
    INDEX idx_b b TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_mm n TYPE minmax
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'ZSTD(3)', text_index_dictionary_compression_codec = '';

CREATE TABLE t_peridx_twin
(
    n UInt64, a String, b String,
    INDEX idx_a a TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_b b TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_mm n TYPE minmax
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'ZSTD(3)', text_index_dictionary_compression_codec = '';

INSERT INTO t_peridx SELECT number, concat('tok', toString(number), ' the quick brown fox'), concat('zzz', toString(number * 7), ' lorem ipsum dolor sit amet') FROM numbers(100000);
INSERT INTO t_peridx SELECT number, concat('tok', toString(number), ' the quick brown fox'), concat('zzz', toString(number * 7), ' lorem ipsum dolor sit amet') FROM numbers(100000, 100000);
INSERT INTO t_peridx_twin SELECT number, concat('tok', toString(number), ' the quick brown fox'), concat('zzz', toString(number * 7), ' lorem ipsum dolor sit amet') FROM numbers(100000);
INSERT INTO t_peridx_twin SELECT number, concat('tok', toString(number), ' the quick brown fox'), concat('zzz', toString(number * 7), ' lorem ipsum dolor sit amet') FROM numbers(100000, 100000);

OPTIMIZE TABLE t_peridx FINAL;
OPTIMIZE TABLE t_peridx_twin FINAL;

SELECT 'per index resolution';
WITH indices AS
(
    SELECT table, name, data_compressed_bytes AS c, data_uncompressed_bytes AS u
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table IN ('t_peridx', 't_peridx_twin')
)
SELECT
    (SELECT c FROM indices WHERE table = 't_peridx' AND name = 'idx_a')
        != (SELECT c FROM indices WHERE table = 't_peridx_twin' AND name = 'idx_a') AS argument_reached_idx_a,
    (SELECT u FROM indices WHERE table = 't_peridx' AND name = 'idx_a')
        = (SELECT u FROM indices WHERE table = 't_peridx_twin' AND name = 'idx_a') AS idx_a_same_content,
    (SELECT c FROM indices WHERE table = 't_peridx' AND name = 'idx_b')
        = (SELECT c FROM indices WHERE table = 't_peridx_twin' AND name = 'idx_b') AS idx_b_untouched,
    (SELECT u FROM indices WHERE table = 't_peridx' AND name = 'idx_b')
        = (SELECT u FROM indices WHERE table = 't_peridx_twin' AND name = 'idx_b') AS idx_b_same_content,
    (SELECT c FROM indices WHERE table = 't_peridx' AND name = 'idx_mm')
        = (SELECT c FROM indices WHERE table = 't_peridx_twin' AND name = 'idx_mm') AS minmax_untouched;

-- A part whose two text indexes disagree on their dictionary codec still reads correctly.
SELECT 'two dictionary codecs in one part read correctly';
SELECT
    (SELECT count() FROM t_peridx WHERE hasToken(a, 'tok123')) = (SELECT count() FROM t_peridx_twin WHERE hasToken(a, 'tok123')),
    (SELECT count() FROM t_peridx WHERE hasToken(b, 'zzz861')) = (SELECT count() FROM t_peridx_twin WHERE hasToken(b, 'zzz861')),
    (SELECT count() FROM t_peridx WHERE a LIKE '%tok12345%') = (SELECT count() FROM t_peridx_twin WHERE a LIKE '%tok12345%'),
    (SELECT count() FROM t_peridx WHERE b LIKE '%zzz8610%') = (SELECT count() FROM t_peridx_twin WHERE b LIKE '%zzz8610%');

CHECK TABLE t_peridx SETTINGS check_query_single_value_result = 1;

-- (e) The index argument wins over a table setting that names a different codec. Section (d) keeps
-- the table setting empty, so it compares the argument against nothing; here the table setting is
-- non-empty, which is the only shape that can tell the two sources of the value apart.

CREATE TABLE t_prec_arg (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'LZ4'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'ZSTD(3)', text_index_dictionary_compression_codec = 'ZSTD(3)';

CREATE TABLE t_prec_set (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'ZSTD(3)', text_index_dictionary_compression_codec = 'LZ4';

CREATE TABLE t_prec_base (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'ZSTD(3)', text_index_dictionary_compression_codec = 'ZSTD(3)';

SYSTEM STOP MERGES t_prec_arg;
SYSTEM STOP MERGES t_prec_set;
SYSTEM STOP MERGES t_prec_base;

INSERT INTO t_prec_arg  SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000);
INSERT INTO t_prec_arg  SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000, 100000);
INSERT INTO t_prec_set  SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000);
INSERT INTO t_prec_set  SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000, 100000);
INSERT INTO t_prec_base SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000);
INSERT INTO t_prec_base SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(100000, 100000);

SELECT 'per index precedence, insert path';
WITH prec AS
(
    SELECT table, data_compressed_bytes AS c, data_uncompressed_bytes AS u
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table IN ('t_prec_arg', 't_prec_set', 't_prec_base')
)
SELECT
    (SELECT c FROM prec WHERE table = 't_prec_arg') != (SELECT c FROM prec WHERE table = 't_prec_base') AS argument_beats_table_setting,
    (SELECT c FROM prec WHERE table = 't_prec_base') != (SELECT c FROM prec WHERE table = 't_prec_set') AS codecs_really_differ,
    (SELECT u FROM prec WHERE table = 't_prec_arg') = (SELECT u FROM prec WHERE table = 't_prec_set')
        AND (SELECT u FROM prec WHERE table = 't_prec_set') = (SELECT u FROM prec WHERE table = 't_prec_base') AS same_content,
    (SELECT min(c) FROM prec) > 0 AS all_arms_wrote_a_dictionary,
    (SELECT c FROM prec WHERE table = 't_prec_arg') = (SELECT c FROM prec WHERE table = 't_prec_set') AS both_lz4_arms_agree;

SYSTEM START MERGES t_prec_arg;
SYSTEM START MERGES t_prec_set;
SYSTEM START MERGES t_prec_base;

OPTIMIZE TABLE t_prec_arg FINAL;
OPTIMIZE TABLE t_prec_set FINAL;
OPTIMIZE TABLE t_prec_base FINAL;

SELECT 'per index precedence, merge path';
WITH prec AS
(
    SELECT table, data_compressed_bytes AS c, data_uncompressed_bytes AS u
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table IN ('t_prec_arg', 't_prec_set', 't_prec_base')
)
SELECT
    (SELECT c FROM prec WHERE table = 't_prec_arg') != (SELECT c FROM prec WHERE table = 't_prec_base') AS argument_beats_table_setting,
    (SELECT c FROM prec WHERE table = 't_prec_base') != (SELECT c FROM prec WHERE table = 't_prec_set') AS codecs_really_differ,
    (SELECT u FROM prec WHERE table = 't_prec_arg') = (SELECT u FROM prec WHERE table = 't_prec_set')
        AND (SELECT u FROM prec WHERE table = 't_prec_set') = (SELECT u FROM prec WHERE table = 't_prec_base') AS same_content,
    (SELECT min(c) FROM prec) > 0 AS all_arms_wrote_a_dictionary,
    (SELECT c FROM prec WHERE table = 't_prec_arg') = (SELECT c FROM prec WHERE table = 't_prec_set') AS both_lz4_arms_agree;

DROP TABLE t_dict_inherit;
DROP TABLE t_dict_lz4;
DROP TABLE t_dict_mixed;
DROP TABLE t_dict_control;
DROP TABLE t_dict_noindex;
DROP TABLE t_peridx;
DROP TABLE t_peridx_twin;
DROP TABLE t_prec_arg;
DROP TABLE t_prec_set;
DROP TABLE t_prec_base;
