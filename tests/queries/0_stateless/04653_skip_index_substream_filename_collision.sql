-- The reported case: a `set` index literally named `a.pos` claims the text index's positional
-- substream, so `OPTIMIZE` decodes foreign payload as document ids.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,
    INDEX `a.pos` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0, allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }

-- `.dct` and `.pst` are unconditional text-index substreams, so they collide with any text index
-- and need no experimental setting.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX `a.dct` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0; -- { serverError BAD_ARGUMENTS }

-- Not specific to `set`: every non-inert type that does not override `getSubstreams` writes `.idx`.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX `a.pst` w TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0; -- { serverError BAD_ARGUMENTS }

-- `minmax` writes `.idx2`, so the data files differ, but the marks extension is one writer-wide
-- value: both open `skp_idx_a.pos` + marks extension. Keying on base+extension would miss this.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,
    INDEX `a.pos` w TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0, allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }

-- ALTER ADD INDEX reaches the same check.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS escape_index_filenames = 0;
ALTER TABLE t_collide ADD INDEX `a.dct` w TYPE set(100) GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_collide;

-- Turning escaping off makes an already-legal pair collide. The check must use the settings this
-- ALTER establishes, not the ones cached on the index descriptions (which are refreshed later).
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX `a.dct` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS escape_index_filenames = 1;
ALTER TABLE t_collide MODIFY SETTING escape_index_filenames = 0; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_collide;

-- A long-named pair, whose reported filename is the hash and whose message carries the
-- `replace_long_file_name_to_hash` hint, lives in 04654: asserting the hash and the hint needs stderr.

-- Bounds against an over-broad check.

-- `.b` is not a text-index substream suffix, so a dotted name is not rejected per se.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,
    INDEX `a.b` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100,
         escape_index_filenames = 0, allow_experimental_text_index_phrase_search = 1;
INSERT INTO t_collide SELECT number, concat('hello', number % 50, ' world', number % 50), number FROM numbers(500);
OPTIMIZE TABLE t_collide FINAL;
SELECT count() FROM t_collide;
DROP TABLE t_collide;

-- The very same pair is legal with escaping on: `a.pos` resolves to `skp_idx_a%2Epos`.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,
    INDEX `a.pos` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100,
         escape_index_filenames = 1, allow_experimental_text_index_phrase_search = 1;
INSERT INTO t_collide SELECT number, concat('hello', number % 50, ' world', number % 50), number FROM numbers(500);
OPTIMIZE TABLE t_collide FINAL;
SELECT count() FROM t_collide;
DROP TABLE t_collide;

-- A text index beside unrelated indices, with escaping off.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,
    INDEX bb w TYPE set(100) GRANULARITY 1,
    INDEX cc w TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100,
         escape_index_filenames = 0, allow_experimental_text_index_phrase_search = 1;
INSERT INTO t_collide SELECT number, concat('hello', number % 50, ' world', number % 50), number FROM numbers(500);
OPTIMIZE TABLE t_collide FINAL;
SELECT count() FROM t_collide;
DROP TABLE t_collide;

-- A projection index base coinciding with a parent-table index base is not a collision: the
-- projection's files live in `<name>.proj/`. Here the projection's implicit `auto_minmax_index_v` and
-- the parent's explicit index of the same name resolve to one base, in two different directories.
CREATE TABLE t_collide (k UInt64, v UInt64,
    INDEX auto_minmax_index_v v TYPE minmax GRANULARITY 1,
    PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_collide SELECT number, number FROM numbers(50);
SELECT count() FROM t_collide;
DROP TABLE t_collide;

-- Grandfathering applies to ALTER only: on CREATE there is no earlier definition, so every collision
-- is introduced by the statement itself. A second CREATE of the same shape must still be rejected,
-- not treated as inherited from the first one's failure.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX `a.dct` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0; -- { serverError BAD_ARGUMENTS }

-- The collision check constructs real index objects, so it must run only after per-index validation:
-- several creators read `index.arguments` with no null or size check. A malformed ADD INDEX must
-- still fail with the validator's own error, not a crash and not BAD_ARGUMENTS.
CREATE TABLE t_collide (k UInt64, w UInt64, v Array(Float32))
ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_collide ADD INDEX i1 w TYPE set; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_collide ADD INDEX i2 v TYPE vector_similarity; -- { serverError INCORRECT_QUERY }
DROP TABLE t_collide;
