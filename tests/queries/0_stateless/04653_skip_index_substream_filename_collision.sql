DROP TABLE IF EXISTS t_collide;

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

-- Columns share the index namespace: `skp_idx_` is not reserved, and escapeForFileName keeps `_`
-- and alphanumerics, so this collides at the default escape_index_filenames = 1.
CREATE TABLE t_collide (k UInt64, skp_idx_a String, w UInt64,
    INDEX a(w) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

-- A sparse column's offsets stream is data-dependent: the writer only picks Sparse after seeing
-- the data, and its stream is named `<base>.sparse.idx`, so `.idx` is part of the NAME. Column
-- `skp_idx_a` therefore claims `skp_idx_a.sparse.idx`, which is exactly what index `a.sparse.idx`
-- resolves to with escaping off. Both then open one marks file.
-- The ratio is pinned because at 1.0 sparse is off, and then there is genuinely no collision.
CREATE TABLE t_collide (k UInt64, skp_idx_a UInt64, w UInt64,
    INDEX `a.sparse.idx` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0,
         ratio_of_defaults_for_sparse_serialization = 0.9375; -- { serverError BAD_ARGUMENTS }

-- `Tuple` picks a kind per ELEMENT, so its sparse streams are `<base>%2E<elem>.sparse.idx` and a
-- top-level-only enumeration would miss them.
CREATE TABLE t_collide (k UInt64, skp_idx_t Tuple(a UInt64, b UInt64), w UInt64,
    INDEX `t%2Ea.sparse.idx` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0,
         ratio_of_defaults_for_sparse_serialization = 0.9375; -- { serverError BAD_ARGUMENTS }

-- Sparse off means no offsets stream, so the same pair is legal. This bounds the check to the
-- settings that actually produce the stream.
CREATE TABLE t_collide (k UInt64, skp_idx_a UInt64, w UInt64,
    INDEX `a.sparse.idx` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0, ratio_of_defaults_for_sparse_serialization = 1.0;
-- ... but enabling sparse afterwards must be rejected, like any other setting change.
ALTER TABLE t_collide MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 0.9375; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_collide;

-- A hashed index base can collide with a column literally named that hex string.
SELECT lower(hex(reverse(CAST(sipHash128('skp_idx_a_very_long_index_name_that_will_be_hashed'), 'FixedString(16)'))));

CREATE TABLE t_collide (k UInt64, w UInt64, `71e62d66ddd014b3d32bce57f65004b7` UInt64,
    INDEX a_very_long_index_name_that_will_be_hashed w TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS replace_long_file_name_to_hash = 1, max_file_name_length = 42; -- { serverError BAD_ARGUMENTS }

-- ... and switching hashing on later must be rejected too.
CREATE TABLE t_collide (k UInt64, w UInt64, `71e62d66ddd014b3d32bce57f65004b7` UInt64,
    INDEX a_very_long_index_name_that_will_be_hashed w TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS replace_long_file_name_to_hash = 0, max_file_name_length = 42;
ALTER TABLE t_collide MODIFY SETTING replace_long_file_name_to_hash = 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_collide;

-- A projection is its own namespace and is validated separately: here its implicit minmax index
-- collides with one of its own columns.
CREATE TABLE t_collide (k UInt64, v UInt64, skp_idx_auto_minmax_index_v UInt64,
    PROJECTION p (SELECT v, skp_idx_auto_minmax_index_v ORDER BY v)
        WITH SETTINGS (add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree ORDER BY k
SETTINGS add_minmax_index_for_numeric_columns = 0; -- { serverError BAD_ARGUMENTS }

-- Projection settings are an overlay on the table's: this collides only under the projection's own
-- replace_long_file_name_to_hash, which the parent table has off.
SELECT lower(hex(reverse(CAST(sipHash128('skp_idx_auto_minmax_index_a_long_projection_column_name'), 'FixedString(16)'))));

CREATE TABLE t_collide (k UInt64, a_long_projection_column_name UInt64, `80817c9cab6084fd147119dcdf09c9d1` UInt64,
    PROJECTION p (SELECT a_long_projection_column_name, `80817c9cab6084fd147119dcdf09c9d1` ORDER BY k)
        WITH SETTINGS (replace_long_file_name_to_hash = 1, add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree ORDER BY k
SETTINGS replace_long_file_name_to_hash = 0, max_file_name_length = 42,
         add_minmax_index_for_numeric_columns = 0; -- { serverError BAD_ARGUMENTS }

-- Mirror of the previous case: with the projection's hashing off, nothing collides.
CREATE TABLE t_collide (k UInt64, a_long_projection_column_name UInt64, `80817c9cab6084fd147119dcdf09c9d1` UInt64,
    PROJECTION p (SELECT a_long_projection_column_name, `80817c9cab6084fd147119dcdf09c9d1` ORDER BY k)
        WITH SETTINGS (replace_long_file_name_to_hash = 0, add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree ORDER BY k
SETTINGS replace_long_file_name_to_hash = 0, max_file_name_length = 42,
         add_minmax_index_for_numeric_columns = 0;
DROP TABLE t_collide;

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

-- The `skp_idx_` prefix is not rejected per se: only an actual name collision is.
CREATE TABLE t_collide (k UInt64, skp_idx_b String, w UInt64,
    INDEX a(w) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_collide SELECT number, 'x', number FROM numbers(50);
SELECT count() FROM t_collide;
DROP TABLE t_collide;

-- `.sparse` alone is NOT the offsets base (`.sparse.idx` is), so this pair stays legal. The mostly
-- default column makes the writer really choose Sparse, so the arm exercises the sparse path rather
-- than merely parsing: `serialization_kind` is asserted and the values must read back intact.
CREATE TABLE t_collide (k UInt64, skp_idx_a UInt64, w UInt64,
    INDEX `a.sparse` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 0.9375;
INSERT INTO t_collide SELECT number, if(number % 100 = 0, number, 0), number FROM numbers(1000);
SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_collide' AND active AND column = 'skp_idx_a';
SELECT count(), sum(skp_idx_a) FROM t_collide;
DROP TABLE t_collide;

-- A projection stream base coinciding with a parent-table stream base is not a collision: the
-- projection's files live in `<name>.proj/`.
CREATE TABLE t_collide (k UInt64, v UInt64, PROJECTION p (SELECT v ORDER BY v))
ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_collide SELECT number, number FROM numbers(50);
SELECT count() FROM t_collide;
DROP TABLE t_collide;

-- An index base that does NOT exceed max_file_name_length is not hashed, so it cannot alias the hex
-- column name.
CREATE TABLE t_collide (k UInt64, w UInt64, `71e62d66ddd014b3d32bce57f65004b7` UInt64,
    INDEX a_very_long_index_name_that_will_be_hashed w TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         replace_long_file_name_to_hash = 0, max_file_name_length = 42;
INSERT INTO t_collide SELECT number, number, number FROM numbers(50);
SELECT count() FROM t_collide;
DROP TABLE t_collide;

-- The collision check constructs real index objects, so it must run only after per-index validation:
-- several creators read `index.arguments` with no null or size check. A malformed ADD INDEX must
-- still fail with the validator's own error, not a crash and not BAD_ARGUMENTS.
CREATE TABLE t_collide (k UInt64, w UInt64, v Array(Float32))
ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_collide ADD INDEX i1 w TYPE set; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_collide ADD INDEX i2 v TYPE vector_similarity; -- { serverError INCORRECT_QUERY }
DROP TABLE t_collide;
