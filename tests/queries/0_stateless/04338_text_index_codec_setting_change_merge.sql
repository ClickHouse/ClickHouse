-- Tests that merging a text index works when source parts were written with different posting list
-- codecs, which can happen because `text_index_posting_list_codec` is a mutable table setting.
-- Previously the merge decoded every source part with the destination codec and threw CORRUPTED_DATA.

SET mutations_sync = 2;

-- bitpacking part merged with a none part (the original repro).
DROP TABLE IF EXISTS t_codec_change;
CREATE TABLE t_codec_change (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS text_index_posting_list_codec = 'bitpacking';

-- Keep the two source parts (with their different codecs) stable until the assertion below;
-- otherwise a background merge could merge them early with the current codec.
SYSTEM STOP MERGES t_codec_change;

INSERT INTO t_codec_change SELECT 'hello world ' || toString(number) FROM numbers(1000);
ALTER TABLE t_codec_change MODIFY SETTING text_index_posting_list_codec = 'none';
INSERT INTO t_codec_change SELECT 'foo bar ' || toString(number) FROM numbers(1000);

-- Confirm the table-level setting actually fed the index creator and the two parts really use
-- different codecs before the merge: a high-cardinality token in the bitpacking part has compressed
-- postings, while one in the none part does not. (Without this, none+none parts would also pass below.)
SELECT 'bitpacking part compressed, none part not';
SELECT has_compressed_postings FROM mergeTreeTextIndex(currentDatabase(), t_codec_change, idx) WHERE token = 'hello';
SELECT has_compressed_postings FROM mergeTreeTextIndex(currentDatabase(), t_codec_change, idx) WHERE token = 'foo';

SYSTEM START MERGES t_codec_change;
OPTIMIZE TABLE t_codec_change FINAL;

SELECT 'bitpacking then none';
SELECT count() FROM t_codec_change;
SELECT count() FROM t_codec_change WHERE hasToken(s, 'hello');
SELECT count() FROM t_codec_change WHERE hasToken(s, 'foo');

DROP TABLE t_codec_change;

-- none part merged with a bitpacking part (the reverse direction).
DROP TABLE IF EXISTS t_codec_change;
CREATE TABLE t_codec_change (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS text_index_posting_list_codec = 'none';

SYSTEM STOP MERGES t_codec_change;

INSERT INTO t_codec_change SELECT 'hello world ' || toString(number) FROM numbers(1000);
ALTER TABLE t_codec_change MODIFY SETTING text_index_posting_list_codec = 'bitpacking';
INSERT INTO t_codec_change SELECT 'foo bar ' || toString(number) FROM numbers(1000);

SYSTEM START MERGES t_codec_change;
OPTIMIZE TABLE t_codec_change FINAL;

SELECT 'none then bitpacking';
SELECT count() FROM t_codec_change;
SELECT count() FROM t_codec_change WHERE hasToken(s, 'hello');
SELECT count() FROM t_codec_change WHERE hasToken(s, 'foo');

DROP TABLE t_codec_change;
