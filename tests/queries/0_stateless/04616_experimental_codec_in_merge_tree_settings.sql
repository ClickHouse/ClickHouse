-- An experimental codec (ZXC) must require `enable_zxc_codec` also when it is specified`)
-- through the codec-valued MergeTree settings, not only in a column `CODEC(...)` clause.
-- The same holds for the `dictionary_compression_codec` text index argument, covered at the end.

DROP TABLE IF EXISTS t_zxc_mt_settings;

SET enable_zxc_codec = 0;

CREATE TABLE t_zxc_mt_settings (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_zxc_mt_settings (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS marks_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_zxc_mt_settings (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS primary_key_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_zxc_mt_settings (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'LZ4, ZXC'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_zxc_mt_settings (x UInt64) ENGINE = MergeTree ORDER BY x;

ALTER TABLE t_zxc_mt_settings MODIFY SETTING default_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_zxc_mt_settings MODIFY SETTING marks_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_zxc_mt_settings MODIFY SETTING primary_key_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }

-- A non-experimental codec is still accepted without the setting.
ALTER TABLE t_zxc_mt_settings MODIFY SETTING default_compression_codec = 'ZSTD(3)';

DROP TABLE t_zxc_mt_settings;

SET enable_zxc_codec = 1;

CREATE TABLE t_zxc_mt_settings (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZXC';
INSERT INTO t_zxc_mt_settings SELECT number FROM numbers(10000);
SELECT count(), sum(x) FROM t_zxc_mt_settings;

ALTER TABLE t_zxc_mt_settings MODIFY SETTING marks_compression_codec = 'ZXC', primary_key_compression_codec = 'ZXC';
INSERT INTO t_zxc_mt_settings SELECT number FROM numbers(10000);
SELECT count(), sum(x) FROM t_zxc_mt_settings;

-- The table remains loadable after DETACH/ATTACH even without the session setting.
SET enable_zxc_codec = 0;
DETACH TABLE t_zxc_mt_settings;
ATTACH TABLE t_zxc_mt_settings;
SELECT count(), sum(x) FROM t_zxc_mt_settings;

DROP TABLE t_zxc_mt_settings;

-- The codec-valued MergeTree setting for a text index dictionary is gated the same way.
SET enable_zxc_codec = 0;
CREATE TABLE t_zxc_mt_settings (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS text_index_dictionary_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_zxc_mt_settings (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_zxc_mt_settings MODIFY SETTING text_index_dictionary_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_zxc_mt_settings MODIFY SETTING text_index_dictionary_compression_codec = 'LZ4';
DROP TABLE t_zxc_mt_settings;

-- The lossy-codec rows for both forms live in 05240_text_index_dictionary_codec_lossy, which needs
-- the sz3 library and so carries no-fasttest; this test stays fasttest-eligible.

-- The index argument goes through the same gate as the settings above, on CREATE ...
SET enable_zxc_codec = 0;
CREATE TABLE t_arg_zxc (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'ZXC')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- ... and on ALTER, which the CREATE-side check does not reach.
CREATE TABLE t_arg_alter (s String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_arg_alter ADD INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'ZXC'); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_arg_alter;

-- An ADD INDEX IF NOT EXISTS naming an existing index installs nothing, so there is nothing to
-- judge and it must not throw.
CREATE TABLE t_arg_noop (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_arg_noop ADD INDEX IF NOT EXISTS idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'ZXC');
DROP TABLE t_arg_noop;

-- An unknown codec name is rejected, and a lowercase name is accepted.
CREATE TABLE t_arg_bogus (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'NOSUCHCODEC')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_CODEC }
CREATE TABLE t_arg_lower (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'lz4')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_arg_lower;

-- Malformed text index arguments keep reporting the text index validator's own error, not one
-- from the codec gate that runs before it.
CREATE TABLE t_arg_malformed (s String, INDEX idx s TYPE text('splitByNonAlpha', dictionary_compression_codec = 'LZ4')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- An already accepted argument must not be re-judged, or existing tables could fail to load.
SET enable_zxc_codec = 1;
CREATE TABLE t_arg_attach (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'ZXC')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arg_attach SELECT concat('alpha beta gamma ', toString(number)) FROM numbers(2000);
SET enable_zxc_codec = 0;
DETACH TABLE t_arg_attach;
ATTACH TABLE t_arg_attach;
SELECT count() FROM t_arg_attach;
DROP TABLE t_arg_attach;

-- A codec the table already carries is not re-judged, so an unrelated ALTER on such a table
-- keeps working after the gate is closed.
SET enable_zxc_codec = 1;
CREATE TABLE t_arg_kept (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'ZXC')) ENGINE = MergeTree ORDER BY tuple();
SET enable_zxc_codec = 0;
ALTER TABLE t_arg_kept ADD COLUMN n UInt64;
DROP TABLE t_arg_kept;
