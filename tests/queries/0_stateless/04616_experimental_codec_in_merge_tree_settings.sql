-- An experimental codec (ZXC) must require `allow_experimental_codecs` also when it is specified
-- through the codec-valued MergeTree settings, not only in a column `CODEC(...)` clause.

DROP TABLE IF EXISTS t_zxc_mt_settings;

SET allow_experimental_codecs = 0;

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

SET allow_experimental_codecs = 1;

CREATE TABLE t_zxc_mt_settings (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZXC';
INSERT INTO t_zxc_mt_settings SELECT number FROM numbers(10000);
SELECT count(), sum(x) FROM t_zxc_mt_settings;

ALTER TABLE t_zxc_mt_settings MODIFY SETTING marks_compression_codec = 'ZXC', primary_key_compression_codec = 'ZXC';
INSERT INTO t_zxc_mt_settings SELECT number FROM numbers(10000);
SELECT count(), sum(x) FROM t_zxc_mt_settings;

-- The table remains loadable after DETACH/ATTACH even without the session setting.
SET allow_experimental_codecs = 0;
DETACH TABLE t_zxc_mt_settings;
ATTACH TABLE t_zxc_mt_settings;
SELECT count(), sum(x) FROM t_zxc_mt_settings;

DROP TABLE t_zxc_mt_settings;
