-- Welcome visitor from the future! If it is >= July 2023 and your intention is to adjust the test because "enable_gorilla_codec_for_non_float_data"
-- is now obsolete, then please also extend 01272_suspicious_codecs.sql with new tests cases for Gorilla on non-float data.

DROP TABLE IF EXISTS test;

-- current default behavior is to enable non-float Gorilla compressed data

CREATE TABLE test (id UInt64, val Decimal(15,5) CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id;
DROP TABLE IF EXISTS test;

CREATE TABLE test (id UInt64, val FixedString(2) CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id;
DROP TABLE IF EXISTS test;

CREATE TABLE test (id UInt64, val UInt64 CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id;
DROP TABLE IF EXISTS test;

-- this can be changed (and it is planned to be changed by default in future) with a setting
SET enable_gorilla_codec_for_non_float_data = false;

CREATE TABLE test (id UInt64, val Decimal(15,5) CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE test (id UInt64, val FixedString(2) CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE test (id UInt64, val UInt64 CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- even with above setting, it will still be possible to create non-float Gorilla-compressed data using allow_suspicious_codecs
SET allow_suspicious_codecs = true;

CREATE TABLE test (id UInt64, val Decimal(15,5) CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id;
DROP TABLE IF EXISTS test;

CREATE TABLE test (id UInt64, val FixedString(2) CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id;
DROP TABLE IF EXISTS test;

CREATE TABLE test (id UInt64, val UInt64 CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id;
DROP TABLE IF EXISTS test;
