-- Test for issue #80220

DROP TABLE IF EXISTS tab;

-- Codec DoubleDelta must not be used on FixedString columns

CREATE TABLE tab(c0 FixedString(9) CODEC(DoubleDelta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab(c0 FixedString(9) CODEC(DoubleDelta(1))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab(c0 LowCardinality(FixedString(9)) CODEC(DoubleDelta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab(c0 LowCardinality(FixedString(9)) CODEC(DoubleDelta(2))) ENGINE = MergeTree() ORDER BY tuple();  -- { serverError BAD_ARGUMENTS }

-- Combination DoubleDelta & Time is okay

SET enable_time_time64_type = 1;
CREATE TABLE tab(c0 Time CODEC(DoubleDelta)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE tab (c0) VALUES ('100:00:00');
DROP TABLE tab;

CREATE TABLE tab(c0 Nullable(Time) CODEC(DoubleDelta)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE tab(c0) VALUES ('100:00:00');
INSERT INTO TABLE tab(c0) VALUES (NULL);
DROP TABLE tab;

-- LowCardinality(Nullable(Time)) is rejected
CREATE TABLE tab(c0 LowCardinality(Nullable(Time)) CODEC(DoubleDelta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab(c0 LowCardinality(Nullable(Time)) CODEC(DoubleDelta(2))) ENGINE = MergeTree() ORDER BY tuple();  -- { serverError BAD_ARGUMENTS }

-- This statement from issue #80220 is expected to fail:
CREATE TABLE tab(c0 String) ENGINE = MergeTree() ORDER BY tuple();
CREATE MATERIALIZED VIEW v0 REFRESH AFTER 1 SECOND APPEND TO tab (c0 String CODEC(DoubleDelta(2))) EMPTY AS (SELECT 'a' AS c0); -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;
