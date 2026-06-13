-- Tags: no-fasttest
-- `PCO` requires a column type, so it must be rejected at CREATE time in the untyped MergeTree
-- compression settings instead of failing later, at the first write. The `default_compression_codec`
-- setting is re-resolved with each column's type, but it bypasses the `allow_experimental_codecs`
-- validation of column-level codecs, so experimental codecs are rejected in all three settings.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_pco_settings;

CREATE TABLE t_pco_settings (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS marks_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_pco_settings (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS primary_key_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_pco_settings (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS default_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }

-- A typed column-level `PCO` on the same table is fine.
CREATE TABLE t_pco_settings (x UInt64 CODEC(PCO)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pco_settings SELECT number FROM numbers(1000);
SELECT sum(x != number) FROM t_pco_settings AS t LEFT JOIN numbers(1000) AS n ON t.x = n.number;

-- Changing the settings on an existing table is rejected as well.
ALTER TABLE t_pco_settings MODIFY SETTING marks_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_pco_settings MODIFY SETTING default_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_pco_settings;

-- TTL recompression is another untyped codec path (the codec is validated without a column type), so
-- it must reject `PCO` as well, instead of accepting it and failing later during a merge.
CREATE TABLE t_pco_ttl (d Date, x UInt64) ENGINE = MergeTree ORDER BY tuple()
    TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(PCO); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_pco_ttl (d Date, x UInt64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_pco_ttl MODIFY TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(PCO); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_pco_ttl;
