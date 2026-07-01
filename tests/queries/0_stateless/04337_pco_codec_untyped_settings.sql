-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- (the ATTACH cases below need an explicit table UUID, which requires the Atomic database engine; cf. 04046, 04159)
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

-- Inside a codec chain `PCO` resolves to `CompressionCodecMultiple`, which must still surface the
-- inner codec's experimental / column-type-requiring properties so the chain is rejected too.
CREATE TABLE t_pco_settings (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS marks_compression_codec = 'PCO, ZSTD(1)'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_pco_settings (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS primary_key_compression_codec = 'PCO, ZSTD(1)'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_pco_settings (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS default_compression_codec = 'ZSTD(1), PCO'; -- { serverError BAD_ARGUMENTS }

-- A chain of ordinary codecs that neither is experimental nor requires a column type is accepted.
CREATE TABLE t_pco_settings (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS marks_compression_codec = 'LZ4, ZSTD(1)';
DROP TABLE t_pco_settings;

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
-- `PCO` wrapped in a chain must be rejected in TTL recompression as well.
ALTER TABLE t_pco_ttl MODIFY TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(PCO, ZSTD(1)); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_pco_ttl;

-- `temporary_files_codec` is another untyped codec path: it is resolved without a column type when
-- an external sort/aggregation spills to disk. `PCO` must be rejected before the first spill write
-- instead of failing deep inside the compression of the temporary file.
SET temporary_files_codec = 'PCO';
SET max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;
SELECT number, count() FROM numbers(1000000) GROUP BY number FORMAT Null; -- { serverError BAD_ARGUMENTS }
SET max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0;
SELECT number FROM numbers(1000000) ORDER BY number FORMAT Null; -- { serverError BAD_ARGUMENTS }

-- The CREATE-time sanity check is skipped on ATTACH, but the experimental / column-type-requiring
-- codec gate must still apply to the untyped compression settings on a user ATTACH
-- (`LoadingStrictnessLevel::ATTACH`); otherwise a full-form `ATTACH TABLE ... SETTINGS
-- default_compression_codec = 'PCO'` would let the typed experimental `PCO` codec be used even with
-- `allow_experimental_codecs = 0`. A full-form ATTACH requires an explicit UUID for the Atomic
-- database engine (cf. 04046, 04159); a distinct UUID per statement avoids store-directory reuse.
SET allow_experimental_codecs = 0;

DROP TABLE IF EXISTS t_pco_attach_1 SYNC;
ATTACH TABLE t_pco_attach_1 UUID '00000000-0000-0000-0000-000000004337'
    (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS default_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }

-- The same holds for a codec chain and for the other untyped compression settings.
DROP TABLE IF EXISTS t_pco_attach_2 SYNC;
ATTACH TABLE t_pco_attach_2 UUID '00000000-0000-0000-0000-000000004338'
    (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS default_compression_codec = 'ZSTD(1), PCO'; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_pco_attach_3 SYNC;
ATTACH TABLE t_pco_attach_3 UUID '00000000-0000-0000-0000-000000004339'
    (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS marks_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }

-- A normal ATTACH with only ordinary untyped codecs is still accepted (the gate did not break the
-- common re-attach path). Short-form ATTACH reads the stored metadata and also runs the gate.
DROP TABLE IF EXISTS t_pco_attach_ok SYNC;
CREATE TABLE t_pco_attach_ok (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS default_compression_codec = 'LZ4';
DETACH TABLE t_pco_attach_ok;
ATTACH TABLE t_pco_attach_ok;
DROP TABLE t_pco_attach_ok SYNC;
