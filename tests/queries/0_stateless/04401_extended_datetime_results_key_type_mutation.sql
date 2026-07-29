-- A storage key or skip-index expression type must be stable and independent of the session settings
-- that change the RESULT TYPE of a key expression (enable_extended_results_for_datetime_functions,
-- cast_keep_nullable, geo_distance_returns_float64_on_float64_arguments,
-- function_json_value_return_type_allow_nullable, function_date_trunc_return_type_behavior,
-- allow_lossy_numeric_supertype, use_variant_as_common_type). Previously,
-- running a mutation (or any metadata-changing ALTER) with such a setting at a non-default value
-- recomputed the key/index type to a different type, diverging from the column the storage actually
-- produces and aborting with e.g. 'Bad cast from ColumnVector<UInt32> to ColumnDecimal<DateTime64>'.
-- See issue #109181.

DROP TABLE IF EXISTS t0;

-- Exact reproducer from the issue: the mutation triggered by CLEAR COLUMN must not abort.
CREATE TABLE t0 (c1 Date, c2 DateTime64) ENGINE = MergeTree() ORDER BY (toStartOfTenMinutes(c2));
SET enable_extended_results_for_datetime_functions = 1;
INSERT INTO t0 (c2, c1) VALUES ('2118-08-18 18:05:25', '1998-10-21');
ALTER TABLE t0 CLEAR COLUMN c1 SETTINGS mutations_sync = 1;
SELECT count() FROM t0;

DROP TABLE t0;

-- Broader case: a metadata-only ALTER with the setting on must not poison the in-memory key
-- type and break a subsequent plain INSERT that runs with the setting off.
SET enable_extended_results_for_datetime_functions = 0;
CREATE TABLE t0 (c1 Date, c2 DateTime64) ENGINE = MergeTree() ORDER BY (toStartOfTenMinutes(c2));
ALTER TABLE t0 MODIFY COMMENT 'touch' SETTINGS enable_extended_results_for_datetime_functions = 1;
INSERT INTO t0 (c2, c1) VALUES ('2118-08-18 18:05:25', '1998-10-21');
SELECT count() FROM t0;

DROP TABLE t0;

-- The key type is canonical (not extended) regardless of the session setting, and sort order,
-- key values and mutations remain correct.
SET enable_extended_results_for_datetime_functions = 1;
CREATE TABLE t0 (c1 Date, c2 DateTime64) ENGINE = MergeTree() ORDER BY (toStartOfTenMinutes(c2));
INSERT INTO t0 (c2, c1) VALUES ('2020-03-01 12:37:00', '2000-01-01'), ('2020-03-01 12:31:00', '2000-01-02'), ('2020-03-01 12:45:00', '2000-01-03'), ('2019-01-01 00:05:00', '1999-12-31');
OPTIMIZE TABLE t0 FINAL;
SELECT toTypeName(toStartOfTenMinutes(c2)) FROM t0 LIMIT 1 SETTINGS enable_extended_results_for_datetime_functions = 0;
SELECT toStartOfTenMinutes(c2) AS k FROM t0 ORDER BY k SETTINGS enable_extended_results_for_datetime_functions = 0;
ALTER TABLE t0 CLEAR COLUMN c1 SETTINGS mutations_sync = 1;
SELECT count(), uniqExact(toStartOfTenMinutes(c2)) FROM t0;

DROP TABLE t0;

-- Same class of bug for cast_keep_nullable: CAST(nullable AS T) returns Nullable(T) instead of the
-- canonical T, so a CREATE/ALTER run with it on recomputes a Nullable key type that diverges from the
-- non-nullable column the storage produces, aborting the next write with
-- 'Bad cast from ColumnVector<UInt32> to ColumnNullable'.

DROP TABLE IF EXISTS t1;

-- CREATE with the setting on then write must not abort. CREATE analyses the key under the storage
-- (global) context, so this case also passes on the merge base: it is non-regression coverage, and the
-- recomputation boundaries below (metadata-only ALTER, mutation) are the discriminating ones.
SET cast_keep_nullable = 1;
CREATE TABLE t1 (x Nullable(UInt32)) ENGINE = MergeTree() ORDER BY CAST(x AS UInt32) SETTINGS allow_nullable_key = 1;
INSERT INTO t1 VALUES (1);
SELECT count() FROM t1;

DROP TABLE t1;

-- Metadata-only ALTER with the setting on must not poison the key type for a later write run with it off.
SET cast_keep_nullable = 0;
CREATE TABLE t1 (x Nullable(UInt32)) ENGINE = MergeTree() ORDER BY CAST(x AS UInt32) SETTINGS allow_nullable_key = 1;
ALTER TABLE t1 MODIFY COMMENT 'touch' SETTINGS cast_keep_nullable = 1;
INSERT INTO t1 VALUES (1);
SELECT count() FROM t1;

DROP TABLE t1;

-- A CLEAR COLUMN mutation run with the setting on must not abort, and the key stays canonical
-- (non-nullable), sort order and values remain correct.
SET cast_keep_nullable = 1;
CREATE TABLE t1 (x Nullable(UInt32), y UInt32) ENGINE = MergeTree() ORDER BY CAST(x AS UInt32) SETTINGS allow_nullable_key = 1;
INSERT INTO t1 VALUES (5, 50), (1, 10), (3, 30), (2, 20);
OPTIMIZE TABLE t1 FINAL;
ALTER TABLE t1 CLEAR COLUMN y SETTINGS mutations_sync = 1;
SELECT count(), groupArray(x) FROM (SELECT x FROM t1 ORDER BY CAST(x AS UInt32));

DROP TABLE t1;

-- Secondary (skip) index expressions have the same class of bug: index.data_types/sample_block are
-- computed with the DDL session, while the actual index columns are produced with the canonical
-- (default-settings) context, so a CREATE/ALTER run with a type-affecting setting on records an
-- extended/nullable index type that diverges from the produced column and aborts the next write.

DROP TABLE IF EXISTS ti;

-- Metadata-only ALTER with the setting on must not poison a set index type, breaking a later INSERT.
SET enable_extended_results_for_datetime_functions = 0;
CREATE TABLE ti (ts DateTime64(3), INDEX idx toStartOfTenMinutes(ts) TYPE set(0) GRANULARITY 1) ENGINE = MergeTree() ORDER BY ts;
ALTER TABLE ti MODIFY COMMENT 'touch' SETTINGS enable_extended_results_for_datetime_functions = 1;
INSERT INTO ti VALUES ('2020-01-01 00:00:00');
SELECT count() FROM ti;

DROP TABLE ti;

-- A CLEAR COLUMN mutation run with the setting on must recalculate a set index without aborting.
SET enable_extended_results_for_datetime_functions = 1;
CREATE TABLE ti (ts DateTime64(3), x UInt32, INDEX idx toStartOfTenMinutes(ts) TYPE set(0) GRANULARITY 1) ENGINE = MergeTree() ORDER BY ts;
INSERT INTO ti VALUES ('2020-01-01 00:00:00', 5);
ALTER TABLE ti CLEAR COLUMN x SETTINGS mutations_sync = 1;
SELECT count() FROM ti;

DROP TABLE ti;

-- Same for a minmax index over toStartOfDay(DateTime64), and the index still prunes correctly.
SET enable_extended_results_for_datetime_functions = 0;
CREATE TABLE ti (ts DateTime64(3), INDEX idx toStartOfDay(ts) TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE ti MODIFY COMMENT 'touch' SETTINGS enable_extended_results_for_datetime_functions = 1;
INSERT INTO ti VALUES ('2020-01-01 00:00:00'), ('2020-06-01 00:00:00');
SELECT count() FROM ti WHERE ts >= '2020-05-01 00:00:00';
SELECT count() FROM ti WHERE toStartOfDay(ts) >= toStartOfDay(toDateTime64('2020-05-01 00:00:00', 3)) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE ti;

-- Same for a Date32 argument with toMonday. force_data_skipping_indices makes the query fail unless the
-- index is actually usable, so this does not pass by silently falling back to a full scan.
SET enable_extended_results_for_datetime_functions = 0;
CREATE TABLE ti (d Date32, INDEX idx toMonday(d) TYPE set(0) GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE ti MODIFY COMMENT 'touch' SETTINGS enable_extended_results_for_datetime_functions = 1;
INSERT INTO ti VALUES ('2020-01-01'), ('2020-06-01'), ('2021-03-01');
SELECT count() FROM ti;
SELECT count() FROM ti WHERE toMonday(d) = toMonday(toDate32('2020-06-01')) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE ti;

-- The cast_keep_nullable variant for a skip index: a set index over CAST(nullable AS T).
SET cast_keep_nullable = 0;
CREATE TABLE ti (x Nullable(UInt32), INDEX idx CAST(x AS UInt32) TYPE set(0) GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE ti MODIFY COMMENT 'touch' SETTINGS cast_keep_nullable = 1;
INSERT INTO ti VALUES (1), (2), (3);
SELECT count() FROM ti;
SELECT count() FROM ti WHERE CAST(x AS UInt32) = 2 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE ti;

-- Canonicalization must NOT leak into the query-time validator for parallel_replicas_custom_key.
-- getCustomKeyFilterForParallelReplica only validates the custom key type via KeyDescription, but the
-- filter it builds (custom_key >= lo AND custom_key < hi) executes later in the query context. With
-- cast_keep_nullable = 1, CAST(x AS UInt32) over Nullable(UInt32) produces Nullable(UInt32) at runtime,
-- so the range predicate is NULL for NULL rows and would silently drop them on every replica. That
-- configuration must therefore stay rejected up front (custom_key_range requires an unsigned integer),
-- not be canonicalized to plain UInt32 and accepted.
DROP TABLE IF EXISTS tck;
CREATE TABLE tck (x Nullable(UInt32), v UInt32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO tck VALUES (1, 10), (2, 20), (NULL, 30), (4, 40);

SELECT count()
FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), tck)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
  parallel_replicas_custom_key = 'CAST(x AS UInt32)', parallel_replicas_mode = 'custom_key_range',
  parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 10,
  cast_keep_nullable = 1; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

-- The custom_key_sampling mode has the same nullability hole: the filter positiveModulo(key, N) = r is
-- NULL for NULL keys, so a nullable custom key would silently drop those rows on every replica. The
-- sampling branch previously did no type validation at all, so this must now also be rejected up front.
SELECT count()
FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), tck)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
  parallel_replicas_custom_key = 'CAST(x AS UInt32)', parallel_replicas_mode = 'custom_key_sampling',
  cast_keep_nullable = 1; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

DROP TABLE tck;

-- The same class of bug is reachable through every session setting that changes the RESULT TYPE of a
-- key expression, not only enable_extended_results_for_datetime_functions / cast_keep_nullable. Five
-- more such settings are neutralized in createKeyExpressionContext; each is exercised below with the
-- metadata-poisoning recipe (create canonical, metadata-only ALTER with the setting flipped, then a
-- write with the default): before the fix the recomputed key type diverged from the produced column
-- and aborted the write with a Bad cast.

-- geo_distance_returns_float64_on_float64_arguments (default 1): geoDistance over Float64 returns
-- Float64 by default, Float32 when off. 'Bad cast from ColumnVector<double> to ColumnVector<float>'.
DROP TABLE IF EXISTS tg;
SET geo_distance_returns_float64_on_float64_arguments = 1;
CREATE TABLE tg (a Float64, b Float64, c Float64, d Float64) ENGINE = MergeTree() ORDER BY geoDistance(a, b, c, d);
ALTER TABLE tg MODIFY COMMENT 'touch' SETTINGS geo_distance_returns_float64_on_float64_arguments = 0;
INSERT INTO tg VALUES (55.0, 37.0, 55.1, 37.1);
SELECT count() FROM tg;

DROP TABLE tg;

-- function_date_trunc_return_type_behavior (default 0): dateTrunc over DateTime64/Date32 returns the
-- extended type by default, the canonical Date/DateTime when set to 1.
-- 'Bad cast from ColumnDecimal<DateTime64> to ColumnVector<...>'.
DROP TABLE IF EXISTS td;
SET function_date_trunc_return_type_behavior = 0;
CREATE TABLE td (ts DateTime64(3)) ENGINE = MergeTree() ORDER BY dateTrunc('hour', ts);
ALTER TABLE td MODIFY COMMENT 'touch' SETTINGS function_date_trunc_return_type_behavior = 1;
INSERT INTO td VALUES ('2020-01-01 00:00:00');
SELECT count() FROM td;

DROP TABLE td;

-- function_json_value_return_type_allow_nullable (default 0): JSON_VALUE returns String by default,
-- Nullable(String) when on. 'Bad cast from ColumnString to ColumnNullable'.
DROP TABLE IF EXISTS tj;
SET function_json_value_return_type_allow_nullable = 0;
CREATE TABLE tj (j String) ENGINE = MergeTree() ORDER BY JSON_VALUE(j, '$.a') SETTINGS allow_nullable_key = 1;
ALTER TABLE tj MODIFY COMMENT 'touch' SETTINGS function_json_value_return_type_allow_nullable = 1;
INSERT INTO tj VALUES ('{"a":"x"}');
SELECT count() FROM tj;

DROP TABLE tj;

-- least_greatest_legacy_null_behavior (default 0): greatest/least with a NULL argument ignores the NULL
-- and returns Nullable(T) by default; when set to 1 the NULL short-circuits the resolved type to
-- Nullable(Nothing). A metadata-only ALTER with the setting on recomputes the key type to
-- Nullable(Nothing) while the write still produces Nullable(UInt32), aborting with a Bad cast.
DROP TABLE IF EXISTS tlg;
SET least_greatest_legacy_null_behavior = 0;
CREATE TABLE tlg (x UInt32) ENGINE = MergeTree() ORDER BY greatest(x, NULL) SETTINGS allow_nullable_key = 1;
ALTER TABLE tlg MODIFY COMMENT 'touch' SETTINGS least_greatest_legacy_null_behavior = 1;
INSERT INTO tlg VALUES (1);
SELECT count() FROM tlg;

DROP TABLE tlg;


-- allow_lossy_numeric_supertype (default 0) and use_variant_as_common_type are the two knobs that decide
-- how if/multiIf/ifNull/coalesce/array/map resolve branches with no lossless common type. With
-- allow_lossy_numeric_supertype = 1 an all-numeric pair like Decimal64 and Float64 resolves to Float64,
-- so a skip index over if(c, dec, f64) is accepted and its serializer is fixed to Float64, while the
-- baseline resolves the same expression to Variant(Decimal(18, 3), Float64) and the produced column has
-- that type. Pinning the setting makes the recorded index type follow the baseline, so a later write in a
-- session without the setting keeps working.
DROP TABLE IF EXISTS tlossy;
SET allow_lossy_numeric_supertype = 0;
CREATE TABLE tlossy (dec Decimal64(3), f64 Float64, c UInt8) ENGINE = MergeTree() ORDER BY c;
SET allow_lossy_numeric_supertype = 1;
ALTER TABLE tlossy ADD INDEX idx if(c, dec, f64) TYPE set(0) GRANULARITY 1;
SET allow_lossy_numeric_supertype = 0;
INSERT INTO tlossy VALUES (1.5, 2.5, 1);
SELECT count() FROM tlossy;

DROP TABLE tlossy;

-- Same for use_variant_as_common_type, the other argument the supertype helpers take. Its built-in
-- default is 1, so here the divergent session value is 0: without the pin the index type is resolved
-- without the Variant fallback and the ALTER is rejected outright, while a reload of the same metadata
-- (which runs under the baseline) resolves it to Variant(Decimal(18, 3), Float64). Pinning makes the
-- ALTER resolve the type the same way the reload does. Only the `set` index accepts a Variant column
-- (minmax rejects it as BAD_ARGUMENTS and bloom_filter as ILLEGAL_COLUMN), hence `set` here.
-- With a baseline that has the setting off (a server under an older `compatibility`) the pin works in the
-- opposite direction and rejects the ALTER, which is what keeps the table loadable: recording a Variant
-- index type there makes every later ATTACH of the table fail with NO_COMMON_TYPE.
DROP TABLE IF EXISTS tvar;
CREATE TABLE tvar (dec Decimal64(3), f64 Float64, c UInt8) ENGINE = MergeTree() ORDER BY c;
ALTER TABLE tvar ADD INDEX idx if(c, dec, f64) TYPE set(0) GRANULARITY 1 SETTINGS use_variant_as_common_type = 0;
INSERT INTO tvar VALUES (1.5, 2.5, 1);
SELECT count() FROM tvar;

DROP TABLE tvar;

-- The sampling key is the one key that must NOT be canonicalized: its runtime filter (greaterOrEquals/
-- less on the sampling expression) is re-analyzed in the query context at read time
-- (MergeTreeDataSelectExecutor), so validating it under a pinned context would accept a config the
-- runtime filter then breaks. With cast_keep_nullable = 1, CAST(x AS UInt32) over a Nullable column
-- resolves to Nullable(UInt32); canonicalizing would strip that to plain UInt32 and pass validation,
-- but the read-time filter would still build a Nullable predicate that silently drops NULL rows on every
-- shard. Keeping the caller's context (canonicalize = false) makes validation reject the Nullable
-- sampling key, matching master and the parallel_replicas_custom_key opt-out already in this change.
DROP TABLE IF EXISTS tsmp;
SET cast_keep_nullable = 0;
CREATE TABLE tsmp (x Nullable(UInt32)) ENGINE = MergeTree() ORDER BY CAST(x AS UInt32) SETTINGS allow_nullable_key = 1;
SET cast_keep_nullable = 1;
ALTER TABLE tsmp MODIFY SAMPLE BY CAST(x AS UInt32); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

DROP TABLE tsmp;
