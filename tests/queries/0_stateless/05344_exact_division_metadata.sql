SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET optimize_use_projections = 0;
SET sql_compatible_division_for_integers = 1;

CREATE TABLE division_exact_metadata
(
    x Int64,
    d Float64 DEFAULT x / 2,
    m Float64 MATERIALIZED divide(x, 2),
    k Int64 ALIAS x / 2,
    CONSTRAINT nonnegative CHECK x / 2 = intDiv(x, 2),
    INDEX division_idx (x / 2) TYPE minmax GRANULARITY 1,
    PROJECTION division_projection (SELECT x / 2, count() GROUP BY x / 2)
)
ENGINE = MergeTree PARTITION BY x / 2 ORDER BY x / 2;
INSERT INTO division_exact_metadata (x) VALUES (1), (3), (5), (7);
SELECT count(), sum(d), sum(m), sum(k) FROM division_exact_metadata;
DETACH TABLE division_exact_metadata;
SET sql_compatible_division_for_integers = 0;
ATTACH TABLE division_exact_metadata;
SELECT x, d, m, k FROM division_exact_metadata WHERE k >= 1 ORDER BY k, x SETTINGS force_primary_key = 1;
INSERT INTO division_exact_metadata (x) VALUES (9);
SELECT partition, count() FROM system.parts WHERE database = currentDatabase() AND table = 'division_exact_metadata' AND active GROUP BY partition ORDER BY partition;
SELECT x FROM division_exact_metadata WHERE k >= 2 ORDER BY k, x SETTINGS optimize_read_in_order = 1, force_primary_key = 1;
SELECT k, count() FROM division_exact_metadata GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT position(create_table_query, 'divideExact') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'division_exact_metadata';
CREATE TABLE division_exact_copy AS division_exact_metadata;
INSERT INTO division_exact_copy (x) VALUES (3);
SELECT d, m, k FROM division_exact_copy;
DROP TABLE division_exact_copy;

-- Metadata written with the setting off has exactly the legacy spelling.
CREATE TABLE division_legacy_metadata
(
    x Int64,
    d Float64 DEFAULT x / 2,
    m Float64 MATERIALIZED divide(x, 2),
    k Float64 ALIAS x / 2,
    CONSTRAINT positive CHECK x / 2 != intDiv(x, 2),
    INDEX division_idx (x / 2) TYPE minmax GRANULARITY 1,
    PROJECTION division_projection (SELECT x / 2, count() GROUP BY x / 2)
)
ENGINE = MergeTree PARTITION BY x / 2 ORDER BY x / 2
SETTINGS allow_floating_point_partition_key = 1;
INSERT INTO division_legacy_metadata (x) VALUES (1), (3), (5), (7);
DETACH TABLE division_legacy_metadata;
SET sql_compatible_division_for_integers = 1;
ATTACH TABLE division_legacy_metadata;
SELECT x, d, m, k FROM division_legacy_metadata WHERE k >= 1 ORDER BY k, x SETTINGS force_primary_key = 1;
INSERT INTO division_legacy_metadata (x) VALUES (9);
SELECT partition, count() FROM system.parts WHERE database = currentDatabase() AND table = 'division_legacy_metadata' AND active GROUP BY partition ORDER BY partition;
SELECT k, count() FROM division_legacy_metadata GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT position(create_table_query, 'divideExact') = 0 FROM system.tables WHERE database = currentDatabase() AND name = 'division_legacy_metadata';
CREATE TABLE division_legacy_copy AS division_legacy_metadata;
INSERT INTO division_legacy_copy (x) VALUES (3);
SELECT d, m, k FROM division_legacy_copy;
DROP TABLE division_legacy_copy;

-- New `ALTER` payloads use the DDL session's semantics, independently of the existing columns.
ALTER TABLE division_legacy_metadata ADD COLUMN e Int64 DEFAULT x / 2;
SET sql_compatible_division_for_integers = 0;
DETACH TABLE division_legacy_metadata;
ATTACH TABLE division_legacy_metadata;
INSERT INTO division_legacy_metadata (x) VALUES (11);
SELECT d, e FROM division_legacy_metadata WHERE x = 11;
SET sql_compatible_division_for_integers = 1;
ALTER TABLE division_legacy_metadata UPDATE e = x / 2 WHERE x = 11 SETTINGS mutations_sync = 2;
SET sql_compatible_division_for_integers = 0;
SELECT d, e FROM division_legacy_metadata WHERE x = 11;

DROP TABLE division_exact_metadata;
DROP TABLE division_legacy_metadata;
