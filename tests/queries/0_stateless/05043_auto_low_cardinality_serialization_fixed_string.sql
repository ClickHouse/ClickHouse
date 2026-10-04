-- Automatic `LowCardinality` serialization is chosen for `FixedString` columns as well as for
-- `String` ones. `ColumnFixedString` has its own `ColumnUnique` and serialization path, so it is
-- covered separately: insert, read, merge and mutation (rewrite) are all exercised here.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_auto_lc_fs;

CREATE TABLE t_auto_lc_fs
(
    id UInt64,
    lc FixedString(16) STATISTICS(uniq),    -- low cardinality -> LowCardinality
    hc FixedString(16) STATISTICS(uniq)     -- high cardinality -> Default
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 1.0,
    max_uniq_number_for_low_cardinality = 1000,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_auto_lc_fs;

INSERT INTO t_auto_lc_fs SELECT number, 'val_' || toString(number % 10), 'uniq_' || toString(number) FROM numbers(10000);
INSERT INTO t_auto_lc_fs SELECT number, 'item_' || toString(number % 7), 'uniq2_' || toString(number) FROM numbers(10000);

SELECT 'serialization per part';
SELECT name, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_fs' AND active AND NOT startsWith(column, '_')
ORDER BY name, column;

SELECT 'transparent type';
SELECT toTypeName(lc), toTypeName(hc) FROM t_auto_lc_fs LIMIT 1;

SELECT 'correctness';
SELECT count(), uniqExact(lc), uniqExact(hc) FROM t_auto_lc_fs;

SELECT 'functions on lc';
SELECT countIf(lc LIKE 'val\_%'), uniqExact(substring(lc, 1, 4)) FROM t_auto_lc_fs;

SELECT 'group by lc';
SELECT replaceAll(toString(lc), '\0', ''), count() FROM t_auto_lc_fs GROUP BY lc ORDER BY lc LIMIT 5;

SELECT 'after merge';
SYSTEM START MERGES t_auto_lc_fs;
OPTIMIZE TABLE t_auto_lc_fs FINAL;
SELECT name, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_fs' AND active AND NOT startsWith(column, '_')
ORDER BY name, column;
SELECT count(), uniqExact(lc), uniqExact(hc) FROM t_auto_lc_fs;

SELECT 'after mutation';
SET mutations_sync = 2;
ALTER TABLE t_auto_lc_fs UPDATE lc = 'upd_' || toString(id % 4) WHERE id % 2 = 0;
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_fs' AND active AND NOT startsWith(column, '_')
ORDER BY column;
SELECT count(), uniqExact(lc), uniqExact(hc) FROM t_auto_lc_fs;

SELECT 'after detach/attach';
DETACH TABLE t_auto_lc_fs;
ATTACH TABLE t_auto_lc_fs;
SELECT count(), uniqExact(lc) FROM t_auto_lc_fs;

DROP TABLE t_auto_lc_fs;
