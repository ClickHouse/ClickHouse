-- Tests automatic LowCardinality serialization: a String/FixedString column with a `uniq`
-- statistic and a low cardinality estimate is stored in dictionary-encoded form, while keeping
-- its declared (non-LowCardinality) data type. The encoding is transparent to queries.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_auto_lc;

CREATE TABLE t_auto_lc
(
    id UInt64,
    lc String STATISTICS(uniq),         -- low cardinality -> LowCardinality
    hc String STATISTICS(uniq),         -- high cardinality -> Default
    num UInt64 STATISTICS(uniq),        -- low cardinality but not String -> Default
    sp String STATISTICS(uniq)          -- mostly default (empty) -> Sparse (sparse wins over LowCardinality)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.9,
    max_uniq_number_for_low_cardinality = 1000,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_auto_lc;

INSERT INTO t_auto_lc SELECT number, 'val_' || toString(number % 10), 'uniq_' || toString(number), number % 5 + 1, if (number % 100 = 0, 'x', '') FROM numbers(100000);
INSERT INTO t_auto_lc SELECT number, 'item_' || toString(number % 7), 'uniq2_' || toString(number), number % 6 + 10, if (number % 100 = 0, 'y', '') FROM numbers(100000);

SELECT 'serialization per part';
SELECT name, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc' AND active ORDER BY name, column;

SELECT 'transparent type';
SELECT toTypeName(lc), toTypeName(hc), toTypeName(sp) FROM t_auto_lc LIMIT 1;

SELECT 'correctness';
SELECT count(), uniqExact(lc), uniqExact(hc), uniqExact(num), uniqExact(sp) FROM t_auto_lc;

SELECT 'functions on lc';
SELECT sum(length(lc)), countIf(lc LIKE 'val\_%'), countIf(empty(sp)) FROM t_auto_lc;

SELECT 'group by lc';
SELECT lc, count() FROM t_auto_lc GROUP BY lc ORDER BY lc LIMIT 5;

SELECT 'after merge';
SYSTEM START MERGES t_auto_lc;
OPTIMIZE TABLE t_auto_lc FINAL;
SELECT name, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc' AND active ORDER BY name, column;
SELECT count(), uniqExact(lc), uniqExact(hc), uniqExact(num), uniqExact(sp) FROM t_auto_lc;

SELECT 'after detach/attach';
DETACH TABLE t_auto_lc;
ATTACH TABLE t_auto_lc;
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc' AND active ORDER BY column;
SELECT count(), uniqExact(lc) FROM t_auto_lc;

DROP TABLE t_auto_lc;
