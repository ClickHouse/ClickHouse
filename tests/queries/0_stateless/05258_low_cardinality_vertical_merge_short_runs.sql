-- Vertical merge of parts with interleaving keys: `LowCardinality` columns are gathered as many short runs
-- from parts with different dictionaries, which takes the row-by-row path of `ColumnLowCardinality::insertRangeFrom`.

DROP TABLE IF EXISTS t_lc_short_runs;
DROP VIEW IF EXISTS v_lc_short_runs;

CREATE TABLE t_lc_short_runs
(
    k UInt32,
    v UInt64,
    lc1 LowCardinality(String),
    lc2 LowCardinality(String),
    lc3 LowCardinality(String),
    lc4 LowCardinality(String),
    lc5 LowCardinality(String),
    lc6 LowCardinality(String),
    lc7 LowCardinality(String),
    lc8 LowCardinality(String),
    lc9 LowCardinality(String),
    lc10 LowCardinality(String),
    lcn LowCardinality(Nullable(String)),
    m Map(LowCardinality(String), Int64)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

-- Each part has all 500 keys with 1 to 20 rows per key, so the merged part alternates between parts every few rows.
CREATE VIEW v_lc_short_runs AS
SELECT
    toUInt32(k) AS k,
    p * 1000000 + number AS v,
    'const-value' AS lc1,
    concat('part-', toString(p)) AS lc2,
    concat('key-', toString(k)) AS lc3,
    concat('key-part-', toString(k), '-', toString(p)) AS lc4,
    concat('mod7-', toString((k + p) % 7)) AS lc5,
    concat('mod300-', toString((k * 13 + p) % 300), repeat('x', (k % 30) + 5)) AS lc6,
    concat('row-', toString(number % 3)) AS lc7,
    if(k % 5 = 0, '', 'nonempty') AS lc8,
    concat('p-parity-', toString(p % 2)) AS lc9,
    concat('rnd-', toString(cityHash64(k, p, number) % 1000)) AS lc10,
    if((k + number) % 4 = 0, NULL, concat('n-', toString(k % 11))) AS lcn,
    if(k % 3 = 0, map(), map(concat('mk-', toString(k % 17)), toInt64(number), concat('mp-', toString(p)), toInt64(k))) AS m
FROM
(
    SELECT p, k, number
    FROM (SELECT number AS p FROM numbers(10)) AS parts
    CROSS JOIN (SELECT number AS k FROM numbers(500)) AS keys
    ARRAY JOIN range(1 + (k * 7 + p) % 20) AS number
)
WHERE p = {p:UInt64};

SYSTEM STOP MERGES t_lc_short_runs;

INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 0);
INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 1);
INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 2);
INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 3);
INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 4);
INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 5);
INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 6);
INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 7);
INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 8);
INSERT INTO t_lc_short_runs SELECT * FROM v_lc_short_runs(p = 9);

SELECT count(), countDistinct(_part) FROM t_lc_short_runs;
SELECT groupBitXor(cityHash64(*)) FROM t_lc_short_runs;

SYSTEM START MERGES t_lc_short_runs;
OPTIMIZE TABLE t_lc_short_runs FINAL;

SELECT count(), countDistinct(_part) FROM t_lc_short_runs;
SELECT groupBitXor(cityHash64(*)) FROM t_lc_short_runs;

SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm FROM system.part_log
-- The merge of the 10 inserted parts. `OPTIMIZE FINAL` may also rewrite the single merged part if a background merge came first.
WHERE database = currentDatabase() AND table = 't_lc_short_runs' AND event_type = 'MergeParts' AND length(merged_from) = 10;

DROP VIEW v_lc_short_runs;
DROP TABLE t_lc_short_runs;
