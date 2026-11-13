-- Skip indexes are used for OR-connected filter conditions with many atoms (`use_skip_indexes_for_disjunctions`),
-- up to `use_skip_indexes_for_disjunctions_max_rpn_elements` RPN elements.

SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_for_disjunctions = 1;
SET explain_query_plan_default = 'legacy';
-- Keep the equalities as separate atoms instead of folding them into `IN`.
SET optimize_min_equality_disjunction_chain_length = 1000;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    v1 UInt32,
    v2 UInt32,
    INDEX v1_idx v1 TYPE minmax GRANULARITY 1,
    INDEX v2_idx v2 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0,
    min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- 625 granules
INSERT INTO tab SELECT number, number, 40000 - number FROM numbers(40000);
OPTIMIZE TABLE tab FINAL;

-- Each atom matches one row in a separate granule, so N atoms read N granules (64 * N rows).
-- If the skip indexes are not combined, neither index can prune a granule on its own and all 40000 rows are read.

SELECT '-- 40 atoms (79 RPN elements), default limit';
SELECT count() FROM tab
WHERE v1 = 100 OR v1 = 200 OR v1 = 300 OR v1 = 400 OR v1 = 500 OR v1 = 600 OR v1 = 700 OR v1 = 800 OR v1 = 900 OR v1 = 1000
   OR v1 = 1100 OR v1 = 1200 OR v1 = 1300 OR v1 = 1400 OR v1 = 1500 OR v1 = 1600 OR v1 = 1700 OR v1 = 1800 OR v1 = 1900 OR v1 = 2000
   OR v2 = 19900 OR v2 = 19800 OR v2 = 19700 OR v2 = 19600 OR v2 = 19500 OR v2 = 19400 OR v2 = 19300 OR v2 = 19200 OR v2 = 19100 OR v2 = 19000
   OR v2 = 18900 OR v2 = 18800 OR v2 = 18700 OR v2 = 18600 OR v2 = 18500 OR v2 = 18400 OR v2 = 18300 OR v2 = 18200 OR v2 = 18100 OR v2 = 18000
SETTINGS max_rows_to_read = 2560, read_overflow_mode = 'throw';
WITH
    (
        SELECT groupArray(trimBoth(explain)) FROM (
            EXPLAIN indexes = 1 SELECT count() FROM tab
            WHERE v1 = 100 OR v1 = 200 OR v1 = 300 OR v1 = 400 OR v1 = 500 OR v1 = 600 OR v1 = 700 OR v1 = 800 OR v1 = 900 OR v1 = 1000
               OR v1 = 1100 OR v1 = 1200 OR v1 = 1300 OR v1 = 1400 OR v1 = 1500 OR v1 = 1600 OR v1 = 1700 OR v1 = 1800 OR v1 = 1900 OR v1 = 2000
               OR v2 = 19900 OR v2 = 19800 OR v2 = 19700 OR v2 = 19600 OR v2 = 19500 OR v2 = 19400 OR v2 = 19300 OR v2 = 19200 OR v2 = 19100 OR v2 = 19000
               OR v2 = 18900 OR v2 = 18800 OR v2 = 18700 OR v2 = 18600 OR v2 = 18500 OR v2 = 18400 OR v2 = 18300 OR v2 = 18200 OR v2 = 18100 OR v2 = 18000
        )
    ) AS lines,
    indexOf(lines, 'Name: <Combined skip indexes>') AS pos
SELECT lines[pos], lines[pos + 3];

SELECT '-- 200 atoms (399 RPN elements), default limit: the skip indexes are not combined';
WITH
    (
        SELECT groupArray(trimBoth(explain)) FROM (
            EXPLAIN indexes = 1 SELECT count() FROM tab
            WHERE v1 = 100 OR v1 = 200 OR v1 = 300 OR v1 = 400 OR v1 = 500 OR v1 = 600 OR v1 = 700 OR v1 = 800 OR v1 = 900 OR v1 = 1000
               OR v1 = 1100 OR v1 = 1200 OR v1 = 1300 OR v1 = 1400 OR v1 = 1500 OR v1 = 1600 OR v1 = 1700 OR v1 = 1800 OR v1 = 1900 OR v1 = 2000
               OR v1 = 2100 OR v1 = 2200 OR v1 = 2300 OR v1 = 2400 OR v1 = 2500 OR v1 = 2600 OR v1 = 2700 OR v1 = 2800 OR v1 = 2900 OR v1 = 3000
               OR v1 = 3100 OR v1 = 3200 OR v1 = 3300 OR v1 = 3400 OR v1 = 3500 OR v1 = 3600 OR v1 = 3700 OR v1 = 3800 OR v1 = 3900 OR v1 = 4000
               OR v1 = 4100 OR v1 = 4200 OR v1 = 4300 OR v1 = 4400 OR v1 = 4500 OR v1 = 4600 OR v1 = 4700 OR v1 = 4800 OR v1 = 4900 OR v1 = 5000
               OR v1 = 5100 OR v1 = 5200 OR v1 = 5300 OR v1 = 5400 OR v1 = 5500 OR v1 = 5600 OR v1 = 5700 OR v1 = 5800 OR v1 = 5900 OR v1 = 6000
               OR v1 = 6100 OR v1 = 6200 OR v1 = 6300 OR v1 = 6400 OR v1 = 6500 OR v1 = 6600 OR v1 = 6700 OR v1 = 6800 OR v1 = 6900 OR v1 = 7000
               OR v1 = 7100 OR v1 = 7200 OR v1 = 7300 OR v1 = 7400 OR v1 = 7500 OR v1 = 7600 OR v1 = 7700 OR v1 = 7800 OR v1 = 7900 OR v1 = 8000
               OR v1 = 8100 OR v1 = 8200 OR v1 = 8300 OR v1 = 8400 OR v1 = 8500 OR v1 = 8600 OR v1 = 8700 OR v1 = 8800 OR v1 = 8900 OR v1 = 9000
               OR v1 = 9100 OR v1 = 9200 OR v1 = 9300 OR v1 = 9400 OR v1 = 9500 OR v1 = 9600 OR v1 = 9700 OR v1 = 9800 OR v1 = 9900 OR v1 = 10000
               OR v2 = 19900 OR v2 = 19800 OR v2 = 19700 OR v2 = 19600 OR v2 = 19500 OR v2 = 19400 OR v2 = 19300 OR v2 = 19200 OR v2 = 19100 OR v2 = 19000
               OR v2 = 18900 OR v2 = 18800 OR v2 = 18700 OR v2 = 18600 OR v2 = 18500 OR v2 = 18400 OR v2 = 18300 OR v2 = 18200 OR v2 = 18100 OR v2 = 18000
               OR v2 = 17900 OR v2 = 17800 OR v2 = 17700 OR v2 = 17600 OR v2 = 17500 OR v2 = 17400 OR v2 = 17300 OR v2 = 17200 OR v2 = 17100 OR v2 = 17000
               OR v2 = 16900 OR v2 = 16800 OR v2 = 16700 OR v2 = 16600 OR v2 = 16500 OR v2 = 16400 OR v2 = 16300 OR v2 = 16200 OR v2 = 16100 OR v2 = 16000
               OR v2 = 15900 OR v2 = 15800 OR v2 = 15700 OR v2 = 15600 OR v2 = 15500 OR v2 = 15400 OR v2 = 15300 OR v2 = 15200 OR v2 = 15100 OR v2 = 15000
               OR v2 = 14900 OR v2 = 14800 OR v2 = 14700 OR v2 = 14600 OR v2 = 14500 OR v2 = 14400 OR v2 = 14300 OR v2 = 14200 OR v2 = 14100 OR v2 = 14000
               OR v2 = 13900 OR v2 = 13800 OR v2 = 13700 OR v2 = 13600 OR v2 = 13500 OR v2 = 13400 OR v2 = 13300 OR v2 = 13200 OR v2 = 13100 OR v2 = 13000
               OR v2 = 12900 OR v2 = 12800 OR v2 = 12700 OR v2 = 12600 OR v2 = 12500 OR v2 = 12400 OR v2 = 12300 OR v2 = 12200 OR v2 = 12100 OR v2 = 12000
               OR v2 = 11900 OR v2 = 11800 OR v2 = 11700 OR v2 = 11600 OR v2 = 11500 OR v2 = 11400 OR v2 = 11300 OR v2 = 11200 OR v2 = 11100 OR v2 = 11000
               OR v2 = 10900 OR v2 = 10800 OR v2 = 10700 OR v2 = 10600 OR v2 = 10500 OR v2 = 10400 OR v2 = 10300 OR v2 = 10200 OR v2 = 10100 OR v2 = 10000
        )
    ) AS lines,
    indexOf(lines, 'Name: <Combined skip indexes>') AS pos
SELECT pos > 0;

SELECT '-- 200 atoms, raised limit';
SET use_skip_indexes_for_disjunctions_max_rpn_elements = 400;
SELECT count() FROM tab
WHERE v1 = 100 OR v1 = 200 OR v1 = 300 OR v1 = 400 OR v1 = 500 OR v1 = 600 OR v1 = 700 OR v1 = 800 OR v1 = 900 OR v1 = 1000
   OR v1 = 1100 OR v1 = 1200 OR v1 = 1300 OR v1 = 1400 OR v1 = 1500 OR v1 = 1600 OR v1 = 1700 OR v1 = 1800 OR v1 = 1900 OR v1 = 2000
   OR v1 = 2100 OR v1 = 2200 OR v1 = 2300 OR v1 = 2400 OR v1 = 2500 OR v1 = 2600 OR v1 = 2700 OR v1 = 2800 OR v1 = 2900 OR v1 = 3000
   OR v1 = 3100 OR v1 = 3200 OR v1 = 3300 OR v1 = 3400 OR v1 = 3500 OR v1 = 3600 OR v1 = 3700 OR v1 = 3800 OR v1 = 3900 OR v1 = 4000
   OR v1 = 4100 OR v1 = 4200 OR v1 = 4300 OR v1 = 4400 OR v1 = 4500 OR v1 = 4600 OR v1 = 4700 OR v1 = 4800 OR v1 = 4900 OR v1 = 5000
   OR v1 = 5100 OR v1 = 5200 OR v1 = 5300 OR v1 = 5400 OR v1 = 5500 OR v1 = 5600 OR v1 = 5700 OR v1 = 5800 OR v1 = 5900 OR v1 = 6000
   OR v1 = 6100 OR v1 = 6200 OR v1 = 6300 OR v1 = 6400 OR v1 = 6500 OR v1 = 6600 OR v1 = 6700 OR v1 = 6800 OR v1 = 6900 OR v1 = 7000
   OR v1 = 7100 OR v1 = 7200 OR v1 = 7300 OR v1 = 7400 OR v1 = 7500 OR v1 = 7600 OR v1 = 7700 OR v1 = 7800 OR v1 = 7900 OR v1 = 8000
   OR v1 = 8100 OR v1 = 8200 OR v1 = 8300 OR v1 = 8400 OR v1 = 8500 OR v1 = 8600 OR v1 = 8700 OR v1 = 8800 OR v1 = 8900 OR v1 = 9000
   OR v1 = 9100 OR v1 = 9200 OR v1 = 9300 OR v1 = 9400 OR v1 = 9500 OR v1 = 9600 OR v1 = 9700 OR v1 = 9800 OR v1 = 9900 OR v1 = 10000
   OR v2 = 19900 OR v2 = 19800 OR v2 = 19700 OR v2 = 19600 OR v2 = 19500 OR v2 = 19400 OR v2 = 19300 OR v2 = 19200 OR v2 = 19100 OR v2 = 19000
   OR v2 = 18900 OR v2 = 18800 OR v2 = 18700 OR v2 = 18600 OR v2 = 18500 OR v2 = 18400 OR v2 = 18300 OR v2 = 18200 OR v2 = 18100 OR v2 = 18000
   OR v2 = 17900 OR v2 = 17800 OR v2 = 17700 OR v2 = 17600 OR v2 = 17500 OR v2 = 17400 OR v2 = 17300 OR v2 = 17200 OR v2 = 17100 OR v2 = 17000
   OR v2 = 16900 OR v2 = 16800 OR v2 = 16700 OR v2 = 16600 OR v2 = 16500 OR v2 = 16400 OR v2 = 16300 OR v2 = 16200 OR v2 = 16100 OR v2 = 16000
   OR v2 = 15900 OR v2 = 15800 OR v2 = 15700 OR v2 = 15600 OR v2 = 15500 OR v2 = 15400 OR v2 = 15300 OR v2 = 15200 OR v2 = 15100 OR v2 = 15000
   OR v2 = 14900 OR v2 = 14800 OR v2 = 14700 OR v2 = 14600 OR v2 = 14500 OR v2 = 14400 OR v2 = 14300 OR v2 = 14200 OR v2 = 14100 OR v2 = 14000
   OR v2 = 13900 OR v2 = 13800 OR v2 = 13700 OR v2 = 13600 OR v2 = 13500 OR v2 = 13400 OR v2 = 13300 OR v2 = 13200 OR v2 = 13100 OR v2 = 13000
   OR v2 = 12900 OR v2 = 12800 OR v2 = 12700 OR v2 = 12600 OR v2 = 12500 OR v2 = 12400 OR v2 = 12300 OR v2 = 12200 OR v2 = 12100 OR v2 = 12000
   OR v2 = 11900 OR v2 = 11800 OR v2 = 11700 OR v2 = 11600 OR v2 = 11500 OR v2 = 11400 OR v2 = 11300 OR v2 = 11200 OR v2 = 11100 OR v2 = 11000
   OR v2 = 10900 OR v2 = 10800 OR v2 = 10700 OR v2 = 10600 OR v2 = 10500 OR v2 = 10400 OR v2 = 10300 OR v2 = 10200 OR v2 = 10100 OR v2 = 10000
SETTINGS max_rows_to_read = 12800, read_overflow_mode = 'throw';
WITH
    (
        SELECT groupArray(trimBoth(explain)) FROM (
            EXPLAIN indexes = 1 SELECT count() FROM tab
            WHERE v1 = 100 OR v1 = 200 OR v1 = 300 OR v1 = 400 OR v1 = 500 OR v1 = 600 OR v1 = 700 OR v1 = 800 OR v1 = 900 OR v1 = 1000
               OR v1 = 1100 OR v1 = 1200 OR v1 = 1300 OR v1 = 1400 OR v1 = 1500 OR v1 = 1600 OR v1 = 1700 OR v1 = 1800 OR v1 = 1900 OR v1 = 2000
               OR v1 = 2100 OR v1 = 2200 OR v1 = 2300 OR v1 = 2400 OR v1 = 2500 OR v1 = 2600 OR v1 = 2700 OR v1 = 2800 OR v1 = 2900 OR v1 = 3000
               OR v1 = 3100 OR v1 = 3200 OR v1 = 3300 OR v1 = 3400 OR v1 = 3500 OR v1 = 3600 OR v1 = 3700 OR v1 = 3800 OR v1 = 3900 OR v1 = 4000
               OR v1 = 4100 OR v1 = 4200 OR v1 = 4300 OR v1 = 4400 OR v1 = 4500 OR v1 = 4600 OR v1 = 4700 OR v1 = 4800 OR v1 = 4900 OR v1 = 5000
               OR v1 = 5100 OR v1 = 5200 OR v1 = 5300 OR v1 = 5400 OR v1 = 5500 OR v1 = 5600 OR v1 = 5700 OR v1 = 5800 OR v1 = 5900 OR v1 = 6000
               OR v1 = 6100 OR v1 = 6200 OR v1 = 6300 OR v1 = 6400 OR v1 = 6500 OR v1 = 6600 OR v1 = 6700 OR v1 = 6800 OR v1 = 6900 OR v1 = 7000
               OR v1 = 7100 OR v1 = 7200 OR v1 = 7300 OR v1 = 7400 OR v1 = 7500 OR v1 = 7600 OR v1 = 7700 OR v1 = 7800 OR v1 = 7900 OR v1 = 8000
               OR v1 = 8100 OR v1 = 8200 OR v1 = 8300 OR v1 = 8400 OR v1 = 8500 OR v1 = 8600 OR v1 = 8700 OR v1 = 8800 OR v1 = 8900 OR v1 = 9000
               OR v1 = 9100 OR v1 = 9200 OR v1 = 9300 OR v1 = 9400 OR v1 = 9500 OR v1 = 9600 OR v1 = 9700 OR v1 = 9800 OR v1 = 9900 OR v1 = 10000
               OR v2 = 19900 OR v2 = 19800 OR v2 = 19700 OR v2 = 19600 OR v2 = 19500 OR v2 = 19400 OR v2 = 19300 OR v2 = 19200 OR v2 = 19100 OR v2 = 19000
               OR v2 = 18900 OR v2 = 18800 OR v2 = 18700 OR v2 = 18600 OR v2 = 18500 OR v2 = 18400 OR v2 = 18300 OR v2 = 18200 OR v2 = 18100 OR v2 = 18000
               OR v2 = 17900 OR v2 = 17800 OR v2 = 17700 OR v2 = 17600 OR v2 = 17500 OR v2 = 17400 OR v2 = 17300 OR v2 = 17200 OR v2 = 17100 OR v2 = 17000
               OR v2 = 16900 OR v2 = 16800 OR v2 = 16700 OR v2 = 16600 OR v2 = 16500 OR v2 = 16400 OR v2 = 16300 OR v2 = 16200 OR v2 = 16100 OR v2 = 16000
               OR v2 = 15900 OR v2 = 15800 OR v2 = 15700 OR v2 = 15600 OR v2 = 15500 OR v2 = 15400 OR v2 = 15300 OR v2 = 15200 OR v2 = 15100 OR v2 = 15000
               OR v2 = 14900 OR v2 = 14800 OR v2 = 14700 OR v2 = 14600 OR v2 = 14500 OR v2 = 14400 OR v2 = 14300 OR v2 = 14200 OR v2 = 14100 OR v2 = 14000
               OR v2 = 13900 OR v2 = 13800 OR v2 = 13700 OR v2 = 13600 OR v2 = 13500 OR v2 = 13400 OR v2 = 13300 OR v2 = 13200 OR v2 = 13100 OR v2 = 13000
               OR v2 = 12900 OR v2 = 12800 OR v2 = 12700 OR v2 = 12600 OR v2 = 12500 OR v2 = 12400 OR v2 = 12300 OR v2 = 12200 OR v2 = 12100 OR v2 = 12000
               OR v2 = 11900 OR v2 = 11800 OR v2 = 11700 OR v2 = 11600 OR v2 = 11500 OR v2 = 11400 OR v2 = 11300 OR v2 = 11200 OR v2 = 11100 OR v2 = 11000
               OR v2 = 10900 OR v2 = 10800 OR v2 = 10700 OR v2 = 10600 OR v2 = 10500 OR v2 = 10400 OR v2 = 10300 OR v2 = 10200 OR v2 = 10100 OR v2 = 10000
        )
    ) AS lines,
    indexOf(lines, 'Name: <Combined skip indexes>') AS pos
SELECT lines[pos], lines[pos + 3];

SELECT '-- 200 atoms, no limit';
SET use_skip_indexes_for_disjunctions_max_rpn_elements = 0;
WITH
    (
        SELECT groupArray(trimBoth(explain)) FROM (
            EXPLAIN indexes = 1 SELECT count() FROM tab
            WHERE v1 = 100 OR v1 = 200 OR v1 = 300 OR v1 = 400 OR v1 = 500 OR v1 = 600 OR v1 = 700 OR v1 = 800 OR v1 = 900 OR v1 = 1000
               OR v1 = 1100 OR v1 = 1200 OR v1 = 1300 OR v1 = 1400 OR v1 = 1500 OR v1 = 1600 OR v1 = 1700 OR v1 = 1800 OR v1 = 1900 OR v1 = 2000
               OR v1 = 2100 OR v1 = 2200 OR v1 = 2300 OR v1 = 2400 OR v1 = 2500 OR v1 = 2600 OR v1 = 2700 OR v1 = 2800 OR v1 = 2900 OR v1 = 3000
               OR v1 = 3100 OR v1 = 3200 OR v1 = 3300 OR v1 = 3400 OR v1 = 3500 OR v1 = 3600 OR v1 = 3700 OR v1 = 3800 OR v1 = 3900 OR v1 = 4000
               OR v1 = 4100 OR v1 = 4200 OR v1 = 4300 OR v1 = 4400 OR v1 = 4500 OR v1 = 4600 OR v1 = 4700 OR v1 = 4800 OR v1 = 4900 OR v1 = 5000
               OR v1 = 5100 OR v1 = 5200 OR v1 = 5300 OR v1 = 5400 OR v1 = 5500 OR v1 = 5600 OR v1 = 5700 OR v1 = 5800 OR v1 = 5900 OR v1 = 6000
               OR v1 = 6100 OR v1 = 6200 OR v1 = 6300 OR v1 = 6400 OR v1 = 6500 OR v1 = 6600 OR v1 = 6700 OR v1 = 6800 OR v1 = 6900 OR v1 = 7000
               OR v1 = 7100 OR v1 = 7200 OR v1 = 7300 OR v1 = 7400 OR v1 = 7500 OR v1 = 7600 OR v1 = 7700 OR v1 = 7800 OR v1 = 7900 OR v1 = 8000
               OR v1 = 8100 OR v1 = 8200 OR v1 = 8300 OR v1 = 8400 OR v1 = 8500 OR v1 = 8600 OR v1 = 8700 OR v1 = 8800 OR v1 = 8900 OR v1 = 9000
               OR v1 = 9100 OR v1 = 9200 OR v1 = 9300 OR v1 = 9400 OR v1 = 9500 OR v1 = 9600 OR v1 = 9700 OR v1 = 9800 OR v1 = 9900 OR v1 = 10000
               OR v2 = 19900 OR v2 = 19800 OR v2 = 19700 OR v2 = 19600 OR v2 = 19500 OR v2 = 19400 OR v2 = 19300 OR v2 = 19200 OR v2 = 19100 OR v2 = 19000
               OR v2 = 18900 OR v2 = 18800 OR v2 = 18700 OR v2 = 18600 OR v2 = 18500 OR v2 = 18400 OR v2 = 18300 OR v2 = 18200 OR v2 = 18100 OR v2 = 18000
               OR v2 = 17900 OR v2 = 17800 OR v2 = 17700 OR v2 = 17600 OR v2 = 17500 OR v2 = 17400 OR v2 = 17300 OR v2 = 17200 OR v2 = 17100 OR v2 = 17000
               OR v2 = 16900 OR v2 = 16800 OR v2 = 16700 OR v2 = 16600 OR v2 = 16500 OR v2 = 16400 OR v2 = 16300 OR v2 = 16200 OR v2 = 16100 OR v2 = 16000
               OR v2 = 15900 OR v2 = 15800 OR v2 = 15700 OR v2 = 15600 OR v2 = 15500 OR v2 = 15400 OR v2 = 15300 OR v2 = 15200 OR v2 = 15100 OR v2 = 15000
               OR v2 = 14900 OR v2 = 14800 OR v2 = 14700 OR v2 = 14600 OR v2 = 14500 OR v2 = 14400 OR v2 = 14300 OR v2 = 14200 OR v2 = 14100 OR v2 = 14000
               OR v2 = 13900 OR v2 = 13800 OR v2 = 13700 OR v2 = 13600 OR v2 = 13500 OR v2 = 13400 OR v2 = 13300 OR v2 = 13200 OR v2 = 13100 OR v2 = 13000
               OR v2 = 12900 OR v2 = 12800 OR v2 = 12700 OR v2 = 12600 OR v2 = 12500 OR v2 = 12400 OR v2 = 12300 OR v2 = 12200 OR v2 = 12100 OR v2 = 12000
               OR v2 = 11900 OR v2 = 11800 OR v2 = 11700 OR v2 = 11600 OR v2 = 11500 OR v2 = 11400 OR v2 = 11300 OR v2 = 11200 OR v2 = 11100 OR v2 = 11000
               OR v2 = 10900 OR v2 = 10800 OR v2 = 10700 OR v2 = 10600 OR v2 = 10500 OR v2 = 10400 OR v2 = 10300 OR v2 = 10200 OR v2 = 10100 OR v2 = 10000
        )
    ) AS lines,
    indexOf(lines, 'Name: <Combined skip indexes>') AS pos
SELECT lines[pos], lines[pos + 3];

DROP TABLE tab;
