-- Skip indexes are used for OR-connected filter conditions with many atoms (`use_skip_indexes_for_disjunctions`),
-- up to `max_rpn_elements_for_skip_indexes_with_disjunctions` RPN elements.

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

-- 1563 granules
INSERT INTO tab SELECT number, number, 100000 - number FROM numbers(100000);
OPTIMIZE TABLE tab FINAL;

-- Each atom matches one row in a separate granule, so N atoms read N granules (64 * N rows).
-- If the skip indexes are not combined, neither index can prune a granule on its own and all 100000 rows are read.

SELECT '-- 40 atoms (79 RPN elements), default limit';
SELECT count() FROM tab
WHERE v1 = 100 OR v1 = 200 OR v1 = 300 OR v1 = 400 OR v1 = 500 OR v1 = 600 OR v1 = 700 OR v1 = 800 OR v1 = 900 OR v1 = 1000
   OR v1 = 1100 OR v1 = 1200 OR v1 = 1300 OR v1 = 1400 OR v1 = 1500 OR v1 = 1600 OR v1 = 1700 OR v1 = 1800 OR v1 = 1900 OR v1 = 2000
   OR v2 = 49900 OR v2 = 49800 OR v2 = 49700 OR v2 = 49600 OR v2 = 49500 OR v2 = 49400 OR v2 = 49300 OR v2 = 49200 OR v2 = 49100 OR v2 = 49000
   OR v2 = 48900 OR v2 = 48800 OR v2 = 48700 OR v2 = 48600 OR v2 = 48500 OR v2 = 48400 OR v2 = 48300 OR v2 = 48200 OR v2 = 48100 OR v2 = 48000
SETTINGS max_rows_to_read = 2560, read_overflow_mode = 'throw';
WITH
    (
        SELECT groupArray(trimBoth(explain)) FROM (
            EXPLAIN indexes = 1 SELECT count() FROM tab
            WHERE v1 = 100 OR v1 = 200 OR v1 = 300 OR v1 = 400 OR v1 = 500 OR v1 = 600 OR v1 = 700 OR v1 = 800 OR v1 = 900 OR v1 = 1000
               OR v1 = 1100 OR v1 = 1200 OR v1 = 1300 OR v1 = 1400 OR v1 = 1500 OR v1 = 1600 OR v1 = 1700 OR v1 = 1800 OR v1 = 1900 OR v1 = 2000
               OR v2 = 49900 OR v2 = 49800 OR v2 = 49700 OR v2 = 49600 OR v2 = 49500 OR v2 = 49400 OR v2 = 49300 OR v2 = 49200 OR v2 = 49100 OR v2 = 49000
               OR v2 = 48900 OR v2 = 48800 OR v2 = 48700 OR v2 = 48600 OR v2 = 48500 OR v2 = 48400 OR v2 = 48300 OR v2 = 48200 OR v2 = 48100 OR v2 = 48000
        )
    ) AS lines,
    indexOf(lines, 'Name: <Combined skip indexes>') AS pos
SELECT lines[pos], lines[pos + 3];

SELECT '-- 500 atoms (999 RPN elements), default limit: the skip indexes are not combined';
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
               OR v1 = 10100 OR v1 = 10200 OR v1 = 10300 OR v1 = 10400 OR v1 = 10500 OR v1 = 10600 OR v1 = 10700 OR v1 = 10800 OR v1 = 10900 OR v1 = 11000
               OR v1 = 11100 OR v1 = 11200 OR v1 = 11300 OR v1 = 11400 OR v1 = 11500 OR v1 = 11600 OR v1 = 11700 OR v1 = 11800 OR v1 = 11900 OR v1 = 12000
               OR v1 = 12100 OR v1 = 12200 OR v1 = 12300 OR v1 = 12400 OR v1 = 12500 OR v1 = 12600 OR v1 = 12700 OR v1 = 12800 OR v1 = 12900 OR v1 = 13000
               OR v1 = 13100 OR v1 = 13200 OR v1 = 13300 OR v1 = 13400 OR v1 = 13500 OR v1 = 13600 OR v1 = 13700 OR v1 = 13800 OR v1 = 13900 OR v1 = 14000
               OR v1 = 14100 OR v1 = 14200 OR v1 = 14300 OR v1 = 14400 OR v1 = 14500 OR v1 = 14600 OR v1 = 14700 OR v1 = 14800 OR v1 = 14900 OR v1 = 15000
               OR v1 = 15100 OR v1 = 15200 OR v1 = 15300 OR v1 = 15400 OR v1 = 15500 OR v1 = 15600 OR v1 = 15700 OR v1 = 15800 OR v1 = 15900 OR v1 = 16000
               OR v1 = 16100 OR v1 = 16200 OR v1 = 16300 OR v1 = 16400 OR v1 = 16500 OR v1 = 16600 OR v1 = 16700 OR v1 = 16800 OR v1 = 16900 OR v1 = 17000
               OR v1 = 17100 OR v1 = 17200 OR v1 = 17300 OR v1 = 17400 OR v1 = 17500 OR v1 = 17600 OR v1 = 17700 OR v1 = 17800 OR v1 = 17900 OR v1 = 18000
               OR v1 = 18100 OR v1 = 18200 OR v1 = 18300 OR v1 = 18400 OR v1 = 18500 OR v1 = 18600 OR v1 = 18700 OR v1 = 18800 OR v1 = 18900 OR v1 = 19000
               OR v1 = 19100 OR v1 = 19200 OR v1 = 19300 OR v1 = 19400 OR v1 = 19500 OR v1 = 19600 OR v1 = 19700 OR v1 = 19800 OR v1 = 19900 OR v1 = 20000
               OR v1 = 20100 OR v1 = 20200 OR v1 = 20300 OR v1 = 20400 OR v1 = 20500 OR v1 = 20600 OR v1 = 20700 OR v1 = 20800 OR v1 = 20900 OR v1 = 21000
               OR v1 = 21100 OR v1 = 21200 OR v1 = 21300 OR v1 = 21400 OR v1 = 21500 OR v1 = 21600 OR v1 = 21700 OR v1 = 21800 OR v1 = 21900 OR v1 = 22000
               OR v1 = 22100 OR v1 = 22200 OR v1 = 22300 OR v1 = 22400 OR v1 = 22500 OR v1 = 22600 OR v1 = 22700 OR v1 = 22800 OR v1 = 22900 OR v1 = 23000
               OR v1 = 23100 OR v1 = 23200 OR v1 = 23300 OR v1 = 23400 OR v1 = 23500 OR v1 = 23600 OR v1 = 23700 OR v1 = 23800 OR v1 = 23900 OR v1 = 24000
               OR v1 = 24100 OR v1 = 24200 OR v1 = 24300 OR v1 = 24400 OR v1 = 24500 OR v1 = 24600 OR v1 = 24700 OR v1 = 24800 OR v1 = 24900 OR v1 = 25000
               OR v2 = 49900 OR v2 = 49800 OR v2 = 49700 OR v2 = 49600 OR v2 = 49500 OR v2 = 49400 OR v2 = 49300 OR v2 = 49200 OR v2 = 49100 OR v2 = 49000
               OR v2 = 48900 OR v2 = 48800 OR v2 = 48700 OR v2 = 48600 OR v2 = 48500 OR v2 = 48400 OR v2 = 48300 OR v2 = 48200 OR v2 = 48100 OR v2 = 48000
               OR v2 = 47900 OR v2 = 47800 OR v2 = 47700 OR v2 = 47600 OR v2 = 47500 OR v2 = 47400 OR v2 = 47300 OR v2 = 47200 OR v2 = 47100 OR v2 = 47000
               OR v2 = 46900 OR v2 = 46800 OR v2 = 46700 OR v2 = 46600 OR v2 = 46500 OR v2 = 46400 OR v2 = 46300 OR v2 = 46200 OR v2 = 46100 OR v2 = 46000
               OR v2 = 45900 OR v2 = 45800 OR v2 = 45700 OR v2 = 45600 OR v2 = 45500 OR v2 = 45400 OR v2 = 45300 OR v2 = 45200 OR v2 = 45100 OR v2 = 45000
               OR v2 = 44900 OR v2 = 44800 OR v2 = 44700 OR v2 = 44600 OR v2 = 44500 OR v2 = 44400 OR v2 = 44300 OR v2 = 44200 OR v2 = 44100 OR v2 = 44000
               OR v2 = 43900 OR v2 = 43800 OR v2 = 43700 OR v2 = 43600 OR v2 = 43500 OR v2 = 43400 OR v2 = 43300 OR v2 = 43200 OR v2 = 43100 OR v2 = 43000
               OR v2 = 42900 OR v2 = 42800 OR v2 = 42700 OR v2 = 42600 OR v2 = 42500 OR v2 = 42400 OR v2 = 42300 OR v2 = 42200 OR v2 = 42100 OR v2 = 42000
               OR v2 = 41900 OR v2 = 41800 OR v2 = 41700 OR v2 = 41600 OR v2 = 41500 OR v2 = 41400 OR v2 = 41300 OR v2 = 41200 OR v2 = 41100 OR v2 = 41000
               OR v2 = 40900 OR v2 = 40800 OR v2 = 40700 OR v2 = 40600 OR v2 = 40500 OR v2 = 40400 OR v2 = 40300 OR v2 = 40200 OR v2 = 40100 OR v2 = 40000
               OR v2 = 39900 OR v2 = 39800 OR v2 = 39700 OR v2 = 39600 OR v2 = 39500 OR v2 = 39400 OR v2 = 39300 OR v2 = 39200 OR v2 = 39100 OR v2 = 39000
               OR v2 = 38900 OR v2 = 38800 OR v2 = 38700 OR v2 = 38600 OR v2 = 38500 OR v2 = 38400 OR v2 = 38300 OR v2 = 38200 OR v2 = 38100 OR v2 = 38000
               OR v2 = 37900 OR v2 = 37800 OR v2 = 37700 OR v2 = 37600 OR v2 = 37500 OR v2 = 37400 OR v2 = 37300 OR v2 = 37200 OR v2 = 37100 OR v2 = 37000
               OR v2 = 36900 OR v2 = 36800 OR v2 = 36700 OR v2 = 36600 OR v2 = 36500 OR v2 = 36400 OR v2 = 36300 OR v2 = 36200 OR v2 = 36100 OR v2 = 36000
               OR v2 = 35900 OR v2 = 35800 OR v2 = 35700 OR v2 = 35600 OR v2 = 35500 OR v2 = 35400 OR v2 = 35300 OR v2 = 35200 OR v2 = 35100 OR v2 = 35000
               OR v2 = 34900 OR v2 = 34800 OR v2 = 34700 OR v2 = 34600 OR v2 = 34500 OR v2 = 34400 OR v2 = 34300 OR v2 = 34200 OR v2 = 34100 OR v2 = 34000
               OR v2 = 33900 OR v2 = 33800 OR v2 = 33700 OR v2 = 33600 OR v2 = 33500 OR v2 = 33400 OR v2 = 33300 OR v2 = 33200 OR v2 = 33100 OR v2 = 33000
               OR v2 = 32900 OR v2 = 32800 OR v2 = 32700 OR v2 = 32600 OR v2 = 32500 OR v2 = 32400 OR v2 = 32300 OR v2 = 32200 OR v2 = 32100 OR v2 = 32000
               OR v2 = 31900 OR v2 = 31800 OR v2 = 31700 OR v2 = 31600 OR v2 = 31500 OR v2 = 31400 OR v2 = 31300 OR v2 = 31200 OR v2 = 31100 OR v2 = 31000
               OR v2 = 30900 OR v2 = 30800 OR v2 = 30700 OR v2 = 30600 OR v2 = 30500 OR v2 = 30400 OR v2 = 30300 OR v2 = 30200 OR v2 = 30100 OR v2 = 30000
               OR v2 = 29900 OR v2 = 29800 OR v2 = 29700 OR v2 = 29600 OR v2 = 29500 OR v2 = 29400 OR v2 = 29300 OR v2 = 29200 OR v2 = 29100 OR v2 = 29000
               OR v2 = 28900 OR v2 = 28800 OR v2 = 28700 OR v2 = 28600 OR v2 = 28500 OR v2 = 28400 OR v2 = 28300 OR v2 = 28200 OR v2 = 28100 OR v2 = 28000
               OR v2 = 27900 OR v2 = 27800 OR v2 = 27700 OR v2 = 27600 OR v2 = 27500 OR v2 = 27400 OR v2 = 27300 OR v2 = 27200 OR v2 = 27100 OR v2 = 27000
               OR v2 = 26900 OR v2 = 26800 OR v2 = 26700 OR v2 = 26600 OR v2 = 26500 OR v2 = 26400 OR v2 = 26300 OR v2 = 26200 OR v2 = 26100 OR v2 = 26000
               OR v2 = 25900 OR v2 = 25800 OR v2 = 25700 OR v2 = 25600 OR v2 = 25500 OR v2 = 25400 OR v2 = 25300 OR v2 = 25200 OR v2 = 25100 OR v2 = 25000
        )
    ) AS lines,
    indexOf(lines, 'Name: <Combined skip indexes>') AS pos
SELECT pos > 0;

SELECT '-- 500 atoms, raised limit';
SET max_rpn_elements_for_skip_indexes_with_disjunctions = 1000;
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
   OR v1 = 10100 OR v1 = 10200 OR v1 = 10300 OR v1 = 10400 OR v1 = 10500 OR v1 = 10600 OR v1 = 10700 OR v1 = 10800 OR v1 = 10900 OR v1 = 11000
   OR v1 = 11100 OR v1 = 11200 OR v1 = 11300 OR v1 = 11400 OR v1 = 11500 OR v1 = 11600 OR v1 = 11700 OR v1 = 11800 OR v1 = 11900 OR v1 = 12000
   OR v1 = 12100 OR v1 = 12200 OR v1 = 12300 OR v1 = 12400 OR v1 = 12500 OR v1 = 12600 OR v1 = 12700 OR v1 = 12800 OR v1 = 12900 OR v1 = 13000
   OR v1 = 13100 OR v1 = 13200 OR v1 = 13300 OR v1 = 13400 OR v1 = 13500 OR v1 = 13600 OR v1 = 13700 OR v1 = 13800 OR v1 = 13900 OR v1 = 14000
   OR v1 = 14100 OR v1 = 14200 OR v1 = 14300 OR v1 = 14400 OR v1 = 14500 OR v1 = 14600 OR v1 = 14700 OR v1 = 14800 OR v1 = 14900 OR v1 = 15000
   OR v1 = 15100 OR v1 = 15200 OR v1 = 15300 OR v1 = 15400 OR v1 = 15500 OR v1 = 15600 OR v1 = 15700 OR v1 = 15800 OR v1 = 15900 OR v1 = 16000
   OR v1 = 16100 OR v1 = 16200 OR v1 = 16300 OR v1 = 16400 OR v1 = 16500 OR v1 = 16600 OR v1 = 16700 OR v1 = 16800 OR v1 = 16900 OR v1 = 17000
   OR v1 = 17100 OR v1 = 17200 OR v1 = 17300 OR v1 = 17400 OR v1 = 17500 OR v1 = 17600 OR v1 = 17700 OR v1 = 17800 OR v1 = 17900 OR v1 = 18000
   OR v1 = 18100 OR v1 = 18200 OR v1 = 18300 OR v1 = 18400 OR v1 = 18500 OR v1 = 18600 OR v1 = 18700 OR v1 = 18800 OR v1 = 18900 OR v1 = 19000
   OR v1 = 19100 OR v1 = 19200 OR v1 = 19300 OR v1 = 19400 OR v1 = 19500 OR v1 = 19600 OR v1 = 19700 OR v1 = 19800 OR v1 = 19900 OR v1 = 20000
   OR v1 = 20100 OR v1 = 20200 OR v1 = 20300 OR v1 = 20400 OR v1 = 20500 OR v1 = 20600 OR v1 = 20700 OR v1 = 20800 OR v1 = 20900 OR v1 = 21000
   OR v1 = 21100 OR v1 = 21200 OR v1 = 21300 OR v1 = 21400 OR v1 = 21500 OR v1 = 21600 OR v1 = 21700 OR v1 = 21800 OR v1 = 21900 OR v1 = 22000
   OR v1 = 22100 OR v1 = 22200 OR v1 = 22300 OR v1 = 22400 OR v1 = 22500 OR v1 = 22600 OR v1 = 22700 OR v1 = 22800 OR v1 = 22900 OR v1 = 23000
   OR v1 = 23100 OR v1 = 23200 OR v1 = 23300 OR v1 = 23400 OR v1 = 23500 OR v1 = 23600 OR v1 = 23700 OR v1 = 23800 OR v1 = 23900 OR v1 = 24000
   OR v1 = 24100 OR v1 = 24200 OR v1 = 24300 OR v1 = 24400 OR v1 = 24500 OR v1 = 24600 OR v1 = 24700 OR v1 = 24800 OR v1 = 24900 OR v1 = 25000
   OR v2 = 49900 OR v2 = 49800 OR v2 = 49700 OR v2 = 49600 OR v2 = 49500 OR v2 = 49400 OR v2 = 49300 OR v2 = 49200 OR v2 = 49100 OR v2 = 49000
   OR v2 = 48900 OR v2 = 48800 OR v2 = 48700 OR v2 = 48600 OR v2 = 48500 OR v2 = 48400 OR v2 = 48300 OR v2 = 48200 OR v2 = 48100 OR v2 = 48000
   OR v2 = 47900 OR v2 = 47800 OR v2 = 47700 OR v2 = 47600 OR v2 = 47500 OR v2 = 47400 OR v2 = 47300 OR v2 = 47200 OR v2 = 47100 OR v2 = 47000
   OR v2 = 46900 OR v2 = 46800 OR v2 = 46700 OR v2 = 46600 OR v2 = 46500 OR v2 = 46400 OR v2 = 46300 OR v2 = 46200 OR v2 = 46100 OR v2 = 46000
   OR v2 = 45900 OR v2 = 45800 OR v2 = 45700 OR v2 = 45600 OR v2 = 45500 OR v2 = 45400 OR v2 = 45300 OR v2 = 45200 OR v2 = 45100 OR v2 = 45000
   OR v2 = 44900 OR v2 = 44800 OR v2 = 44700 OR v2 = 44600 OR v2 = 44500 OR v2 = 44400 OR v2 = 44300 OR v2 = 44200 OR v2 = 44100 OR v2 = 44000
   OR v2 = 43900 OR v2 = 43800 OR v2 = 43700 OR v2 = 43600 OR v2 = 43500 OR v2 = 43400 OR v2 = 43300 OR v2 = 43200 OR v2 = 43100 OR v2 = 43000
   OR v2 = 42900 OR v2 = 42800 OR v2 = 42700 OR v2 = 42600 OR v2 = 42500 OR v2 = 42400 OR v2 = 42300 OR v2 = 42200 OR v2 = 42100 OR v2 = 42000
   OR v2 = 41900 OR v2 = 41800 OR v2 = 41700 OR v2 = 41600 OR v2 = 41500 OR v2 = 41400 OR v2 = 41300 OR v2 = 41200 OR v2 = 41100 OR v2 = 41000
   OR v2 = 40900 OR v2 = 40800 OR v2 = 40700 OR v2 = 40600 OR v2 = 40500 OR v2 = 40400 OR v2 = 40300 OR v2 = 40200 OR v2 = 40100 OR v2 = 40000
   OR v2 = 39900 OR v2 = 39800 OR v2 = 39700 OR v2 = 39600 OR v2 = 39500 OR v2 = 39400 OR v2 = 39300 OR v2 = 39200 OR v2 = 39100 OR v2 = 39000
   OR v2 = 38900 OR v2 = 38800 OR v2 = 38700 OR v2 = 38600 OR v2 = 38500 OR v2 = 38400 OR v2 = 38300 OR v2 = 38200 OR v2 = 38100 OR v2 = 38000
   OR v2 = 37900 OR v2 = 37800 OR v2 = 37700 OR v2 = 37600 OR v2 = 37500 OR v2 = 37400 OR v2 = 37300 OR v2 = 37200 OR v2 = 37100 OR v2 = 37000
   OR v2 = 36900 OR v2 = 36800 OR v2 = 36700 OR v2 = 36600 OR v2 = 36500 OR v2 = 36400 OR v2 = 36300 OR v2 = 36200 OR v2 = 36100 OR v2 = 36000
   OR v2 = 35900 OR v2 = 35800 OR v2 = 35700 OR v2 = 35600 OR v2 = 35500 OR v2 = 35400 OR v2 = 35300 OR v2 = 35200 OR v2 = 35100 OR v2 = 35000
   OR v2 = 34900 OR v2 = 34800 OR v2 = 34700 OR v2 = 34600 OR v2 = 34500 OR v2 = 34400 OR v2 = 34300 OR v2 = 34200 OR v2 = 34100 OR v2 = 34000
   OR v2 = 33900 OR v2 = 33800 OR v2 = 33700 OR v2 = 33600 OR v2 = 33500 OR v2 = 33400 OR v2 = 33300 OR v2 = 33200 OR v2 = 33100 OR v2 = 33000
   OR v2 = 32900 OR v2 = 32800 OR v2 = 32700 OR v2 = 32600 OR v2 = 32500 OR v2 = 32400 OR v2 = 32300 OR v2 = 32200 OR v2 = 32100 OR v2 = 32000
   OR v2 = 31900 OR v2 = 31800 OR v2 = 31700 OR v2 = 31600 OR v2 = 31500 OR v2 = 31400 OR v2 = 31300 OR v2 = 31200 OR v2 = 31100 OR v2 = 31000
   OR v2 = 30900 OR v2 = 30800 OR v2 = 30700 OR v2 = 30600 OR v2 = 30500 OR v2 = 30400 OR v2 = 30300 OR v2 = 30200 OR v2 = 30100 OR v2 = 30000
   OR v2 = 29900 OR v2 = 29800 OR v2 = 29700 OR v2 = 29600 OR v2 = 29500 OR v2 = 29400 OR v2 = 29300 OR v2 = 29200 OR v2 = 29100 OR v2 = 29000
   OR v2 = 28900 OR v2 = 28800 OR v2 = 28700 OR v2 = 28600 OR v2 = 28500 OR v2 = 28400 OR v2 = 28300 OR v2 = 28200 OR v2 = 28100 OR v2 = 28000
   OR v2 = 27900 OR v2 = 27800 OR v2 = 27700 OR v2 = 27600 OR v2 = 27500 OR v2 = 27400 OR v2 = 27300 OR v2 = 27200 OR v2 = 27100 OR v2 = 27000
   OR v2 = 26900 OR v2 = 26800 OR v2 = 26700 OR v2 = 26600 OR v2 = 26500 OR v2 = 26400 OR v2 = 26300 OR v2 = 26200 OR v2 = 26100 OR v2 = 26000
   OR v2 = 25900 OR v2 = 25800 OR v2 = 25700 OR v2 = 25600 OR v2 = 25500 OR v2 = 25400 OR v2 = 25300 OR v2 = 25200 OR v2 = 25100 OR v2 = 25000
SETTINGS max_rows_to_read = 32000, read_overflow_mode = 'throw';
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
               OR v1 = 10100 OR v1 = 10200 OR v1 = 10300 OR v1 = 10400 OR v1 = 10500 OR v1 = 10600 OR v1 = 10700 OR v1 = 10800 OR v1 = 10900 OR v1 = 11000
               OR v1 = 11100 OR v1 = 11200 OR v1 = 11300 OR v1 = 11400 OR v1 = 11500 OR v1 = 11600 OR v1 = 11700 OR v1 = 11800 OR v1 = 11900 OR v1 = 12000
               OR v1 = 12100 OR v1 = 12200 OR v1 = 12300 OR v1 = 12400 OR v1 = 12500 OR v1 = 12600 OR v1 = 12700 OR v1 = 12800 OR v1 = 12900 OR v1 = 13000
               OR v1 = 13100 OR v1 = 13200 OR v1 = 13300 OR v1 = 13400 OR v1 = 13500 OR v1 = 13600 OR v1 = 13700 OR v1 = 13800 OR v1 = 13900 OR v1 = 14000
               OR v1 = 14100 OR v1 = 14200 OR v1 = 14300 OR v1 = 14400 OR v1 = 14500 OR v1 = 14600 OR v1 = 14700 OR v1 = 14800 OR v1 = 14900 OR v1 = 15000
               OR v1 = 15100 OR v1 = 15200 OR v1 = 15300 OR v1 = 15400 OR v1 = 15500 OR v1 = 15600 OR v1 = 15700 OR v1 = 15800 OR v1 = 15900 OR v1 = 16000
               OR v1 = 16100 OR v1 = 16200 OR v1 = 16300 OR v1 = 16400 OR v1 = 16500 OR v1 = 16600 OR v1 = 16700 OR v1 = 16800 OR v1 = 16900 OR v1 = 17000
               OR v1 = 17100 OR v1 = 17200 OR v1 = 17300 OR v1 = 17400 OR v1 = 17500 OR v1 = 17600 OR v1 = 17700 OR v1 = 17800 OR v1 = 17900 OR v1 = 18000
               OR v1 = 18100 OR v1 = 18200 OR v1 = 18300 OR v1 = 18400 OR v1 = 18500 OR v1 = 18600 OR v1 = 18700 OR v1 = 18800 OR v1 = 18900 OR v1 = 19000
               OR v1 = 19100 OR v1 = 19200 OR v1 = 19300 OR v1 = 19400 OR v1 = 19500 OR v1 = 19600 OR v1 = 19700 OR v1 = 19800 OR v1 = 19900 OR v1 = 20000
               OR v1 = 20100 OR v1 = 20200 OR v1 = 20300 OR v1 = 20400 OR v1 = 20500 OR v1 = 20600 OR v1 = 20700 OR v1 = 20800 OR v1 = 20900 OR v1 = 21000
               OR v1 = 21100 OR v1 = 21200 OR v1 = 21300 OR v1 = 21400 OR v1 = 21500 OR v1 = 21600 OR v1 = 21700 OR v1 = 21800 OR v1 = 21900 OR v1 = 22000
               OR v1 = 22100 OR v1 = 22200 OR v1 = 22300 OR v1 = 22400 OR v1 = 22500 OR v1 = 22600 OR v1 = 22700 OR v1 = 22800 OR v1 = 22900 OR v1 = 23000
               OR v1 = 23100 OR v1 = 23200 OR v1 = 23300 OR v1 = 23400 OR v1 = 23500 OR v1 = 23600 OR v1 = 23700 OR v1 = 23800 OR v1 = 23900 OR v1 = 24000
               OR v1 = 24100 OR v1 = 24200 OR v1 = 24300 OR v1 = 24400 OR v1 = 24500 OR v1 = 24600 OR v1 = 24700 OR v1 = 24800 OR v1 = 24900 OR v1 = 25000
               OR v2 = 49900 OR v2 = 49800 OR v2 = 49700 OR v2 = 49600 OR v2 = 49500 OR v2 = 49400 OR v2 = 49300 OR v2 = 49200 OR v2 = 49100 OR v2 = 49000
               OR v2 = 48900 OR v2 = 48800 OR v2 = 48700 OR v2 = 48600 OR v2 = 48500 OR v2 = 48400 OR v2 = 48300 OR v2 = 48200 OR v2 = 48100 OR v2 = 48000
               OR v2 = 47900 OR v2 = 47800 OR v2 = 47700 OR v2 = 47600 OR v2 = 47500 OR v2 = 47400 OR v2 = 47300 OR v2 = 47200 OR v2 = 47100 OR v2 = 47000
               OR v2 = 46900 OR v2 = 46800 OR v2 = 46700 OR v2 = 46600 OR v2 = 46500 OR v2 = 46400 OR v2 = 46300 OR v2 = 46200 OR v2 = 46100 OR v2 = 46000
               OR v2 = 45900 OR v2 = 45800 OR v2 = 45700 OR v2 = 45600 OR v2 = 45500 OR v2 = 45400 OR v2 = 45300 OR v2 = 45200 OR v2 = 45100 OR v2 = 45000
               OR v2 = 44900 OR v2 = 44800 OR v2 = 44700 OR v2 = 44600 OR v2 = 44500 OR v2 = 44400 OR v2 = 44300 OR v2 = 44200 OR v2 = 44100 OR v2 = 44000
               OR v2 = 43900 OR v2 = 43800 OR v2 = 43700 OR v2 = 43600 OR v2 = 43500 OR v2 = 43400 OR v2 = 43300 OR v2 = 43200 OR v2 = 43100 OR v2 = 43000
               OR v2 = 42900 OR v2 = 42800 OR v2 = 42700 OR v2 = 42600 OR v2 = 42500 OR v2 = 42400 OR v2 = 42300 OR v2 = 42200 OR v2 = 42100 OR v2 = 42000
               OR v2 = 41900 OR v2 = 41800 OR v2 = 41700 OR v2 = 41600 OR v2 = 41500 OR v2 = 41400 OR v2 = 41300 OR v2 = 41200 OR v2 = 41100 OR v2 = 41000
               OR v2 = 40900 OR v2 = 40800 OR v2 = 40700 OR v2 = 40600 OR v2 = 40500 OR v2 = 40400 OR v2 = 40300 OR v2 = 40200 OR v2 = 40100 OR v2 = 40000
               OR v2 = 39900 OR v2 = 39800 OR v2 = 39700 OR v2 = 39600 OR v2 = 39500 OR v2 = 39400 OR v2 = 39300 OR v2 = 39200 OR v2 = 39100 OR v2 = 39000
               OR v2 = 38900 OR v2 = 38800 OR v2 = 38700 OR v2 = 38600 OR v2 = 38500 OR v2 = 38400 OR v2 = 38300 OR v2 = 38200 OR v2 = 38100 OR v2 = 38000
               OR v2 = 37900 OR v2 = 37800 OR v2 = 37700 OR v2 = 37600 OR v2 = 37500 OR v2 = 37400 OR v2 = 37300 OR v2 = 37200 OR v2 = 37100 OR v2 = 37000
               OR v2 = 36900 OR v2 = 36800 OR v2 = 36700 OR v2 = 36600 OR v2 = 36500 OR v2 = 36400 OR v2 = 36300 OR v2 = 36200 OR v2 = 36100 OR v2 = 36000
               OR v2 = 35900 OR v2 = 35800 OR v2 = 35700 OR v2 = 35600 OR v2 = 35500 OR v2 = 35400 OR v2 = 35300 OR v2 = 35200 OR v2 = 35100 OR v2 = 35000
               OR v2 = 34900 OR v2 = 34800 OR v2 = 34700 OR v2 = 34600 OR v2 = 34500 OR v2 = 34400 OR v2 = 34300 OR v2 = 34200 OR v2 = 34100 OR v2 = 34000
               OR v2 = 33900 OR v2 = 33800 OR v2 = 33700 OR v2 = 33600 OR v2 = 33500 OR v2 = 33400 OR v2 = 33300 OR v2 = 33200 OR v2 = 33100 OR v2 = 33000
               OR v2 = 32900 OR v2 = 32800 OR v2 = 32700 OR v2 = 32600 OR v2 = 32500 OR v2 = 32400 OR v2 = 32300 OR v2 = 32200 OR v2 = 32100 OR v2 = 32000
               OR v2 = 31900 OR v2 = 31800 OR v2 = 31700 OR v2 = 31600 OR v2 = 31500 OR v2 = 31400 OR v2 = 31300 OR v2 = 31200 OR v2 = 31100 OR v2 = 31000
               OR v2 = 30900 OR v2 = 30800 OR v2 = 30700 OR v2 = 30600 OR v2 = 30500 OR v2 = 30400 OR v2 = 30300 OR v2 = 30200 OR v2 = 30100 OR v2 = 30000
               OR v2 = 29900 OR v2 = 29800 OR v2 = 29700 OR v2 = 29600 OR v2 = 29500 OR v2 = 29400 OR v2 = 29300 OR v2 = 29200 OR v2 = 29100 OR v2 = 29000
               OR v2 = 28900 OR v2 = 28800 OR v2 = 28700 OR v2 = 28600 OR v2 = 28500 OR v2 = 28400 OR v2 = 28300 OR v2 = 28200 OR v2 = 28100 OR v2 = 28000
               OR v2 = 27900 OR v2 = 27800 OR v2 = 27700 OR v2 = 27600 OR v2 = 27500 OR v2 = 27400 OR v2 = 27300 OR v2 = 27200 OR v2 = 27100 OR v2 = 27000
               OR v2 = 26900 OR v2 = 26800 OR v2 = 26700 OR v2 = 26600 OR v2 = 26500 OR v2 = 26400 OR v2 = 26300 OR v2 = 26200 OR v2 = 26100 OR v2 = 26000
               OR v2 = 25900 OR v2 = 25800 OR v2 = 25700 OR v2 = 25600 OR v2 = 25500 OR v2 = 25400 OR v2 = 25300 OR v2 = 25200 OR v2 = 25100 OR v2 = 25000
        )
    ) AS lines,
    indexOf(lines, 'Name: <Combined skip indexes>') AS pos
SELECT lines[pos], lines[pos + 3];

SELECT '-- 500 atoms, no limit';
SET max_rpn_elements_for_skip_indexes_with_disjunctions = 0;
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
               OR v1 = 10100 OR v1 = 10200 OR v1 = 10300 OR v1 = 10400 OR v1 = 10500 OR v1 = 10600 OR v1 = 10700 OR v1 = 10800 OR v1 = 10900 OR v1 = 11000
               OR v1 = 11100 OR v1 = 11200 OR v1 = 11300 OR v1 = 11400 OR v1 = 11500 OR v1 = 11600 OR v1 = 11700 OR v1 = 11800 OR v1 = 11900 OR v1 = 12000
               OR v1 = 12100 OR v1 = 12200 OR v1 = 12300 OR v1 = 12400 OR v1 = 12500 OR v1 = 12600 OR v1 = 12700 OR v1 = 12800 OR v1 = 12900 OR v1 = 13000
               OR v1 = 13100 OR v1 = 13200 OR v1 = 13300 OR v1 = 13400 OR v1 = 13500 OR v1 = 13600 OR v1 = 13700 OR v1 = 13800 OR v1 = 13900 OR v1 = 14000
               OR v1 = 14100 OR v1 = 14200 OR v1 = 14300 OR v1 = 14400 OR v1 = 14500 OR v1 = 14600 OR v1 = 14700 OR v1 = 14800 OR v1 = 14900 OR v1 = 15000
               OR v1 = 15100 OR v1 = 15200 OR v1 = 15300 OR v1 = 15400 OR v1 = 15500 OR v1 = 15600 OR v1 = 15700 OR v1 = 15800 OR v1 = 15900 OR v1 = 16000
               OR v1 = 16100 OR v1 = 16200 OR v1 = 16300 OR v1 = 16400 OR v1 = 16500 OR v1 = 16600 OR v1 = 16700 OR v1 = 16800 OR v1 = 16900 OR v1 = 17000
               OR v1 = 17100 OR v1 = 17200 OR v1 = 17300 OR v1 = 17400 OR v1 = 17500 OR v1 = 17600 OR v1 = 17700 OR v1 = 17800 OR v1 = 17900 OR v1 = 18000
               OR v1 = 18100 OR v1 = 18200 OR v1 = 18300 OR v1 = 18400 OR v1 = 18500 OR v1 = 18600 OR v1 = 18700 OR v1 = 18800 OR v1 = 18900 OR v1 = 19000
               OR v1 = 19100 OR v1 = 19200 OR v1 = 19300 OR v1 = 19400 OR v1 = 19500 OR v1 = 19600 OR v1 = 19700 OR v1 = 19800 OR v1 = 19900 OR v1 = 20000
               OR v1 = 20100 OR v1 = 20200 OR v1 = 20300 OR v1 = 20400 OR v1 = 20500 OR v1 = 20600 OR v1 = 20700 OR v1 = 20800 OR v1 = 20900 OR v1 = 21000
               OR v1 = 21100 OR v1 = 21200 OR v1 = 21300 OR v1 = 21400 OR v1 = 21500 OR v1 = 21600 OR v1 = 21700 OR v1 = 21800 OR v1 = 21900 OR v1 = 22000
               OR v1 = 22100 OR v1 = 22200 OR v1 = 22300 OR v1 = 22400 OR v1 = 22500 OR v1 = 22600 OR v1 = 22700 OR v1 = 22800 OR v1 = 22900 OR v1 = 23000
               OR v1 = 23100 OR v1 = 23200 OR v1 = 23300 OR v1 = 23400 OR v1 = 23500 OR v1 = 23600 OR v1 = 23700 OR v1 = 23800 OR v1 = 23900 OR v1 = 24000
               OR v1 = 24100 OR v1 = 24200 OR v1 = 24300 OR v1 = 24400 OR v1 = 24500 OR v1 = 24600 OR v1 = 24700 OR v1 = 24800 OR v1 = 24900 OR v1 = 25000
               OR v2 = 49900 OR v2 = 49800 OR v2 = 49700 OR v2 = 49600 OR v2 = 49500 OR v2 = 49400 OR v2 = 49300 OR v2 = 49200 OR v2 = 49100 OR v2 = 49000
               OR v2 = 48900 OR v2 = 48800 OR v2 = 48700 OR v2 = 48600 OR v2 = 48500 OR v2 = 48400 OR v2 = 48300 OR v2 = 48200 OR v2 = 48100 OR v2 = 48000
               OR v2 = 47900 OR v2 = 47800 OR v2 = 47700 OR v2 = 47600 OR v2 = 47500 OR v2 = 47400 OR v2 = 47300 OR v2 = 47200 OR v2 = 47100 OR v2 = 47000
               OR v2 = 46900 OR v2 = 46800 OR v2 = 46700 OR v2 = 46600 OR v2 = 46500 OR v2 = 46400 OR v2 = 46300 OR v2 = 46200 OR v2 = 46100 OR v2 = 46000
               OR v2 = 45900 OR v2 = 45800 OR v2 = 45700 OR v2 = 45600 OR v2 = 45500 OR v2 = 45400 OR v2 = 45300 OR v2 = 45200 OR v2 = 45100 OR v2 = 45000
               OR v2 = 44900 OR v2 = 44800 OR v2 = 44700 OR v2 = 44600 OR v2 = 44500 OR v2 = 44400 OR v2 = 44300 OR v2 = 44200 OR v2 = 44100 OR v2 = 44000
               OR v2 = 43900 OR v2 = 43800 OR v2 = 43700 OR v2 = 43600 OR v2 = 43500 OR v2 = 43400 OR v2 = 43300 OR v2 = 43200 OR v2 = 43100 OR v2 = 43000
               OR v2 = 42900 OR v2 = 42800 OR v2 = 42700 OR v2 = 42600 OR v2 = 42500 OR v2 = 42400 OR v2 = 42300 OR v2 = 42200 OR v2 = 42100 OR v2 = 42000
               OR v2 = 41900 OR v2 = 41800 OR v2 = 41700 OR v2 = 41600 OR v2 = 41500 OR v2 = 41400 OR v2 = 41300 OR v2 = 41200 OR v2 = 41100 OR v2 = 41000
               OR v2 = 40900 OR v2 = 40800 OR v2 = 40700 OR v2 = 40600 OR v2 = 40500 OR v2 = 40400 OR v2 = 40300 OR v2 = 40200 OR v2 = 40100 OR v2 = 40000
               OR v2 = 39900 OR v2 = 39800 OR v2 = 39700 OR v2 = 39600 OR v2 = 39500 OR v2 = 39400 OR v2 = 39300 OR v2 = 39200 OR v2 = 39100 OR v2 = 39000
               OR v2 = 38900 OR v2 = 38800 OR v2 = 38700 OR v2 = 38600 OR v2 = 38500 OR v2 = 38400 OR v2 = 38300 OR v2 = 38200 OR v2 = 38100 OR v2 = 38000
               OR v2 = 37900 OR v2 = 37800 OR v2 = 37700 OR v2 = 37600 OR v2 = 37500 OR v2 = 37400 OR v2 = 37300 OR v2 = 37200 OR v2 = 37100 OR v2 = 37000
               OR v2 = 36900 OR v2 = 36800 OR v2 = 36700 OR v2 = 36600 OR v2 = 36500 OR v2 = 36400 OR v2 = 36300 OR v2 = 36200 OR v2 = 36100 OR v2 = 36000
               OR v2 = 35900 OR v2 = 35800 OR v2 = 35700 OR v2 = 35600 OR v2 = 35500 OR v2 = 35400 OR v2 = 35300 OR v2 = 35200 OR v2 = 35100 OR v2 = 35000
               OR v2 = 34900 OR v2 = 34800 OR v2 = 34700 OR v2 = 34600 OR v2 = 34500 OR v2 = 34400 OR v2 = 34300 OR v2 = 34200 OR v2 = 34100 OR v2 = 34000
               OR v2 = 33900 OR v2 = 33800 OR v2 = 33700 OR v2 = 33600 OR v2 = 33500 OR v2 = 33400 OR v2 = 33300 OR v2 = 33200 OR v2 = 33100 OR v2 = 33000
               OR v2 = 32900 OR v2 = 32800 OR v2 = 32700 OR v2 = 32600 OR v2 = 32500 OR v2 = 32400 OR v2 = 32300 OR v2 = 32200 OR v2 = 32100 OR v2 = 32000
               OR v2 = 31900 OR v2 = 31800 OR v2 = 31700 OR v2 = 31600 OR v2 = 31500 OR v2 = 31400 OR v2 = 31300 OR v2 = 31200 OR v2 = 31100 OR v2 = 31000
               OR v2 = 30900 OR v2 = 30800 OR v2 = 30700 OR v2 = 30600 OR v2 = 30500 OR v2 = 30400 OR v2 = 30300 OR v2 = 30200 OR v2 = 30100 OR v2 = 30000
               OR v2 = 29900 OR v2 = 29800 OR v2 = 29700 OR v2 = 29600 OR v2 = 29500 OR v2 = 29400 OR v2 = 29300 OR v2 = 29200 OR v2 = 29100 OR v2 = 29000
               OR v2 = 28900 OR v2 = 28800 OR v2 = 28700 OR v2 = 28600 OR v2 = 28500 OR v2 = 28400 OR v2 = 28300 OR v2 = 28200 OR v2 = 28100 OR v2 = 28000
               OR v2 = 27900 OR v2 = 27800 OR v2 = 27700 OR v2 = 27600 OR v2 = 27500 OR v2 = 27400 OR v2 = 27300 OR v2 = 27200 OR v2 = 27100 OR v2 = 27000
               OR v2 = 26900 OR v2 = 26800 OR v2 = 26700 OR v2 = 26600 OR v2 = 26500 OR v2 = 26400 OR v2 = 26300 OR v2 = 26200 OR v2 = 26100 OR v2 = 26000
               OR v2 = 25900 OR v2 = 25800 OR v2 = 25700 OR v2 = 25600 OR v2 = 25500 OR v2 = 25400 OR v2 = 25300 OR v2 = 25200 OR v2 = 25100 OR v2 = 25000
        )
    ) AS lines,
    indexOf(lines, 'Name: <Combined skip indexes>') AS pos
SELECT lines[pos], lines[pos + 3];

DROP TABLE tab;
