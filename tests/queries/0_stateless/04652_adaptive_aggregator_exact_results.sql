-- Tags: long

-- Bidirectional exact-result comparison of the adaptive aggregator against the baseline. The
-- other adaptive tests compare aggregates of the result sets (count/sum folds), which cannot
-- notice two keys exchanging their states; `EXCEPT ALL` in both directions proves the
-- multisets of (key, state...) rows are identical. All cells use deterministic aggregates over
-- deterministic data, so each direction's difference must be empty and every cell prints 0.

SET max_threads = 4;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT 'Count-only staging';
SELECT count() FROM
(
    (SELECT * FROM (SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
);

SELECT 'General aggregates';
SELECT count() FROM
(
    (SELECT * FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn, max(number) AS mx FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn, max(number) AS mx FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn, max(number) AS mx FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn, max(number) AS mx FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
);

SELECT 'String key with a string argument';
SELECT count() FROM
(
    (SELECT * FROM (SELECT concat('key_', toString(number % 100000)) AS k, count() AS c, max(toString(number)) AS m FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT concat('key_', toString(number % 100000)) AS k, count() AS c, max(toString(number)) AS m FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT concat('key_', toString(number % 100000)) AS k, count() AS c, max(toString(number)) AS m FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT concat('key_', toString(number % 100000)) AS k, count() AS c, max(toString(number)) AS m FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
);

SELECT 'Composite key';
SELECT count() FROM
(
    (SELECT * FROM (SELECT number % 1000 AS g1, toString(number % 50000) AS g2, sum(number) AS s FROM numbers_mt(400000) GROUP BY g1, g2 SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT number % 1000 AS g1, toString(number % 50000) AS g2, sum(number) AS s FROM numbers_mt(400000) GROUP BY g1, g2 SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT number % 1000 AS g1, toString(number % 50000) AS g2, sum(number) AS s FROM numbers_mt(400000) GROUP BY g1, g2 SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT number % 1000 AS g1, toString(number % 50000) AS g2, sum(number) AS s FROM numbers_mt(400000) GROUP BY g1, g2 SETTINGS enable_adaptive_aggregator = 0))
);

-- Frequent keys that first appear after the freeze are the design's accepted degradation:
-- they must still aggregate exactly, through staging and the merge-time dedup.
SELECT 'Late-hot keys';
SELECT count() FROM
(
    (SELECT * FROM (SELECT if(number < 200000, number, 200000 + number % 100) AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT if(number < 200000, number, 200000 + number % 100) AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT if(number < 200000, number, 200000 + number % 100) AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT if(number < 200000, number, 200000 + number % 100) AS g, count() AS c FROM numbers_mt(600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
);

-- The empty string is a legitimate staged key: the packed representation hands it out as a
-- view with a null data pointer, which the staging copy must keep away from memcpy.
SELECT 'Empty string keys';
SELECT count() FROM
(
    (SELECT * FROM (SELECT if(number % 5 = 0, '', toString(number % 60000)) AS k, count() AS c, sum(number) AS s FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT if(number % 5 = 0, '', toString(number % 60000)) AS k, count() AS c, sum(number) AS s FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1))
    UNION ALL
    (SELECT * FROM (SELECT if(number % 5 = 0, '', toString(number % 60000)) AS k, count() AS c, sum(number) AS s FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT if(number % 5 = 0, '', toString(number % 60000)) AS k, count() AS c, sum(number) AS s FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
);

SELECT 'Under external pressure';
SELECT count() FROM
(
    (SELECT * FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1))
    UNION ALL
    (SELECT * FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
);
