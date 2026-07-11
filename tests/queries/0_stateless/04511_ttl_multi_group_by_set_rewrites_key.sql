-- Multiple GROUP BY TTLs where an earlier SET rewrites a column that is a later GROUP BY TTL's
-- group_by key. The later aggregation must still merge all rows of a rewritten key into one group
-- instead of fragmenting them (the input is no longer ordered by that key after the SET).

DROP TABLE IF EXISTS ttl_multi_group_by;

-- Basic case: first SET rewrites k -> [2,1,2], second GROUP BY k must sum payload per final k.
CREATE TABLE ttl_multi_group_by (k UInt32, ts1 DateTime, ts2 DateTime, payload UInt64, v UInt32)
ENGINE = MergeTree ORDER BY k
TTL ts1 + toIntervalDay(1) GROUP BY k SET k = max(v),
    ts2 + toIntervalDay(1) GROUP BY k SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_multi_group_by VALUES (1, '2020-01-01', '2020-01-01', 100, 2), (2, '2020-01-01', '2020-01-01', 200, 1), (3, '2020-01-01', '2020-01-01', 400, 2);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT k, payload FROM ttl_multi_group_by ORDER BY k;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- Cross-block case: force a small merge block size so the rewritten key recurs across block
-- boundaries. All 100 rows must be preserved (3 final groups, total payload 100).
CREATE TABLE ttl_multi_group_by (k UInt32, ts1 DateTime, ts2 DateTime, payload UInt64, v UInt32)
ENGINE = MergeTree ORDER BY k
TTL ts1 + toIntervalDay(1) GROUP BY k SET k = max(v),
    ts2 + toIntervalDay(1) GROUP BY k SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 8;

INSERT INTO ttl_multi_group_by SELECT number, '2020-01-01', '2020-01-01', 1, number % 3 FROM numbers(100);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT k, payload FROM ttl_multi_group_by ORDER BY k;
SELECT count(), sum(payload) FROM ttl_multi_group_by;

DROP TABLE ttl_multi_group_by;
