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

-- Computed key: the group_by key is toStartOfDay(ts), not a physical column. An earlier SET rewrites
-- ts (the column the key derives from), so the later toStartOfDay(ts) key must be recomputed before the
-- later aggregation groups by it. tgt maps each source day to one of two target days: rows must merge
-- into 2 final groups by the post-SET day, not fragment by the stale pre-SET day.
CREATE TABLE ttl_multi_group_by (ts DateTime, payload UInt64, tgt UInt8)
ENGINE = MergeTree ORDER BY toStartOfDay(ts)
TTL ts + toIntervalDay(1) GROUP BY toStartOfDay(ts) SET ts = toDateTime('2020-01-01 00:00:00') + toIntervalDay(any(tgt)), payload = sum(payload),
    ts + toIntervalDay(1) GROUP BY toStartOfDay(ts) SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 2;

INSERT INTO ttl_multi_group_by VALUES
    (toDateTime('2020-03-01 00:00:00'), 1, 0), (toDateTime('2020-03-02 00:00:00'), 10, 1),
    (toDateTime('2020-03-03 00:00:00'), 100, 0), (toDateTime('2020-03-04 00:00:00'), 1000, 1);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT ts, payload FROM ttl_multi_group_by ORDER BY ts;
SELECT count(), sum(payload) FROM ttl_multi_group_by;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- Subcolumn key: the group_by key is a subcolumn t.a of a Tuple column. An earlier SET rewrites the
-- whole tuple t; the later t.a key must be re-extracted from the post-SET t before grouping.
CREATE TABLE ttl_multi_group_by (t Tuple(a UInt32, b UInt32), ts DateTime, payload UInt64, newa UInt32)
ENGINE = MergeTree ORDER BY t.a
TTL ts + toIntervalDay(1) GROUP BY t.a SET t = tuple(any(newa), 0), payload = sum(payload),
    ts + toIntervalDay(1) GROUP BY t.a SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 2;

INSERT INTO ttl_multi_group_by VALUES
    (tuple(1, 0), '2020-03-01', 1, 7), (tuple(2, 0), '2020-03-01', 10, 8),
    (tuple(3, 0), '2020-03-01', 100, 7), (tuple(4, 0), '2020-03-01', 1000, 8);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT t.a AS a, payload FROM ttl_multi_group_by ORDER BY a;
SELECT count(), sum(payload) FROM ttl_multi_group_by;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- Non-sort-key MATERIALIZED column: d = toDate(ts) is MATERIALIZED but NOT in the sorting key. The SET
-- rewrites ts, so the stored d would stay stale (its pre-SET any(d) value) unless recomputed. d must be
-- refreshed to toDate(post-SET ts) in the written part.
-- The SET pushes ts 50 years into the future so the post-SET row is no longer expired and the GROUP BY
-- TTL fires exactly once; a smaller offset would leave ts still expired and the number of re-fires would
-- depend on merge scheduling, making the printed ts non-deterministic across runs.
CREATE TABLE ttl_multi_group_by (k UInt32, ts DateTime, d Date MATERIALIZED toDate(ts), payload UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalDay(1) GROUP BY k SET ts = max(ts) + toIntervalYear(50), payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_multi_group_by (k, ts, payload) VALUES (1, '2020-01-01 00:00:00', 10), (1, '2020-01-02 00:00:00', 20);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

-- d must equal toDate(ts) after the SET, not the stale pre-SET date.
SELECT k, ts, d, payload, d = toDate(ts) AS d_is_fresh FROM ttl_multi_group_by ORDER BY k;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- MATERIALIZE TTL mutation path (not merge): the same computed-key fix must hold when the GROUP BY TTL
-- runs as a mutation. Without the fix the last group flushed at end-of-stream keeps a stale key and
-- fragments (extra rows in the written part).
CREATE TABLE ttl_multi_group_by (ts DateTime, payload UInt64, tgt UInt8)
ENGINE = MergeTree ORDER BY toStartOfDay(ts)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_multi_group_by VALUES
    (toDateTime('2020-03-01 00:00:00'), 1, 0), (toDateTime('2020-03-02 00:00:00'), 10, 1),
    (toDateTime('2020-03-03 00:00:00'), 100, 0), (toDateTime('2020-03-04 00:00:00'), 1000, 1);

ALTER TABLE ttl_multi_group_by MODIFY TTL
    ts + toIntervalDay(1) GROUP BY toStartOfDay(ts) SET ts = toDateTime('2020-01-01 00:00:00') + toIntervalDay(any(tgt)), payload = sum(payload),
    ts + toIntervalDay(1) GROUP BY toStartOfDay(ts) SET payload = sum(payload)
SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE ttl_multi_group_by MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT ts, payload FROM ttl_multi_group_by ORDER BY ts;
SELECT count(), sum(payload) FROM ttl_multi_group_by;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- Skip index over a non-sort-key MATERIALIZED column affected by the SET: because d is recomputed
-- before the part is written, the rebuilt minmax index over d observes the fresh value, so a query
-- filtering on the post-SET d finds the row.
-- ts is pushed 50 years into the future (same reason as above) so the GROUP BY TTL fires exactly once
-- and the fresh d (post-SET, year 2070) is deterministic across merge schedules.
CREATE TABLE ttl_multi_group_by (k UInt32, ts DateTime, d Date MATERIALIZED toDate(ts), payload UInt64, INDEX d_idx d TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalDay(1) GROUP BY k SET ts = max(ts) + toIntervalYear(50), payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_multi_group_by (k, ts, payload) VALUES (1, '2020-01-01 00:00:00', 10), (1, '2020-01-02 00:00:00', 20);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

-- The minmax index over d must reflect the fresh d (post-SET, year 2070), so a range query on d that
-- only the fresh value satisfies still finds the row through the skip index. A stale index (pre-SET
-- d = 2020) would prune the granule and wrongly return 0.
SELECT count() FROM ttl_multi_group_by WHERE d >= toDate('2069-01-01') SETTINGS force_data_skipping_indices = 'd_idx';

DROP TABLE ttl_multi_group_by;

-- F1: computed key over a MATERIALIZED source. The group_by key is toStartOfMonth(d) and
-- d MATERIALIZED toDate(ts). An earlier SET rewrites ts, so the stored d is stale; the key
-- toStartOfMonth(d) must be rebuilt from the FRESH d (recompute the materialized source FIRST),
-- otherwise the later GROUP BY groups by the pre-SET month and fragments. Run via MATERIALIZE TTL
-- (mutation path) which exposes the aggregation grouping directly. tgt maps the 4 source months to
-- 2 post-SET months, so the 2nd GROUP BY must collapse the part to exactly 2 rows.
CREATE TABLE ttl_multi_group_by (ts DateTime, d Date MATERIALIZED toDate(ts), payload UInt64, tgt UInt8)
ENGINE = MergeTree ORDER BY toStartOfMonth(d)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 2;

INSERT INTO ttl_multi_group_by (ts, payload, tgt) VALUES
    ('2020-01-15 00:00:00', 1, 0), ('2020-02-15 00:00:00', 10, 1),
    ('2020-03-15 00:00:00', 100, 0), ('2020-04-15 00:00:00', 1000, 1);

ALTER TABLE ttl_multi_group_by MODIFY TTL
    ts + toIntervalDay(1) GROUP BY toStartOfMonth(d) SET ts = toDateTime('2021-06-15 00:00:00') + toIntervalMonth(any(tgt)), payload = sum(payload),
    ts + toIntervalDay(1) GROUP BY toStartOfMonth(d) SET payload = sum(payload)
SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE ttl_multi_group_by MATERIALIZE TTL SETTINGS mutations_sync = 2;

-- Exactly 2 physical rows (June/July 2021), each with the summed payload. 4 rows would mean the
-- 2nd GROUP BY grouped by the stale month.
SELECT ts, payload FROM ttl_multi_group_by ORDER BY ts;
SELECT count() FROM ttl_multi_group_by;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- F3: the affected-by-SET check must be scoped to the SPECIFIC group_by key, not the whole primary
-- key. ORDER BY (toStartOfDay(ts), user_id); the 2nd TTL groups only by toStartOfDay(ts), and an
-- earlier SET rewrites user_id (a sibling sort-key column the 2nd key does NOT depend on). The 2nd
-- TTL's input is still ordered by toStartOfDay(ts), so grouping must remain correct: 2 groups by day.
CREATE TABLE ttl_multi_group_by (ts DateTime, user_id UInt32, payload UInt64, nu UInt32)
ENGINE = MergeTree ORDER BY (toStartOfDay(ts), user_id)
TTL ts + toIntervalDay(1) GROUP BY toStartOfDay(ts), user_id SET user_id = max(nu), payload = sum(payload),
    ts + toIntervalDay(1) GROUP BY toStartOfDay(ts) SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 2;

INSERT INTO ttl_multi_group_by VALUES
    ('2020-03-01 01:00:00', 1, 1, 9), ('2020-03-01 02:00:00', 2, 10, 9),
    ('2020-03-02 01:00:00', 3, 100, 8), ('2020-03-02 02:00:00', 4, 1000, 8);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT toStartOfDay(ts) AS day, count(), sum(payload) FROM ttl_multi_group_by GROUP BY day ORDER BY day;
SELECT '---';

DROP TABLE ttl_multi_group_by;
