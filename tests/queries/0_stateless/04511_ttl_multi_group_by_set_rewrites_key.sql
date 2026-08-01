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

-- F2: a MATERIALIZED column that reads both an EPHEMERAL column and a SET target cannot be recomputed
-- (ephemeral columns are not on disk). This must not crash or reject the merge; the merge completes and
-- the aggregated columns are still correct (the stale materialized value is a documented limitation, a
-- warning is logged). x = max(x) = 9, payload = sum = 30.
CREATE TABLE ttl_multi_group_by (k UInt32, ts DateTime, x UInt32, eph String EPHEMERAL 'E', m String MATERIALIZED concat(toString(x), eph), payload UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalDay(1) GROUP BY k SET x = max(x), payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_multi_group_by (k, ts, x, eph, payload) VALUES (1, '2020-01-01', 5, 'A', 10), (1, '2020-01-02', 9, 'B', 20);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT k, x, payload FROM ttl_multi_group_by ORDER BY k;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- G2: cascading order loss. Once an earlier GROUP BY TTL runs unsorted (its SET rewrites its own key),
-- it finalizes via the aggregator hash table, which does NOT preserve primary-key order. A later GROUP
-- BY TTL keyed on a shorter, unaffected prefix (day) therefore also receives an unordered stream and
-- must run unsorted too, otherwise its streaming flush-on-key-change re-fragments the day groups.
-- Here TTL1 groups by (day,region,user) and SET region (its own key) -> unsorted; TTL2 groups by
-- (day,region) SET payload; TTL3 groups by day SET payload. Expect exactly 5 rows (one per day),
-- each with the full per-day payload sum; more rows mean a later TTL fragmented the scrambled stream.
CREATE TABLE ttl_multi_group_by (day Date, region UInt32, user UInt32, ts DateTime, payload UInt64)
ENGINE = MergeTree ORDER BY (day, region, user)
TTL ts + toIntervalSecond(1) GROUP BY day, region, user SET region = max(region),
    ts + toIntervalSecond(1) GROUP BY day, region SET payload = sum(payload),
    ts + toIntervalSecond(1) GROUP BY day SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4, index_granularity = 4;

INSERT INTO ttl_multi_group_by SELECT toDate('2020-01-01') + (number % 5), number % 7, number, toDateTime('2020-01-01 00:00:00'), 1 FROM numbers(70);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT count() AS rows, sum(payload) AS total FROM ttl_multi_group_by;
SELECT day, count() AS n, sum(payload) AS p FROM ttl_multi_group_by GROUP BY day ORDER BY day;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- G2 negative: two GROUP BY TTLs whose keys are unrelated to any earlier SET must both keep the ordered
-- fast path (no cascade is triggered). TTL1 groups by k SET payload only (does NOT rewrite its own key
-- k), TTL2 groups by k SET payload. No key is ever rewritten, so neither TTL loses order and both run
-- sorted. Result must be a single group per k.
CREATE TABLE ttl_multi_group_by (k UInt32, ts DateTime, payload UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalDay(1) GROUP BY k SET payload = sum(payload),
    ts + toIntervalDay(1) GROUP BY k SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4, index_granularity = 4;

INSERT INTO ttl_multi_group_by SELECT number % 5, toDateTime('2020-01-01 00:00:00'), 1 FROM numbers(40);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT count() AS rows, sum(payload) AS total FROM ttl_multi_group_by;
SELECT k, payload FROM ttl_multi_group_by ORDER BY k;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- H1: transitive EPHEMERAL fail-loud. m1 MATERIALIZED concat(toString(x), eph) reads the ephemeral eph
-- and the SET target x; m2 MATERIALIZED lower(m1) reads neither directly but depends on m1, so it is
-- both unrecomputable (ephemeral hop) and affected by the SET. The merge must complete and the
-- aggregated columns must be correct; m1/m2 stay at their pre-SET any() values (documented limitation,
-- both warned). Before the fix only m1 was warned; the fix propagates the warning through m2 as well.
CREATE TABLE ttl_multi_group_by (k UInt32, ts DateTime, x UInt32, eph String EPHEMERAL 'E', m1 String MATERIALIZED concat(toString(x), eph), m2 String MATERIALIZED lower(m1), payload UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalDay(1) GROUP BY k SET x = max(x), payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_multi_group_by (k, ts, x, eph, payload) VALUES (1, '2020-01-01', 5, 'a', 10), (1, '2020-01-02', 9, 'b', 20);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT k, x, payload FROM ttl_multi_group_by ORDER BY k;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- H2: a not-yet-expired earlier GROUP BY TTL must NOT force a later one off the streaming fast path.
-- TTL1 GROUP BY k SET ts (rewrites ts, a column TTL2's key could derive from) expires 40 years out, so
-- it does NOT fire in this merge; TTL2 GROUP BY k SET payload fires now. Because TTL1 does not fire, its
-- SET never runs and the stream stays ordered by k for TTL2. Correctness must hold: one group per k with
-- the summed payload. (The optimization -- TTL2 keeping the sorted fast path -- is not directly
-- observable from SQL, but a regression would corrupt/fragment the result, which this asserts.)
CREATE TABLE ttl_multi_group_by (k UInt32, ts DateTime, payload UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalYear(40) GROUP BY k SET ts = max(ts),
    ts + toIntervalDay(1) GROUP BY k SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4;

INSERT INTO ttl_multi_group_by SELECT number % 5, toDateTime('2020-01-01 00:00:00'), 1 FROM numbers(40);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT count() AS rows, sum(payload) AS total FROM ttl_multi_group_by;
SELECT k, payload FROM ttl_multi_group_by ORDER BY k;
SELECT '---';

DROP TABLE ttl_multi_group_by;

-- H3: an unrelated expired TTL must NOT trigger the GROUP BY SET sort-key repair when the GROUP BY ... SET
-- TTL itself does not fire. A DELETE TTL (ts + 1 day) is expired and fires; the GROUP BY toStartOfDay(ts)
-- SET ts TTL (touches the sort key) expires 40 years out and does NOT fire in this merge. The written part
-- must be correct and consistent with its primary index (no needless re-sort, no corruption): the DELETE
-- removes payload<5 rows and the remaining rows keep their original day ordering / values.
CREATE TABLE ttl_multi_group_by (ts DateTime, payload UInt64)
ENGINE = MergeTree ORDER BY toStartOfDay(ts)
TTL ts + toIntervalDay(1) DELETE WHERE payload < 5,
    ts + toIntervalYear(40) GROUP BY toStartOfDay(ts) SET ts = max(ts), payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_multi_group_by SELECT toDateTime('2020-01-01 00:00:00') + toIntervalDay(number % 3), number FROM numbers(30);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

-- Rows with payload < 5 (values 0..4) deleted -> 25 rows remain, none of them the GROUP-BY-SET result.
SELECT count() AS rows, min(payload) AS min_payload, sum(payload) AS total FROM ttl_multi_group_by;
SELECT toStartOfDay(ts) AS day, count() FROM ttl_multi_group_by GROUP BY day ORDER BY day;

DROP TABLE ttl_multi_group_by;

SELECT '--- I1';

-- I1: a MATERIALIZED column computed from a tuple subcolumn that the SET rewrites must be recomputed
-- from the POST-SET physical column, not from the stale subcolumn already extracted before the TTL step.
-- ORDER BY tup.ts pre-materializes tup.ts; SET tup = (max(tup.ts)+50y, 9) rewrites the parent tuple, so
-- the stale tup.ts (and d MATERIALIZED toDate(tup.ts) recomputed from it) would keep the pre-SET value.
-- After the fix d must equal toDate(tup.ts) of the post-SET tuple (d_fresh = 1).
CREATE TABLE ttl_i1 (tup Tuple(ts DateTime, x UInt32), d Date MATERIALIZED toDate(tup.ts), payload UInt64)
ENGINE = MergeTree ORDER BY tup.ts
TTL tup.ts + toIntervalDay(1) GROUP BY tup.ts SET tup = tuple(max(tup.ts) + toIntervalYear(50), 9), payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_i1 (tup, payload) VALUES ((toDateTime('2020-01-01 00:00:00'), 5), 10), ((toDateTime('2020-01-01 00:00:00'), 6), 20);
OPTIMIZE TABLE ttl_i1 FINAL;

SELECT toDate(tup.ts) AS post_set_day, (d = toDate(tup.ts)) AS d_fresh, payload FROM ttl_i1 ORDER BY payload;

DROP TABLE ttl_i1;

SELECT '--- I2';

-- I2: a not-yet-expired GROUP BY ... SET on the sort key must NOT trigger the whole-part sort-key repair
-- when only an unrelated GROUP BY TTL actually fires. TTL1 GROUP BY (toStartOfDay(ts), k) SET payload
-- fires now (does not touch the sort key); TTL2 GROUP BY toStartOfDay(ts) SET ts (touches the sort key)
-- expires 40 years out and does NOT fire. The repair is gated on the FIRING TTLs' SET targets, so no
-- needless re-sort runs; result must be one group per (day, k) with the summed payload. STOP/START TTL
-- MERGES + OPTIMIZE forces the TTL onto the merge path (MergeTask), which is where the gate lives.
CREATE TABLE ttl_i2 (ts DateTime, k UInt32, payload UInt64)
ENGINE = MergeTree ORDER BY (toStartOfDay(ts), k)
TTL ts + toIntervalDay(1) GROUP BY toStartOfDay(ts), k SET payload = sum(payload),
    ts + toIntervalYear(40) GROUP BY toStartOfDay(ts) SET ts = max(ts)
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP TTL MERGES ttl_i2;
INSERT INTO ttl_i2 VALUES ('2020-01-01 00:00:00', 1, 10);
INSERT INTO ttl_i2 VALUES ('2020-01-01 01:00:00', 1, 20), ('2020-01-02 00:00:00', 2, 5);
SYSTEM START TTL MERGES ttl_i2;
OPTIMIZE TABLE ttl_i2 FINAL;

SELECT toStartOfDay(ts) AS day, k, payload FROM ttl_i2 ORDER BY day, k;

DROP TABLE ttl_i2;

SELECT '--- I3';

-- I3: refreshing a MATERIALIZED group_by key must recompute ONLY the subcolumns feeding that key,
-- not every re-extractable subcolumn in the stream. ORDER BY (d, t.a) pre-extracts t.a; d MATERIALIZED
-- toDate(ts). TTL1 GROUP BY (d, t.a) SET ts fires (rewrites ts); TTL2 GROUP BY d has its key d refreshed.
-- The refresh recomputes d from the post-SET ts but must leave the unrelated pass-through t.a in the
-- stream, otherwise the later TTL throws NOT_FOUND_COLUMN_IN_BLOCK (t.a). Result: one group per (d, t.a).
CREATE TABLE ttl_i3 (ts DateTime, t Tuple(a UInt32, b UInt32), d Date MATERIALIZED toDate(ts), payload UInt64)
ENGINE = MergeTree ORDER BY (d, t.a)
TTL ts + toIntervalDay(1) GROUP BY d, t.a SET ts = max(ts) + toIntervalYear(50),
    ts + toIntervalDay(2) GROUP BY d SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 1;

INSERT INTO ttl_i3 (ts, t, payload) VALUES ('2020-01-01 00:00:00', (1, 1), 10), ('2020-01-01 00:00:00', (2, 2), 20), ('2020-01-01 00:00:00', (1, 1), 5);
OPTIMIZE TABLE ttl_i3 FINAL;

SELECT toYear(d) AS post_set_year, t.a, payload FROM ttl_i3 ORDER BY t.a;

DROP TABLE ttl_i3;

SELECT '--- J1';

-- J1 (round 8): the stale-subcolumn drop must be subcolumn-granular, not keyed on the physical parent.
-- d MATERIALIZED toDate(t.b), ORDER BY (d, t.a). An earlier GROUP BY d SET rewrites the whole tuple t.
-- Recomputing d needs the (now stale) t.b re-extracted from post-SET t, but the sibling pass-through
-- sort-key subcolumn t.a (same parent t) is NOT read by the recompute and must be kept -- dropping it
-- by parent name would make the later TTLAggregationAlgorithm throw NOT_FOUND_COLUMN_IN_BLOCK (t.a).
-- Result: d stays consistent with toDate(t.b) for every row and no column is lost.
CREATE TABLE ttl_j1 (ts DateTime, src UInt32, t Tuple(a UInt32, b DateTime), d Date MATERIALIZED toDate(t.b), payload UInt64)
ENGINE = MergeTree ORDER BY (d, t.a)
TTL ts + toIntervalDay(1) GROUP BY d SET t = (anyLast(src), toDateTime('2070-01-01') + toIntervalDay(anyLast(src))),
    ts + toIntervalDay(2) GROUP BY d SET payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4;

INSERT INTO ttl_j1 SELECT toDateTime('2020-01-01 00:00:00'), number, (number % 3, toDateTime('2020-01-01 00:00:00') + number * 40 * 86400), number FROM numbers(40);
OPTIMIZE TABLE ttl_j1 FINAL;

SELECT count() AS rows, countIf(d = toDate(t.b)) = count() AS all_d_fresh FROM ttl_j1;

DROP TABLE ttl_j1;

SELECT '--- J2';

-- J2 (round 8): a later GROUP BY TTL's precomputed "won't fire" min is invalid once an EARLIER firing
-- GROUP BY ... SET rewrites a column the later TTL's expiry expression reads. d MATERIALIZED toDate(ts),
-- ORDER BY d. TTL1 (ts1 old, fires) GROUP BY d SET ts2 = min(ts2) - 20y moves TTL2's expiry base from
-- future to past; TTL2 (ts2 + 1d) GROUP BY d SET ts = max(ts) + 50y then actually fires and rewrites ts.
-- The merge repair gate must include TTL2's SET target (via the chained-firing expansion), recompute the
-- MATERIALIZED sort-key d from post-SET ts, and re-sort -- otherwise d is written stale. A plain OPTIMIZE
-- (not FINAL) keeps the merge non-forced so the precomputed min actually gates. Result: d matches toDate(ts).
CREATE TABLE ttl_j2 (ts1 DateTime, ts2 DateTime, ts DateTime, d Date MATERIALIZED toDate(ts), payload UInt64)
ENGINE = MergeTree ORDER BY d
TTL ts1 + toIntervalDay(1) GROUP BY d SET ts2 = min(ts2) - toIntervalYear(20),
    ts2 + toIntervalDay(1) GROUP BY d SET ts = max(ts) + toIntervalYear(50)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4;

-- Two parts (merges only when calculated at insert -> non-forced merge) so a plain OPTIMIZE merges
-- them; STOP/START MERGES keeps the two inserts separate until the merge.
SYSTEM STOP MERGES ttl_j2;
INSERT INTO ttl_j2 (ts1, ts2, ts, payload) SELECT toDateTime('2020-01-01 00:00:00'), toDateTime('2040-01-01 00:00:00'), toDateTime('2040-01-01 00:00:00'), 1 FROM numbers(20);
INSERT INTO ttl_j2 (ts1, ts2, ts, payload) SELECT toDateTime('2020-01-01 00:00:00'), toDateTime('2040-01-01 00:00:00'), toDateTime('2040-01-01 00:00:00'), 1 FROM numbers(20);
SYSTEM START MERGES ttl_j2;
OPTIMIZE TABLE ttl_j2;

-- Assert only the staleness invariant, not the aggregated row count: how far a non-forced merge
-- aggregates the part varies with merge scheduling / randomized settings, but the MATERIALIZED sort
-- key d must ALWAYS stay consistent with toDate(ts) after the chained SET. The bug writes d stale
-- (d != toDate(ts)) for the rows TTL2 rewrote; the fix keeps it 0 on every merge path.
SELECT countIf(d != toDate(ts)) AS stale_d FROM ttl_j2;

DROP TABLE ttl_j2;

SELECT '--- K1';

-- K1 (round 9, merge-side): a later TTL whose expiry reads a MATERIALIZED column derived from an
-- earlier SET target must be included in the firing set so its own SET target's dependent MATERIALIZED
-- columns are recomputed. d2 MATERIALIZED toDate(ts2), d MATERIALIZED toDate(ts), ORDER BY d2.
-- TTL1 (ts1 old, fires) GROUP BY d2 SET ts2 = min(ts2) - 20y; TTL2 expires on d2 (= toDate(ts2), moved
-- to the past by TTL1) GROUP BY d2 SET ts = max(ts) + 50y. getFiringGroupByTTLSetTargets only saw TTL2's
-- DIRECT expiry column d2 (not d2 -> toDate(ts2) -> ts2), so ts was left out of firing_set_targets and
-- the merge repair never recomputed d = toDate(ts): d written stale. The fix expands the expiry through
-- the materialized dependency graph. Result: d stays consistent with toDate(ts).
CREATE TABLE ttl_k1 (ts1 DateTime, ts2 DateTime, d2 Date MATERIALIZED toDate(ts2), ts DateTime, d Date MATERIALIZED toDate(ts), payload UInt64)
ENGINE = MergeTree ORDER BY d2
TTL ts1 + toIntervalDay(1) GROUP BY d2 SET ts2 = min(ts2) - toIntervalYear(20),
    d2 + toIntervalDay(1) GROUP BY d2 SET ts = max(ts) + toIntervalYear(50)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4;
INSERT INTO ttl_k1 (ts1, ts2, ts, payload) SELECT toDateTime('2020-01-01 00:00:00'), toDateTime('2040-01-01 00:00:00'), toDateTime('2040-01-01 00:00:00'), 1 FROM numbers(8);
OPTIMIZE TABLE ttl_k1 FINAL;
SELECT countIf(d != toDate(ts)) AS stale_d, countIf(d2 != toDate(ts2)) AS stale_d2 FROM ttl_k1;

DROP TABLE ttl_k1;

SELECT '--- K2';

-- K2 (round 9, in-transform): a later TTL whose expiry reads a MATERIALIZED column derived from an
-- earlier SET target must have that column refreshed before its algorithm runs, otherwise it evaluates
-- expiry on the stale in-stream value and skips aggregation. d MATERIALIZED toDate(ts2), ORDER BY k.
-- TTL1 (ts1 old, fires) GROUP BY k SET ts2 = min(ts2) - 20y moves d from the future into the past;
-- TTL2 (d + 1d) GROUP BY k SET payload = toYear(max(d)) then must fire and read the POST-SET d.
-- The bug reads the stale in-stream d (still 2040, future) so TTL2 either skips (background merge) or
-- aggregates the pre-SET d = 2040 (forced merge). The fix rebuilds d from the post-SET ts2 first, so
-- max(d) reflects the post-SET year (< 2040) and d stays consistent with toDate(ts2). The exact post-SET
-- year depends on how many times the forced merge re-applies the SET; assert only that TTL2 saw a fresh
-- (non-2040) d and that no stored d is stale.
CREATE TABLE ttl_k2 (k UInt32, ts1 DateTime, ts2 DateTime, d Date MATERIALIZED toDate(ts2), payload UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts1 + toIntervalDay(1) GROUP BY k SET ts2 = min(ts2) - toIntervalYear(20),
    d + toIntervalDay(1) GROUP BY k SET payload = toUInt64(toYear(max(d)))
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4;
INSERT INTO ttl_k2 (k, ts1, ts2, payload) SELECT 1, toDateTime('2020-01-01 00:00:00'), toDateTime('2040-01-01 00:00:00'), 10 FROM numbers(8);
OPTIMIZE TABLE ttl_k2 FINAL;
-- TTL2's SET reads d (payload = toYear(max(d))). The bug reads a stale in-stream d that no longer
-- matches the value finally written, so payload diverges from the stored toYear(d). The fix refreshes
-- d before TTL2 runs, so the year TTL2 aggregated equals the year of the stored d. Assert that equality
-- (payload_matches_stored_d = 1) and that no stored d is stale (stale_d = 0).
SELECT max(payload) = toYear(max(d)) AS payload_matches_stored_d, countIf(d != toDate(ts2)) AS stale_d FROM ttl_k2;

DROP TABLE ttl_k2;


SELECT '--- L1';

-- L1 (round 10, subcolumn expiry, in-transform): a later TTL that expires on a SUBCOLUMN of a SET
-- target must have that subcolumn re-extracted from the post-SET parent before its algorithm runs.
-- ORDER BY (k, tup.ts) pre-extracts tup.ts into the TTL stream. TTL1 (ts1 old, fires) GROUP BY k
-- SET tup = (min(tup.ts) - 20y, 9) rewrites the parent tup, moving tup.ts from the future (2040) into
-- the past; TTL2 (tup.ts + 1d) GROUP BY k SET payload = toYear(max(tup.ts)) must then fire on the
-- POST-SET tup.ts. The bug leaves the stale pre-extracted tup.ts (2040, future) in the block:
-- executeExpressionAndGetColumn prefers it (getColumnOrSubcolumnByName) over re-extracting from the
-- post-SET tup, so TTL2 evaluates expiry on 2040, is not expired, and never runs its SET. The post-TTL
-- resort still repairs the STORED tup.ts, so the bug shows only as payload diverging from toYear(the
-- stored tup.ts). The exact stored year depends on how many times the forced merge re-applies TTL1's
-- min()-20y; assert only that TTL2 saw the same value it finally stored (payload_matches_stored = 1).
-- sib is an unrelated pass-through subcolumn of the same parent that TTL2 does NOT read; it must
-- survive (dropping it by parent name would make TTLAggregationAlgorithm throw NOT_FOUND_COLUMN_IN_BLOCK).
CREATE TABLE ttl_l1 (k UInt32, ts1 DateTime, tup Tuple(ts DateTime, sib UInt32), payload UInt64)
ENGINE = MergeTree ORDER BY (k, tup.ts)
TTL ts1 + toIntervalDay(1) GROUP BY k SET tup = tuple(min(tup.ts) - toIntervalYear(20), 9),
    tup.ts + toIntervalDay(1) GROUP BY k SET payload = toUInt64(toYear(max(tup.ts)))
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4;
INSERT INTO ttl_l1 (k, ts1, tup, payload) SELECT 1, toDateTime('2020-01-01 00:00:00'), tuple(toDateTime('2040-01-01 00:00:00'), 7), 10 FROM numbers(8);
OPTIMIZE TABLE ttl_l1 FINAL;
SELECT max(payload) = toUInt64(toYear(max(tup.1))) AS payload_matches_stored, any(tup.2) AS sib_survived FROM ttl_l1;

DROP TABLE ttl_l1;

SELECT '--- M1';

-- M1 (round 11, later COLUMN TTL): the chain-fire / pre-refresh handling applied to later GROUP BY
-- TTLs must also cover a later COLUMN TTL. payload has a column TTL on ts2 + 1d; an earlier
-- TTL1 (ts1 old, fires) GROUP BY k SET ts2 = min(ts2) - 20y moves ts2 from the future into the past.
-- The bug: TTLColumnAlgorithm trusts its precomputed min (over the pre-SET future ts2), says
-- "won't fire", and skips the column, so payload keeps its value. The fix detects the earlier SET
-- can move ts2 into the past, bypasses the stale-min shortcut, and recomputes expiry per row on the
-- post-SET ts2 -> payload is reset to its default (0). ts2 = now + 1y (future pre-SET, so the column
-- TTL would not fire), post-SET ts2 - 20y = now - 19y (past, so it fires).
CREATE TABLE ttl_m1 (k UInt32, ts1 DateTime, ts2 DateTime, payload UInt64 DEFAULT 0 TTL ts2 + toIntervalDay(1))
ENGINE = MergeTree ORDER BY k
TTL ts1 + toIntervalDay(1) GROUP BY k SET ts2 = min(ts2) - toIntervalYear(20)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4;
INSERT INTO ttl_m1 (k, ts1, ts2, payload) SELECT 1, toDateTime('2020-01-01 00:00:00'), now() + toIntervalYear(1), 111 FROM numbers(8);
OPTIMIZE TABLE ttl_m1 FINAL;
SELECT max(payload) AS payload_after_column_ttl FROM ttl_m1;

DROP TABLE ttl_m1;

SELECT '--- M2';

-- M2 (round 11, later COLUMN TTL on a MATERIALIZED expiry input): same as M1 but the column TTL
-- expires on d MATERIALIZED toDate(ts2). Besides bypassing the stale min, the earlier SET's affected
-- MATERIALIZED column d must be refreshed from the post-SET ts2 before the column algorithm runs,
-- otherwise it reads the stale in-stream d (still future) and skips. The fix rebuilds d first, so the
-- column TTL fires and payload resets to default (0).
CREATE TABLE ttl_m2 (k UInt32, ts1 DateTime, ts2 DateTime, d Date MATERIALIZED toDate(ts2), payload UInt64 DEFAULT 0 TTL d + toIntervalDay(1))
ENGINE = MergeTree ORDER BY k
TTL ts1 + toIntervalDay(1) GROUP BY k SET ts2 = min(ts2) - toIntervalYear(20)
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4;
INSERT INTO ttl_m2 (k, ts1, ts2, payload) SELECT 1, toDateTime('2020-01-01 00:00:00'), now() + toIntervalYear(1), 111 FROM numbers(8);
OPTIMIZE TABLE ttl_m2 FINAL;
SELECT max(payload) AS payload_after_column_ttl FROM ttl_m2;

DROP TABLE ttl_m2;
