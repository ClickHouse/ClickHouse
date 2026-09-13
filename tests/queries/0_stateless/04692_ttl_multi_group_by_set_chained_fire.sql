-- Continuation of `04511_ttl_multi_group_by_set_rewrites_key` (split out to keep each test below the
-- per-test time limit): chained firing, where an earlier `GROUP BY ... SET` moves a later TTL's expiry
-- input from the future into the past, so the later `GROUP BY` / column TTL must fire on the post-`SET`
-- value and every `MATERIALIZED` column derived from it must be refreshed before it runs.

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


SELECT '--- K3';

-- K3: a later TTL can read an affected MATERIALIZED column from its SET expression even when its
-- own expiry does not. Refresh it before aggregation; otherwise `payload` is computed from the
-- pre-SET d while the post-TTL repair writes the new d.
CREATE TABLE ttl_k3 (k UInt32, ts DateTime, anchor DateTime, ts2 DateTime, d Date MATERIALIZED toDate(ts2) + toIntervalYear(5), payload UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalDay(1) GROUP BY k SET ts2 = min(anchor),
    ts + toIntervalDay(1) GROUP BY k SET payload = toUInt64(toYear(max(d)))
SETTINGS min_bytes_for_wide_part = 0, merge_max_block_size = 4;
INSERT INTO ttl_k3 (k, ts, anchor, ts2, payload) SELECT 1, toDateTime('2020-01-01 00:00:00'), toDateTime('2000-03-04 00:00:00'), toDateTime('2040-06-15 00:00:00'), 10 FROM numbers(8);
OPTIMIZE TABLE ttl_k3 FINAL;
SELECT max(payload) = toYear(max(d)) AS payload_matches_stored_d, countIf(d != toDate(ts2) + toIntervalYear(5)) AS stale_d FROM ttl_k3;

DROP TABLE ttl_k3;


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
