-- Continuation of `04511_ttl_multi_group_by_set_rewrites_key` (split out to keep each test below the
-- per-test time limit): unrecomputable `EPHEMERAL`-backed `MATERIALIZED` columns, cascading order loss
-- across several `GROUP BY` TTLs, and recomputation of `MATERIALIZED` / subcolumn sort-key inputs.

DROP TABLE IF EXISTS ttl_multi_group_by;

-- F2: a MATERIALIZED column that reads both an EPHEMERAL column and a SET target cannot be recomputed
-- (ephemeral columns are not on disk). This must not throw or reject the merge; the merge completes and
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

-- F3: a MATERIALIZED column may be defined over an ALIAS column. An ALIAS is computed on read and never
-- stored, so its name cannot be resolved against the columns a merge sees: the reference has to be
-- replaced by the expression it stands for before the default is analyzed, otherwise the merge throws
-- UNKNOWN_IDENTIFIER. The SET rewrites the column behind the alias, so `m` is recomputed through it:
-- x = max(x) = 9, a = x + 100 = 109, m = a + 1 = 110, payload = sum = 30.
CREATE TABLE ttl_multi_group_by (k UInt32, ts DateTime, x UInt32, a UInt32 ALIAS x + 100, m UInt32 MATERIALIZED a + 1, payload UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalDay(1) GROUP BY k SET x = max(x), payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_multi_group_by (k, ts, x, payload) VALUES (1, '2020-01-01', 1, 10), (1, '2020-01-02', 9, 20);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT k, x, a, m, payload FROM ttl_multi_group_by ORDER BY k;
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

-- H1b: a downstream MATERIALIZED column that reads BOTH an ephemeral-tainted column and a rewritten
-- regular source directly is recomputed from the columns that exist on disk, so it still equals its
-- own expression over them. Only the ephemeral hop (m1) stays stale; both are warned. Asserting the
-- expression identity rather than the literal value is what distinguishes this from a downstream
-- column left at its pre-SET value, which would satisfy neither side of the identity.
CREATE TABLE ttl_multi_group_by (k UInt32, ts DateTime, x UInt32, eph String EPHEMERAL 'E', m1 String MATERIALIZED concat(toString(x), eph), m2 String MATERIALIZED concat(m1, '-', toString(x)), payload UInt64)
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalDay(1) GROUP BY k SET x = max(x), payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_multi_group_by (k, ts, x, eph, payload) VALUES (1, '2020-01-01', 5, 'a', 10), (1, '2020-01-02', 9, 'b', 20);
OPTIMIZE TABLE ttl_multi_group_by FINAL;

SELECT k, x, payload, (m2 = concat(m1, '-', toString(x))) AS m2_matches_stored_inputs FROM ttl_multi_group_by ORDER BY k;
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
