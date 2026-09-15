-- Tags: no-random-merge-tree-settings
-- ^ The bug needs the TTL GROUP BY merge to actually run; pin MergeTree settings so the
--   tiny inputs are reliably merged into a single part.

-- MATERIALIZED-column half of the "TTL ... GROUP BY ... SET on a sorting key column" regression
-- test. Split out of 04327_ttl_group_by_set_sort_key_resort so that neither file runs into the
-- per-test time limit in the slower CI configurations (S3 storage with metadata in Keeper), where
-- the per-DDL round trips dominate. Nothing is dropped: this file holds the scenarios where the
-- sorting key is a MATERIALIZED column that must be recomputed after the SET.

-- Merge path, MATERIALIZED sort-key column whose SOURCE is rewritten by the SET. The sorting key
-- is the MATERIALIZED column `d = toDate(ts)`, and the SET rewrites its source `ts` (not `d`
-- directly). The aggregation updates `ts` but leaves the stored `d` on its pre-SET value, so `d`
-- must be recomputed from its default expression before re-sorting. The SET aggregate reverses the
-- day order, so a correct re-sort is observable. `optimize_sorting_by_input_stream_properties = 1`
-- is kept on to cover the optimizer path.
DROP TABLE IF EXISTS t_mat_key;
CREATE TABLE t_mat_key (ts DateTime, d Date MATERIALIZED toDate(ts))
ENGINE = MergeTree ORDER BY d
TTL ts + toIntervalDay(1) GROUP BY d SET ts = toDateTime('2100-01-01') - (max(ts) - toDateTime('2000-01-01'))
SETTINGS min_bytes_for_full_part_storage = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_mat_key;
INSERT INTO t_mat_key SELECT toDateTime('2020-01-01') + number * 86400 FROM numbers(5);
INSERT INTO t_mat_key SELECT toDateTime('2020-02-01') + number * 86400 FROM numbers(5);
SYSTEM START MERGES t_mat_key;
OPTIMIZE TABLE t_mat_key FINAL SETTINGS optimize_sorting_by_input_stream_properties = 1;
-- Stored `d` must equal the recomputed `toDate(ts)` for every row (not the stale pre-SET value).
SELECT 'mat key consistent', countIf(d = toDate(ts)) = count() FROM t_mat_key;
-- Part must be physically sorted by the recomputed `d`.
SELECT 'mat key sorted', (SELECT groupArray(d) FROM (SELECT d FROM t_mat_key SETTINGS optimize_read_in_order = 0))
                       = (SELECT groupArray(d) FROM (SELECT d FROM t_mat_key ORDER BY d));
DROP TABLE t_mat_key;

-- Mutation path (MATERIALIZE TTL), MATERIALIZED sort-key column whose source is SET. Same shape as
-- above but the TTL is applied by a mutation instead of a merge.
DROP TABLE IF EXISTS t_mat_key_mut;
CREATE TABLE t_mat_key_mut (ts DateTime, d Date MATERIALIZED toDate(ts))
ENGINE = MergeTree ORDER BY d
TTL ts + toIntervalDay(1) GROUP BY d SET ts = toDateTime('2100-01-01') - (max(ts) - toDateTime('2000-01-01'))
SETTINGS min_bytes_for_full_part_storage = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP TTL MERGES t_mat_key_mut;
INSERT INTO t_mat_key_mut SELECT toDateTime('2020-01-01') + number * 86400 FROM numbers(10);
ALTER TABLE t_mat_key_mut MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'mat key mut consistent', countIf(d = toDate(ts)) = count() FROM t_mat_key_mut;
SELECT 'mat key mut sorted', (SELECT groupArray(d) FROM (SELECT d FROM t_mat_key_mut SETTINGS optimize_read_in_order = 0))
                           = (SELECT groupArray(d) FROM (SELECT d FROM t_mat_key_mut ORDER BY d));
DROP TABLE t_mat_key_mut;

-- Transitive MATERIALIZED chain: the sorting key is `z`, `z` is MATERIALIZED from `y`, and `y` is
-- MATERIALIZED from the base column `x` that the SET rewrites. A one-hop check misses `z`, so the
-- stored `z` would keep its pre-SET value and the part would be written and pruned by stale
-- sort-key data. The affected-column detection must take the transitive closure and recompute the
-- intermediate `y` before `z`.
DROP TABLE IF EXISTS t_mat_chain;
CREATE TABLE t_mat_chain (x DateTime, y Date MATERIALIZED toDate(x), z UInt32 MATERIALIZED toYYYYMM(y))
ENGINE = MergeTree ORDER BY z
TTL x + toIntervalDay(1) GROUP BY z SET x = max(x) + interval 100 year
SETTINGS min_bytes_for_full_part_storage = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP TTL MERGES t_mat_chain;
INSERT INTO t_mat_chain (x) SELECT toDateTime('2020-01-01') + number * 86400 * 40 FROM numbers(10);
ALTER TABLE t_mat_chain MATERIALIZE TTL SETTINGS mutations_sync = 2;
-- Stored `z` must equal the recomputed `toYYYYMM(toDate(x))` for every row.
SELECT 'mat chain consistent', countIf(z = toYYYYMM(toDate(x))) = count() FROM t_mat_chain;
SELECT 'mat chain sorted', (SELECT groupArray(z) FROM (SELECT z FROM t_mat_chain SETTINGS optimize_read_in_order = 0))
                        = (SELECT groupArray(z) FROM (SELECT z FROM t_mat_chain ORDER BY z));
DROP TABLE t_mat_chain;

-- MATERIALIZED chain through a Tuple subcolumn. `z` reads `y.d`, where `y` is itself a
-- MATERIALIZED Tuple rebuilt after the SET rewrites `x`. The default-expression DAG must rebuild
-- `y` before extracting its subcolumn for `z`; otherwise `z` can be derived from a stale or
-- default `y.d`. The physical part must still be ordered by the freshly recomputed `z`.
DROP TABLE IF EXISTS t_mat_tuple_chain;
CREATE TABLE t_mat_tuple_chain
(
    x DateTime,
    y Tuple(d Date) MATERIALIZED tuple(toDate(x)),
    z UInt32 MATERIALIZED toYYYYMM(y.d)
)
ENGINE = MergeTree ORDER BY z
TTL x + toIntervalDay(1) GROUP BY z
    SET x = max(x) + interval 100 years
SETTINGS min_bytes_for_full_part_storage = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_mat_tuple_chain;
-- The old analyzer resolves `y.d` against the insert block, which holds only `x`, so the insert
-- needs the new one. The merge below still runs on whichever analyzer the job selects.
INSERT INTO t_mat_tuple_chain (x) SELECT toDateTime('2020-01-01') + number * 86400 * 40 FROM numbers(10) SETTINGS enable_analyzer = 1;
SYSTEM START MERGES t_mat_tuple_chain;
OPTIMIZE TABLE t_mat_tuple_chain FINAL;
SELECT 'mat tuple chain consistent', countIf(z = toYYYYMM(y.d) AND y.d = toDate(x)) = count() FROM t_mat_tuple_chain;
SELECT 'mat tuple chain sorted', (SELECT groupArray(z) FROM (SELECT z FROM t_mat_tuple_chain SETTINGS optimize_read_in_order = 0))
                              = (SELECT groupArray(z) FROM (SELECT z FROM t_mat_tuple_chain ORDER BY z));
DROP TABLE t_mat_tuple_chain;

-- MATERIALIZED sort-key column defined over a Tuple SUBCOLUMN source (`d = toDate(tup.ts)`,
-- `ORDER BY d`), with the SET rewriting the whole `tup` via an aggregate over an unrelated plain
-- column. Recomputing `d` needs the subcolumn `tup.ts`, which is not directly present in the
-- stream (only the physical `tup` is), so the recompute DAG must prepend a subcolumn-extraction
-- step. Before the fix this failed with NOT_FOUND_COLUMN_IN_BLOCK.
DROP TABLE IF EXISTS t_mat_subcol;
CREATE TABLE t_mat_subcol (tup Tuple(ts DateTime), v UInt32, d Date MATERIALIZED toDate(tup.ts))
ENGINE = MergeTree ORDER BY d
TTL tup.ts + toIntervalDay(1) GROUP BY d SET tup = tuple(toDateTime('2200-01-01') + toIntervalSecond(max(v)))
SETTINGS min_bytes_for_full_part_storage = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP TTL MERGES t_mat_subcol;
INSERT INTO t_mat_subcol (tup, v) SELECT tuple(toDateTime('2020-01-01') + number * 86400), number FROM numbers(10);
ALTER TABLE t_mat_subcol MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'mat subcol consistent', countIf(d = toDate(tup.ts)) = count() FROM t_mat_subcol;
SELECT 'mat subcol sorted', (SELECT groupArray(d) FROM (SELECT d FROM t_mat_subcol SETTINGS optimize_read_in_order = 0))
                         = (SELECT groupArray(d) FROM (SELECT d FROM t_mat_subcol ORDER BY d));
DROP TABLE t_mat_subcol;

-- MATERIALIZED sort-key column defined over an EPHEMERAL source (`sk MATERIALIZED reverse(eph)`,
-- `ORDER BY sk`), with the SET rewriting a NON-sort-key column. The SET does not touch the sort
-- key, so the resort gate must simply return false. Collecting materialized-source columns must
-- include ephemeral columns in the analysis set (as the UPDATE mutation path does) so the analysis
-- resolves `eph`, then skip the ephemeral-sourced materialized column instead of throwing. Before
-- the fix this raised UNKNOWN_IDENTIFIER ('Missing columns: eph while processing reverse(eph)').
DROP TABLE IF EXISTS t_eph_mat;
CREATE TABLE t_eph_mat (eph String EPHEMERAL, sk String MATERIALIZED reverse(eph), ts DateTime, v UInt32)
ENGINE = MergeTree ORDER BY sk
TTL ts + toIntervalDay(1) GROUP BY sk SET v = max(v)
SETTINGS min_bytes_for_full_part_storage = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP TTL MERGES t_eph_mat;
INSERT INTO t_eph_mat (eph, ts, v) VALUES ('abc', '2020-01-01 00:00:00', 1), ('xyz', '2020-01-01 00:00:00', 2), ('abc', '2020-01-01 00:00:00', 5);
SYSTEM START TTL MERGES t_eph_mat;
OPTIMIZE TABLE t_eph_mat FINAL;
-- Two groups: 'cba' (v = max(1,5) = 5) and 'zyx' (v = 2). No throw; part sorted by sk.
SELECT 'eph mat data', sk, v FROM t_eph_mat ORDER BY sk;
SELECT 'eph mat sorted', (SELECT groupArray(sk) FROM (SELECT sk FROM t_eph_mat SETTINGS optimize_read_in_order = 0))
                       = (SELECT groupArray(sk) FROM (SELECT sk FROM t_eph_mat ORDER BY sk));
DROP TABLE t_eph_mat;
