-- Correctness of the fast `MODIFY TTL` optimization. The fast path only shifts each part's stored TTL
-- timestamps by a constant, so it must:
--   * handle every TTL result type (Date, DateTime, Date32, DateTime64) without a logical error;
--   * fall back to a full rewrite when the delta is not a provably constant number of seconds
--     (calendar month/year intervals, day/week intervals in a DST time zone, column-dependent
--     expressions);
--   * fall back when the rows TTL is not the only TTL in the table (e.g. a column TTL), because the
--     parts' aggregate TTL bounds cover all TTLs and must not be shifted;
--   * fall back for parts whose stored TTL info is stale (left by materialize_ttl_after_modify = 0),
--     never deleting rows that are not actually expired;
--   * never delete rows under materialize_ttl_recalculate_only, which promises a metadata-only refresh.
-- In every case the resulting row set must be correct.

SET alter_sync = 2;
SET allow_suspicious_ttl_expressions = 1;
SET enable_fast_modify_ttl = 1;

SELECT 'Date32 TTL shortened so every row expires';
DROP TABLE IF EXISTS t_ttl_date32;
CREATE TABLE t_ttl_date32 (id UInt32, d Date32) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_date32 SELECT number, today() - 100 FROM numbers(1000);
SELECT count() FROM t_ttl_date32;
ALTER TABLE t_ttl_date32 MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_date32;
DROP TABLE t_ttl_date32;

SELECT 'DateTime64 TTL shortened so every row expires';
DROP TABLE IF EXISTS t_ttl_dt64;
CREATE TABLE t_ttl_dt64 (id UInt32, d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_dt64 SELECT number, now64(3, 'UTC') - INTERVAL 100 DAY FROM numbers(1000);
ALTER TABLE t_ttl_dt64 MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_dt64;
DROP TABLE t_ttl_dt64;

SELECT 'MODIFY TTL whose result type becomes DateTime64 must not throw (was a logical error)';
DROP TABLE IF EXISTS t_ttl_ns;
CREATE TABLE t_ttl_ns (id UInt32, d DateTime) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_ns SELECT number, now() - INTERVAL 500 DAY FROM numbers(10);
ALTER TABLE t_ttl_ns MODIFY TTL d + INTERVAL 1 NANOSECOND;
SELECT count() FROM t_ttl_ns;
DROP TABLE t_ttl_ns;

SELECT 'Calendar month interval falls back and still expires the right rows';
DROP TABLE IF EXISTS t_ttl_month;
CREATE TABLE t_ttl_month (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 12 MONTH;
INSERT INTO t_ttl_month SELECT number, now('UTC') - INTERVAL 11 MONTH FROM numbers(1000);
ALTER TABLE t_ttl_month MODIFY TTL d + INTERVAL 10 MONTH;
SELECT count() FROM t_ttl_month;
DROP TABLE t_ttl_month;

SELECT 'Column-dependent TTL falls back and still expires the right rows';
DROP TABLE IF EXISTS t_ttl_if;
-- Start with a TTL that keeps every row (so nothing is dropped at INSERT time), then shorten it only
-- for even ids. A constant per-part shift would wrongly drop every row here; the fallback keeps the odd
-- ids. `id` is a non-date column, so the fast path is not eligible and a full rewrite is used.
CREATE TABLE t_ttl_if (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id
    TTL d + toIntervalDay(if(id % 2 = 0, 1000, 2000));
INSERT INTO t_ttl_if SELECT number, now('UTC') FROM numbers(1000);
ALTER TABLE t_ttl_if MODIFY TTL d + toIntervalDay(if(id % 2 = 0, -1000, 2000));
SELECT count() FROM t_ttl_if;
DROP TABLE t_ttl_if;

SELECT 'Day interval in a DST time zone falls back and still expires the right rows';
DROP TABLE IF EXISTS t_ttl_dst;
CREATE TABLE t_ttl_dst (id UInt32, d DateTime('Europe/Madrid')) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_dst SELECT number, now('Europe/Madrid') - INTERVAL 100 DAY FROM numbers(1000);
ALTER TABLE t_ttl_dst MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_dst;
DROP TABLE t_ttl_dst;

SELECT 'Column TTL in the table forces fallback and still expires the right rows';
DROP TABLE IF EXISTS t_ttl_col;
CREATE TABLE t_ttl_col (id UInt32, d DateTime('UTC'), x String TTL d + INTERVAL 1000 DAY) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_col SELECT number, now('UTC') - INTERVAL 100 DAY, 'x' FROM numbers(1000);
ALTER TABLE t_ttl_col MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_col;
DROP TABLE t_ttl_col;

SELECT 'Batched ALTER with another TTL change falls back and materializes the column TTL';
DROP TABLE IF EXISTS t_ttl_batch;
-- The `MODIFY TTL` alone would be eligible for the fast path (constant 100-day shift), but the same
-- ALTER also sets a column TTL. The fast path only shifts the stored rows-TTL timestamps and would
-- never materialize the new column TTL, so the batch must fall back to the full rewrite, which
-- clears the expired `x` values.
CREATE TABLE t_ttl_batch (id UInt32, d DateTime('UTC'), x String) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_batch SELECT number, now('UTC') - INTERVAL 100 DAY, 'x' FROM numbers(1000);
ALTER TABLE t_ttl_batch MODIFY COLUMN x String TTL d + INTERVAL 10 DAY, MODIFY TTL d + INTERVAL 200 DAY;
SELECT count(), countIf(x = '') FROM t_ttl_batch;
DROP TABLE t_ttl_batch;

SELECT 'Batched ALTER adding a column with a TTL falls back to the regular rewrite';
DROP TABLE IF EXISTS t_ttl_batch2;
-- `ADD COLUMN` is not a TTL alter by itself, so the `MODIFY TTL` command is the one that triggers
-- materialization here; it must still notice the column TTL introduced by the sibling command and
-- use the regular `MATERIALIZE TTL` (no delta) instead of the fast form.
CREATE TABLE t_ttl_batch2 (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_batch2 SELECT number, now('UTC') - INTERVAL 100 DAY FROM numbers(1000);
ALTER TABLE t_ttl_batch2 ADD COLUMN x String TTL d + INTERVAL 10 DAY, MODIFY TTL d + INTERVAL 200 DAY;
SELECT count() FROM t_ttl_batch2;
SELECT countIf(command LIKE '%MATERIALIZE TTL %') FROM system.mutations WHERE database = currentDatabase() AND table = 't_ttl_batch2';
DROP TABLE t_ttl_batch2;

SELECT 'Batched ALTER adding the column the new TTL references falls back instead of throwing';
DROP TABLE IF EXISTS t_ttl_batch3;
-- The new TTL references a column introduced by a sibling `ADD COLUMN` of the same ALTER. The fast
-- path proves its delta against the original metadata, where `d2` does not exist, so it must not be
-- attempted at all (it used to throw UNKNOWN_IDENTIFIER instead of falling back).
CREATE TABLE t_ttl_batch3 (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_batch3 SELECT number, now('UTC') - INTERVAL 100 DAY FROM numbers(1000);
ALTER TABLE t_ttl_batch3 ADD COLUMN d2 DateTime('UTC') DEFAULT d, MODIFY TTL d2 + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_batch3;
SELECT countIf(command LIKE '%MATERIALIZE TTL %') FROM system.mutations WHERE database = currentDatabase() AND table = 't_ttl_batch3';
DROP TABLE t_ttl_batch3;

SELECT 'Batched ALTER changing the default of the TTL column falls back to the regular rewrite';
DROP TABLE IF EXISTS t_ttl_batch4;
-- A sibling metadata-only `MODIFY COLUMN` changes the DEFAULT of the column the TTL reads. For parts
-- where the column is derived rather than stored that changes the historical values the TTL sees, so
-- the delta proven from the TTL ASTs alone would be unsound; any sibling command disables the fast path.
CREATE TABLE t_ttl_batch4 (id UInt32, d DateTime('UTC'), d2 DateTime('UTC') DEFAULT d) ENGINE = MergeTree ORDER BY id TTL d2 + INTERVAL 300 DAY;
INSERT INTO t_ttl_batch4 (id, d) SELECT number, now('UTC') - INTERVAL 100 DAY FROM numbers(1000);
ALTER TABLE t_ttl_batch4 MODIFY COLUMN d2 DateTime('UTC') DEFAULT d + INTERVAL 1 DAY, MODIFY TTL d2 + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_batch4;
SELECT countIf(command LIKE '%MATERIALIZE TTL %') FROM system.mutations WHERE database = currentDatabase() AND table = 't_ttl_batch4';
DROP TABLE t_ttl_batch4;

SELECT 'Part lagging a standalone metadata-only DEFAULT change of the TTL column falls back';
DROP TABLE IF EXISTS t_ttl_lag_default;
-- `materialize_ttl_recalculate_only` keeps every `MATERIALIZE TTL` a metadata-only recalculation, so
-- the part never gets rewritten and never stores `d2` physically: it is synthesized from the DEFAULT
-- on every read, and its stored TTL bounds were computed under whatever DEFAULT was current then.
-- `min_bytes_for_wide_part` is pinned so that the part stays compact: a wide part takes the
-- hardlink-and-recalculate path, which does write `d2` out, and then the part is no longer lagging.
CREATE TABLE t_ttl_lag_default (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id
    SETTINGS materialize_ttl_recalculate_only = 1, min_bytes_for_wide_part = 1000000000;
INSERT INTO t_ttl_lag_default SELECT number, now('UTC') - INTERVAL 1000 DAY FROM numbers(1000);
ALTER TABLE t_ttl_lag_default ADD COLUMN d2 DateTime('UTC') DEFAULT d + INTERVAL 900 DAY;
-- Recalculates the part's TTL bounds under the old DEFAULT: d + 900 + 200 days = now + 100 days.
ALTER TABLE t_ttl_lag_default MODIFY TTL d2 + INTERVAL 200 DAY;
SELECT count() FROM t_ttl_lag_default;
-- Standalone metadata-only DEFAULT change: `d2` now reads as d + 2000 days, but the part's stored
-- TTL bounds still reflect the old DEFAULT, under the very same surface TTL expression.
ALTER TABLE t_ttl_lag_default MODIFY COLUMN d2 DateTime('UTC') DEFAULT d + INTERVAL 2000 DAY;
-- The TTL change (200 -> 50 days) is a provable constant -150 day shift of the same expression, but
-- applying it to the stored bounds would declare the part expired (now + 100 - 150 days), so the next
-- merge would drop every row, while the true expiry is d + 2000 + 50 = now + 1050 days. The fast path
-- must reject a part that does not physically store the TTL source column and fall back to the regular
-- recalculation. The ALTER itself is still recorded in the delta form (that decision is table-wide), so
-- the fallback is asserted on the part's resulting TTL bounds: recalculated from the current DEFAULT
-- they are far in the future, whereas a blind shift of the stored bounds puts them in the past.
ALTER TABLE t_ttl_lag_default MODIFY TTL d2 + INTERVAL 50 DAY;
SELECT count() FROM t_ttl_lag_default;
SELECT delete_ttl_info_max > now('UTC') + INTERVAL 1000 DAY FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_lag_default' AND active;
DROP TABLE t_ttl_lag_default;

SELECT 'Stale part TTL info (materialize_ttl_after_modify = 0) must not delete live rows';
DROP TABLE IF EXISTS t_ttl_stale;
CREATE TABLE t_ttl_stale (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY;
INSERT INTO t_ttl_stale SELECT number, now('UTC') FROM numbers(1000);
ALTER TABLE t_ttl_stale MODIFY TTL d + INTERVAL 300 DAY SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_ttl_stale MODIFY TTL d + INTERVAL 290 DAY;
SELECT count() FROM t_ttl_stale;
DROP TABLE t_ttl_stale;

SELECT 'materialize_ttl_recalculate_only must not delete rows, only refresh the TTL metadata';
DROP TABLE IF EXISTS t_ttl_recalc_only;
-- With `materialize_ttl_recalculate_only` the regular `MATERIALIZE TTL` never rewrites a part, so
-- expired rows stay until the next merge. The fast path must behave the same: it may shift the stored
-- TTL bounds (the mutation is still recorded in the delta form below), but it must not replace a fully
-- expired part with an empty one.
CREATE TABLE t_ttl_recalc_only (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id
    TTL d + INTERVAL 300 DAY SETTINGS materialize_ttl_recalculate_only = 1;
-- The refreshed metadata marks the part as fully expired, so a background TTL merge is free to drop
-- its rows right after the mutation - that is the regular behaviour and it would race with the check
-- below. Only mutations are of interest here, and `SYSTEM STOP TTL MERGES` does not block them.
SYSTEM STOP TTL MERGES t_ttl_recalc_only;
INSERT INTO t_ttl_recalc_only SELECT number, now('UTC') - INTERVAL 100 DAY FROM numbers(1000);
ALTER TABLE t_ttl_recalc_only MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_recalc_only;
SELECT countIf(command LIKE '%MATERIALIZE TTL %') FROM system.mutations WHERE database = currentDatabase() AND table = 't_ttl_recalc_only';
DROP TABLE t_ttl_recalc_only;
