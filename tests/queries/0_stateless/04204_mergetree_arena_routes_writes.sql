-- Tags: use_jemalloc
-- use_jemalloc: the test asserts on `jemalloc.mergetree_arena.active_bytes`, which is only
--               registered when the build has jemalloc.

-- Smoke test that part- and table-level allocations actually land in the dedicated MergeTree
-- arena. We exercise both code paths (CREATE TABLE, then INSERT) and assert the arena holds
-- a non-trivial number of bytes afterwards.
--
-- Why we don't measure a *delta* across the test (`after - before`) the way one might expect:
-- `jemalloc.mergetree_arena.active_bytes` is a process-wide metric and the server is constantly
-- allocating into the arena from background activity (`system.metric_log` / `text_log` /
-- `query_log` flushes — those are MergeTree tables — plus their merges and outdated-part
-- cleanup). Even with `no-parallel`, those background writers run during the test and the net
-- delta over a few seconds is dominated by their churn, not by the test's own work. We tried
-- amplifying the signal (hundreds of CREATE TABLE / INSERT, wide schemas) and forcing
-- `SYSTEM STOP MERGES`; the tests still flaked under random settings. So we settle for a
-- coarser invariant: if routing is functional, the arena holds far more than 1 MiB after any
-- realistic workload (in practice it's tens to hundreds of MiB on any populated server). If
-- routing is broken (`getArenaIndex()` returning 0, or scope guards no-op'd), the arena
-- collapses to ~0 and this assertion catches it.

DROP TABLE IF EXISTS t_mergetree_arena_smoke SYNC;

CREATE TABLE t_mergetree_arena_smoke (a UInt64, b String, c LowCardinality(String))
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_mergetree_arena_smoke
SELECT number, toString(number), toString(number % 10) FROM numbers(1000);

SYSTEM RELOAD ASYNCHRONOUS METRICS;

WITH
    (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC') AS jemalloc_on,
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.mergetree_arena.active_bytes') AS arena
SELECT (NOT jemalloc_on) OR (arena > 1024 * 1024);

DROP TABLE t_mergetree_arena_smoke;
