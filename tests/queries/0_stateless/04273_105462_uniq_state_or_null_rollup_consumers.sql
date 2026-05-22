-- Additional regression coverage for https://github.com/ClickHouse/ClickHouse/issues/105462
--
-- The original regression test (04262_105462_uniq_state_or_null_with_rollup)
-- only exercises the `serialize`-driven final-projection path of the
-- rollup-produced `-OrNull` / `-OrDefault` state. The bug from #99839 (enabling
-- `*StateOrNull` / `*StateOrDefault` over `Nullable` arguments) reaches the same
-- corrupted `UniquesHashSet` through several other consumer paths that don't
-- touch the `serialize` path at all -- they all rely on the
-- `AggregateFunctionOrFill::insertResultIntoImpl` fix keeping the result
-- column's `src` pointer set so the source state is not double-destroyed.

DROP TABLE IF EXISTS source_105462;
DROP TABLE IF EXISTS agg_t_105462;
DROP TABLE IF EXISTS source_mem_105462;

CREATE TABLE source_105462 (h Nullable(UInt16)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO source_105462 SELECT if(number % 5 = 0, NULL, toUInt16(number)) FROM numbers(20);

-- 1) `<func>Merge` over the rollup-produced state column.
-- Reaches `UniquesHashSet::merge` via `AggregateFunctionMerge::add` rather than
-- `UniquesHashSet::write`. Without the `OrFill` fix this crashes (segfault on
-- `rhs.buf[i]` dereference in debug) and is `use-of-uninitialized-value` under
-- `MemorySanitizer`.
SELECT 'uniqMerge over rollup', uniqMerge(s)
FROM (SELECT uniqStateOrNull(h) AS s FROM source_105462 GROUP BY h WITH ROLLUP);

-- 1b) Same path for `uniqStateOrDefault` -- #99839 enabled both combinators.
SELECT 'uniqMerge over rollup, OrDefault', uniqMerge(s)
FROM (SELECT uniqStateOrDefault(h) AS s FROM source_105462 GROUP BY h WITH ROLLUP);

-- 2) Persist the rollup-produced state into `AggregatingMergeTree`.
-- Reaches `UniquesHashSet::merge` via `ColumnAggregateFunction::insertMergeFrom`
-- -> `AggregatingSortedAlgorithm::AggregatingMergedData::addRow` ->
-- `MergeTreeDataWriter::mergeBlock` -- a part-merge path that persists the
-- (formerly uninitialised) state to disk.
CREATE TABLE agg_t_105462 (k UInt8, s AggregateFunction(uniq, UInt16))
    ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO agg_t_105462
    SELECT toUInt8(coalesce(h, 999)) AS k, uniqStateOrNull(h)
    FROM source_105462 GROUP BY h WITH ROLLUP;
-- `uniqMerge` over the persisted states is settings-deterministic (commutative
-- and associative under any row split), unlike `count()` which depends on how
-- the INSERT pipeline parallelises the rollup output.
SELECT 'aggregating merge tree insert', uniqMerge(s) FROM agg_t_105462;

-- 3+4) Two-level + multi-thread aggregator variants. Same buggy state reached
-- through different scheduling, used as a `SETTINGS` matrix in one query each.
SELECT 'two-level single-thread', count() FROM (
    SELECT hex(uniqStateOrNull(h)) FROM source_105462 GROUP BY h WITH ROLLUP
    SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, max_threads = 1);

SELECT 'multi-threaded merge', count() FROM (
    SELECT hex(uniqStateOrNull(h)) FROM source_105462 GROUP BY h WITH ROLLUP
    SETTINGS max_threads = 4);

-- 5) `Memory` engine source: confirms the regression is pipeline-level rather
-- than `MergeTree`-specific.
CREATE TABLE source_mem_105462 (h Nullable(UInt16)) ENGINE = Memory;
INSERT INTO source_mem_105462 SELECT if(number % 5 = 0, NULL, toUInt16(number)) FROM numbers(20);
SELECT 'memory engine, uniqMerge over rollup', uniqMerge(s)
FROM (SELECT uniqStateOrNull(h) AS s FROM source_mem_105462 GROUP BY h WITH ROLLUP);

DROP TABLE source_105462;
DROP TABLE agg_t_105462;
DROP TABLE source_mem_105462;
