-- Consumers of an aggregate state produced by `*StateOrNull` / `*StateOrDefault`
-- under `WITH ROLLUP` / `CUBE` / `TOTALS`: re-aggregation via `Merge`,
-- persistence into `AggregatingMergeTree`, two-level and multi-threaded
-- aggregator variants, and a `Memory`-engine source.

DROP TABLE IF EXISTS source_105462;
DROP TABLE IF EXISTS agg_t_105462;
DROP TABLE IF EXISTS source_mem_105462;

CREATE TABLE source_105462 (h Nullable(UInt16)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO source_105462 SELECT if(number % 5 = 0, NULL, toUInt16(number)) FROM numbers(20);

-- `Merge` consumes the rollup-produced state column, reaching
-- `UniquesHashSet::merge` rather than `UniquesHashSet::write`.
SELECT 'uniqMerge over rollup', uniqMerge(s)
FROM (SELECT uniqStateOrNull(h) AS s FROM source_105462 GROUP BY h WITH ROLLUP);

SELECT 'uniqMerge over rollup, OrDefault', uniqMerge(s)
FROM (SELECT uniqStateOrDefault(h) AS s FROM source_105462 GROUP BY h WITH ROLLUP);

-- Persist the rollup-produced state into `AggregatingMergeTree`, reaching
-- `UniquesHashSet::merge` via `ColumnAggregateFunction::insertMergeFrom` and the
-- background-merge / `MergeTreeDataWriter::mergeBlock` path.
-- `uniqMerge` over the persisted column is the deterministic check (commutative
-- and associative under any row split), unlike `count` which depends on how the
-- `INSERT` pipeline parallelises the rollup output.
CREATE TABLE agg_t_105462 (k UInt8, s AggregateFunction(uniq, UInt16))
    ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO agg_t_105462
    SELECT toUInt8(coalesce(h, 999)) AS k, uniqStateOrNull(h)
    FROM source_105462 GROUP BY h WITH ROLLUP;
SELECT 'aggregating merge tree insert', uniqMerge(s) FROM agg_t_105462;

-- Two-level and multi-threaded aggregator variants: same state shape reached
-- through different scheduling.
SELECT 'two-level single-thread', count() FROM (
    SELECT hex(uniqStateOrNull(h)) FROM source_105462 GROUP BY h WITH ROLLUP
    SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, max_threads = 1);

SELECT 'multi-threaded merge', count() FROM (
    SELECT hex(uniqStateOrNull(h)) FROM source_105462 GROUP BY h WITH ROLLUP
    SETTINGS max_threads = 4);

-- `Memory`-engine source: confirms the consumer paths above work independently
-- of `MergeTree`.
CREATE TABLE source_mem_105462 (h Nullable(UInt16)) ENGINE = Memory;
INSERT INTO source_mem_105462 SELECT if(number % 5 = 0, NULL, toUInt16(number)) FROM numbers(20);
SELECT 'memory engine, uniqMerge over rollup', uniqMerge(s)
FROM (SELECT uniqStateOrNull(h) AS s FROM source_mem_105462 GROUP BY h WITH ROLLUP);

DROP TABLE source_105462;
DROP TABLE agg_t_105462;
DROP TABLE source_mem_105462;
