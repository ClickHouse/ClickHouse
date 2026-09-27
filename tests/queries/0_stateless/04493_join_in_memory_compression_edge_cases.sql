-- Regression coverage for two surfaces that read stored right-side blocks directly and must therefore
-- be decompression-aware when `enable_join_in_memory_compression` compressed them:
--   1. the extra (mixed) `JOIN ON` predicate that materializes a RHS column (`buildAdditionalFilter`);
--   2. `releaseJoinedBlocks`, used when a `hash` join spills to `GraceHashJoin`.
-- Each query compares the forced-compression result against the uncompressed one; they must be equal.

DROP TABLE IF EXISTS jimc_ec_left;
DROP TABLE IF EXISTS jimc_ec_right;

-- 1. Extra ON predicate reading a compressed right column.
-- `pad` is highly compressible, so under pressure it is stored as `ColumnCompressed`. The condition
-- must become a residual join filter (evaluated by `buildAdditionalFilter`, which materializes `r.pad`
-- straight from the stored block), not a post-join filter on already-materialized output. For that it
-- must be a real join condition that decides matching (so a LEFT JOIN, where a failing residual makes
-- the row non-matching) and reference both sides (so it cannot be pushed to the right-table scan). The
-- cross-side `r.pad != toString(l.lv)` satisfies both and is always true here, so the result equals the
-- uncompressed one. Without decompression in `buildAdditionalFilter` this raised
-- `ColumnCompressed must be decompressed before use`. `enable_analyzer = 1` is pinned because residual
-- `JOIN ON` conditions are only supported by the analyzer.

CREATE TABLE jimc_ec_left (k UInt64, lv UInt64) ENGINE = Memory;
CREATE TABLE jimc_ec_right (k UInt64, rv UInt64, pad String) ENGINE = Memory;

INSERT INTO jimc_ec_left SELECT number, number FROM numbers(40000);
INSERT INTO jimc_ec_right SELECT number, number, repeat('x', 1000) FROM numbers(40000);

SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_ec_left AS l LEFT JOIN jimc_ec_right AS r
            ON l.k = r.k AND r.pad != toString(l.lv)
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_ec_left AS l LEFT JOIN jimc_ec_right AS r
            ON l.k = r.k AND r.pad != toString(l.lv)
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1, max_bytes_in_join = 24000000, query_plan_join_swap_table = 'false')
SETTINGS enable_analyzer = 1;

DROP TABLE jimc_ec_left;
DROP TABLE jimc_ec_right;

-- 2. Forced compression followed by a spill that releases the compressed blocks.
-- The build side is only partially compressible: `inc` is (nearly) incompressible, so it keeps the
-- stored size above `max_bytes_before_external_join` even after `pad` is compressed. The in-memory
-- `hash` join compresses under `max_bytes_in_join` pressure (have_compressed = true) but stays below
-- that limit (no throw); then the `SpillingHashJoin` wrapper sees the compressed build still exceeds
-- the lower `max_bytes_before_external_join` and hands the partially compressed blocks to a
-- `GraceHashJoin` via `releaseJoinedBlocks` (and the grace buckets release again while rehashing).
-- Without decompression in `releaseJoinedBlocks` these reads raise
-- `ColumnCompressed must be decompressed before use`.

CREATE TABLE jimc_ec_left (k UInt64, lv UInt64) ENGINE = Memory;
CREATE TABLE jimc_ec_right (k UInt64, inc String, pad String) ENGINE = Memory;

INSERT INTO jimc_ec_left SELECT number, number FROM numbers(100000);
INSERT INTO jimc_ec_right SELECT number, randomString(160), repeat('x', 64) FROM numbers(100000);

SELECT (SELECT sum(cityHash64(l.k, l.lv, r.inc, r.pad)) FROM jimc_ec_left AS l INNER JOIN jimc_ec_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0,
                     max_bytes_in_join = 0, max_bytes_before_external_join = 0,
                     max_threads = 1, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.inc, r.pad)) FROM jimc_ec_left AS l INNER JOIN jimc_ec_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1,
                     max_bytes_in_join = 40000000, max_bytes_before_external_join = 12000000,
                     max_threads = 1, query_plan_join_swap_table = 'false');

DROP TABLE jimc_ec_left;
DROP TABLE jimc_ec_right;
