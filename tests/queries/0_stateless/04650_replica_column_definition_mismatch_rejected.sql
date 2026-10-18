-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: the test uses explicit ZooKeeper paths in the engine arguments.
-- Tag no-shared-merge-tree: same reason.

-- Column definitions are compared as ASTs rather than as formatted text. Genuinely different
-- definitions must still be rejected when a replica joins an existing table, and definitions that
-- differ only in formatting must still be accepted.

DROP TABLE IF EXISTS t_coldef_a;
DROP TABLE IF EXISTS t_coldef_b;

-- DEFAULT vs MATERIALIZED: same expression, different kind.
CREATE TABLE t_coldef_a (a UInt32, x UInt32 DEFAULT a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_default', 'r1') ORDER BY a;
CREATE TABLE t_coldef_b (a UInt32, x UInt32 MATERIALIZED a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_default', 'r2') ORDER BY a; -- { serverError INCOMPATIBLE_COLUMNS }
DROP TABLE t_coldef_a;

-- Different codec.
CREATE TABLE t_coldef_a (a UInt32, x UInt32 CODEC(ZSTD))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_codec', 'r1') ORDER BY a;
CREATE TABLE t_coldef_b (a UInt32, x UInt32 CODEC(LZ4))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_codec', 'r2') ORDER BY a; -- { serverError INCOMPATIBLE_COLUMNS }
DROP TABLE t_coldef_a;

-- Different column TTL.
CREATE TABLE t_coldef_a (a UInt32, d Date, x UInt32 TTL d + INTERVAL 1 DAY)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_ttl', 'r1') ORDER BY a;
CREATE TABLE t_coldef_b (a UInt32, d Date, x UInt32 TTL d + INTERVAL 2 DAY)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_ttl', 'r2') ORDER BY a; -- { serverError INCOMPATIBLE_COLUMNS }
DROP TABLE t_coldef_a;

-- Different explicit statistics.
SET allow_experimental_statistics = 1;
CREATE TABLE t_coldef_a (a UInt32, x UInt32 STATISTICS(tdigest))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_stats', 'r1') ORDER BY a;
CREATE TABLE t_coldef_b (a UInt32, x UInt32 STATISTICS(uniq))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_stats', 'r2') ORDER BY a; -- { serverError INCOMPATIBLE_COLUMNS }
DROP TABLE t_coldef_a;

-- Same statistics type, different parameters.
CREATE TABLE t_coldef_a (a UInt32, x UInt32 STATISTICS(tdigest(1)))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_stats_param', 'r1') ORDER BY a;
CREATE TABLE t_coldef_b (a UInt32, x UInt32 STATISTICS(tdigest(2)))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_stats_param', 'r2') ORDER BY a; -- { serverError INCOMPATIBLE_COLUMNS }
DROP TABLE t_coldef_a;

-- `SimpleAggregateFunction(sum, UInt64)` and a plain `UInt64` are different declared types, even
-- though `IDataType::equals` ignores the wrapper.
CREATE TABLE t_coldef_a (a UInt32, x SimpleAggregateFunction(sum, UInt64))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/{database}/t_coldef_saf', 'r1') ORDER BY a;
CREATE TABLE t_coldef_b (a UInt32, x UInt64)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/{database}/t_coldef_saf', 'r2') ORDER BY a; -- { serverError INCOMPATIBLE_COLUMNS }
DROP TABLE t_coldef_a;

-- The same definitions written with redundant parentheses are accepted.
CREATE TABLE t_coldef_a (a UInt32, d Date, x UInt32 DEFAULT (a) + 1 CODEC(ZSTD) TTL (d) + INTERVAL 1 DAY)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_equal', 'r1') ORDER BY a;
CREATE TABLE t_coldef_b (a UInt32, d Date, x UInt32 DEFAULT a + 1 CODEC(ZSTD) TTL d + INTERVAL 1 DAY)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_coldef_equal', 'r2') ORDER BY a;
SELECT 'equal definitions accepted';

DROP TABLE t_coldef_a;
DROP TABLE t_coldef_b;
