-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-random-merge-tree-settings
-- Tag no-shared-merge-tree, no-replicated-database: the test joins two explicit replicas of one
-- ReplicatedMergeTree table to compare what each of them has written to ZooKeeper.
-- Tag no-random-merge-tree-settings: the test shows the definition of a table, and the randomized
-- settings would be printed with it.

-- A replica compares its own definitions with the ones stored in ZooKeeper, which may have been
-- written by a server that did not remember the redundant parentheses the user wrote (they became
-- a part of the AST only in 26.5). Neither the comparison nor the stored form may depend on them.

DROP TABLE IF EXISTS t_parens_zk_r1 SYNC;
DROP TABLE IF EXISTS t_parens_zk_r2 SYNC;

CREATE TABLE t_parens_zk_r1 (x UInt64, d Date, y UInt64 DEFAULT x + 1,
    INDEX ix x * y TYPE minmax,
    PROJECTION p (SELECT x ORDER BY d),
    CONSTRAINT c CHECK x > 0)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parens_zk', 'r1')
PARTITION BY x + 1 ORDER BY (x, d) SAMPLE BY x TTL d + INTERVAL 10 YEAR;

-- The same table, written with redundant parentheses in every definition.
CREATE TABLE t_parens_zk_r2 (x UInt64, d Date, y UInt64 DEFAULT (x + 1),
    INDEX ix (x * y) TYPE minmax,
    PROJECTION p (SELECT (x) ORDER BY (d)),
    CONSTRAINT c CHECK (x > 0))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parens_zk', 'r2')
PARTITION BY (x + (1)) ORDER BY ((x), (d)) SAMPLE BY (x) TTL (d + INTERVAL 10 YEAR);

SELECT 'joined';

-- The other direction: the parenthesized definitions are the ones already in ZooKeeper.
DROP TABLE IF EXISTS t_parens_zk2_r1 SYNC;
DROP TABLE IF EXISTS t_parens_zk2_r2 SYNC;

CREATE TABLE t_parens_zk2_r1 (x UInt64, d Date, INDEX ix (x) TYPE minmax)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parens_zk2', 'r1')
ORDER BY (d) TTL (d + INTERVAL 10 YEAR);

CREATE TABLE t_parens_zk2_r2 (x UInt64, d Date, INDEX ix x TYPE minmax)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parens_zk2', 'r2')
ORDER BY d TTL d + INTERVAL 10 YEAR;

SELECT 'joined';

-- The stored definitions still show the parentheses exactly as they were written.
SHOW CREATE TABLE t_parens_zk2_r1;

-- A replica whose definition differs in more than the parentheses is still rejected.
CREATE TABLE t_parens_zk2_r3 (x UInt64, d Date, INDEX ix (x + 1) TYPE minmax)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parens_zk2', 'r3')
ORDER BY d TTL d + INTERVAL 10 YEAR; -- { serverError METADATA_MISMATCH }
