-- Tags: zookeeper, no-old-analyzer
-- An `ALIAS` column is not physically present, so the pruning analysis has to know it anyway:
-- it is not part of the partition key, so the predicate is simply opaque to the pruner and every
-- partition is mutated.
-- The old analyzer cannot resolve an `ALIAS` column in a mutation predicate at all (it fails with
-- `Missing columns` regardless of the pruning optimization), and a mutation is executed in the
-- background with the server settings, so a query level `enable_analyzer` does not help.

SET mutations_sync = 2;
SET optimize_mutations_with_partition_pruning = 1;

DROP TABLE IF EXISTS t_mutation_pruning_alias;

CREATE TABLE t_mutation_pruning_alias (d Date, x UInt32, y UInt32, a UInt32 ALIAS y + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutation_pruning_alias', 'r1')
PARTITION BY toYYYYMM(d) ORDER BY x;

INSERT INTO t_mutation_pruning_alias VALUES ('2024-01-01', 1, 100);
INSERT INTO t_mutation_pruning_alias VALUES ('2024-02-01', 2, 200);

SELECT 'replicated, ALIAS column in predicate';
ALTER TABLE t_mutation_pruning_alias UPDATE y = y + 1 WHERE a = 201;
SELECT * FROM t_mutation_pruning_alias ORDER BY d;

DROP TABLE t_mutation_pruning_alias;
