-- Tags: zookeeper
-- Mutations whose WHERE cannot be analyzed for partition pruning in isolation
-- (virtual columns, subqueries) must still work when the pruning optimization is enabled.

SET mutations_sync = 2;
SET optimize_mutations_with_partition_pruning = 1;

DROP TABLE IF EXISTS t_mutation_pruning_exotic;

CREATE TABLE t_mutation_pruning_exotic (d Date, x UInt32, y UInt32)
ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x;

INSERT INTO t_mutation_pruning_exotic VALUES ('2024-01-01', 1, 100);
INSERT INTO t_mutation_pruning_exotic VALUES ('2024-02-01', 2, 200);

SELECT 'virtual column in predicate';
ALTER TABLE t_mutation_pruning_exotic DELETE WHERE _partition_id = '202401';
SELECT * FROM t_mutation_pruning_exotic ORDER BY d;

ALTER TABLE t_mutation_pruning_exotic UPDATE y = 10 WHERE _part LIKE '202402%';
SELECT * FROM t_mutation_pruning_exotic ORDER BY d;

SELECT 'subquery in predicate';
ALTER TABLE t_mutation_pruning_exotic UPDATE y = y + 1 WHERE x IN (SELECT number + 2 FROM numbers(1));
SELECT * FROM t_mutation_pruning_exotic ORDER BY d;

DROP TABLE t_mutation_pruning_exotic;

SELECT 'replicated, virtual column in predicate';

DROP TABLE IF EXISTS t_mutation_pruning_exotic_r;

CREATE TABLE t_mutation_pruning_exotic_r (d Date, x UInt32, y UInt32, a UInt32 ALIAS y + 1, m UInt32 MATERIALIZED y * 2)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutation_pruning_exotic_r', 'r1')
PARTITION BY toYYYYMM(d) ORDER BY x;

INSERT INTO t_mutation_pruning_exotic_r VALUES ('2024-01-01', 1, 100);
INSERT INTO t_mutation_pruning_exotic_r VALUES ('2024-02-01', 2, 200);

ALTER TABLE t_mutation_pruning_exotic_r DELETE WHERE _partition_id = '202401';
SELECT * FROM t_mutation_pruning_exotic_r ORDER BY d;

SELECT 'replicated, subquery in predicate';
SET allow_nondeterministic_mutations = 1;
ALTER TABLE t_mutation_pruning_exotic_r UPDATE y = y + 1 WHERE x IN (SELECT number + 2 FROM numbers(1));
SELECT * FROM t_mutation_pruning_exotic_r ORDER BY d;

-- The `ALIAS` column case lives in `04697_mutation_pruning_alias_column_predicate`, because it
-- cannot work with the old analyzer at all.
SELECT 'replicated, MATERIALIZED column in predicate';
ALTER TABLE t_mutation_pruning_exotic_r UPDATE y = y + 1 WHERE m = 402;
SELECT * FROM t_mutation_pruning_exotic_r ORDER BY d;

-- The mutation predicate is stored as text and re-parsed, which resets the set operation nodes to
-- their un-normalized form. The pruning analysis has to normalize them exactly like the mutation
-- execution path, otherwise these mutations fail with "UNION mode UNION_DEFAULT must be normalized".
SELECT 'replicated, set operations in predicate';
ALTER TABLE t_mutation_pruning_exotic_r UPDATE y = y + 1 WHERE x IN ((SELECT 2) UNION DISTINCT (SELECT 3));
SELECT * FROM t_mutation_pruning_exotic_r ORDER BY d;

ALTER TABLE t_mutation_pruning_exotic_r UPDATE y = y + 1 WHERE x IN ((SELECT 2) EXCEPT (SELECT 3));
SELECT * FROM t_mutation_pruning_exotic_r ORDER BY d;

ALTER TABLE t_mutation_pruning_exotic_r UPDATE y = y + 1 WHERE x IN ((SELECT 2) INTERSECT (SELECT 2));
SELECT * FROM t_mutation_pruning_exotic_r ORDER BY d;

-- The same, but over the partition key expression, so the pruner does look at the predicate.
ALTER TABLE t_mutation_pruning_exotic_r DELETE WHERE toYYYYMM(d) IN ((SELECT 202401) UNION DISTINCT (SELECT 202403));
SELECT * FROM t_mutation_pruning_exotic_r ORDER BY d;

DROP TABLE t_mutation_pruning_exotic_r;
