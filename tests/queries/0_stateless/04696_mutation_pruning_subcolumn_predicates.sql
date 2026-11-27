-- Tags: zookeeper
-- A mutation predicate over a subcolumn (`Tuple`, `Nullable`, `JSON`) is accepted by the mutation
-- itself, so the partition pruning analysis must accept it too instead of failing with
-- `UNKNOWN_IDENTIFIER`.

SET mutations_sync = 2;
SET optimize_mutations_with_partition_pruning = 1;
SET enable_json_type = 1;

DROP TABLE IF EXISTS t_mutation_pruning_subcolumns;

CREATE TABLE t_mutation_pruning_subcolumns (d Date, x UInt32, y UInt32, t Tuple(a UInt32, b String), n Nullable(UInt32), j JSON)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutation_pruning_subcolumns', 'r1')
PARTITION BY toYYYYMM(d) ORDER BY x;

INSERT INTO t_mutation_pruning_subcolumns VALUES ('2024-01-01', 1, 100, (1, 'a'), NULL, '{"a": 1}');
INSERT INTO t_mutation_pruning_subcolumns VALUES ('2024-02-01', 2, 200, (2, 'b'), 2, '{"a": 2}');

SELECT 'tuple element';
ALTER TABLE t_mutation_pruning_subcolumns UPDATE y = y + 1 WHERE t.a = 1;
SELECT d, x, y FROM t_mutation_pruning_subcolumns ORDER BY d;

SELECT 'null subcolumn';
ALTER TABLE t_mutation_pruning_subcolumns UPDATE y = y + 1 WHERE n.null;
SELECT d, x, y FROM t_mutation_pruning_subcolumns ORDER BY d;

SELECT 'json subcolumn';
ALTER TABLE t_mutation_pruning_subcolumns UPDATE y = y + 1 WHERE j.a = 2;
SELECT d, x, y FROM t_mutation_pruning_subcolumns ORDER BY d;

SELECT 'subcolumn combined with the partition key';
ALTER TABLE t_mutation_pruning_subcolumns DELETE WHERE t.a = 1 AND toYYYYMM(d) = 202401;
SELECT d, x, y FROM t_mutation_pruning_subcolumns ORDER BY d;

DROP TABLE t_mutation_pruning_subcolumns;

-- A subcolumn can be the partition key itself, and then the pruner does use the predicate.
SELECT 'subcolumn as the partition key';

DROP TABLE IF EXISTS t_mutation_pruning_subcolumn_key;

CREATE TABLE t_mutation_pruning_subcolumn_key (d Date, x UInt32, y UInt32, t Tuple(a UInt32, b String))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutation_pruning_subcolumn_key', 'r1')
PARTITION BY t.a ORDER BY x;

INSERT INTO t_mutation_pruning_subcolumn_key VALUES ('2024-01-01', 1, 100, (1, 'a'));
INSERT INTO t_mutation_pruning_subcolumn_key VALUES ('2024-02-01', 2, 200, (2, 'b'));

ALTER TABLE t_mutation_pruning_subcolumn_key UPDATE y = y + 1 WHERE t.a = 1;
SELECT d, x, y FROM t_mutation_pruning_subcolumn_key ORDER BY d;

DROP TABLE t_mutation_pruning_subcolumn_key;
