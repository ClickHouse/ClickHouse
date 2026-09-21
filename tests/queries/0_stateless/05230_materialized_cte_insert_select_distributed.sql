-- The distributed `INSERT SELECT` fast paths probe the source table of the `SELECT` by name. A
-- reference to a `MATERIALIZED` CTE is not a table name, so those paths must not be taken: the rows
-- of the CTE must reach the target exactly once, whatever table carries the CTE's name.
-- https://github.com/ClickHouse/ClickHouse/issues/113711

SET enable_materialized_cte = 1;
SET parallel_distributed_insert_select = 2;
SET distributed_foreground_insert = 1;

DROP TABLE IF EXISTS ins_local_113711b, ins_dist_113711b, c_ins_113711b, c_ins_dist_113711b;

CREATE TABLE ins_local_113711b (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ins_dist_113711b (id UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), ins_local_113711b);
-- Tables carrying the CTE names, holding rows that must never reach the target.
CREATE TABLE c_ins_113711b (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO c_ins_113711b VALUES (100), (200);
CREATE TABLE c_ins_dist_113711b (id UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), c_ins_113711b);

SELECT '-- the CTE name belongs to a local table';
INSERT INTO ins_dist_113711b WITH c_ins_113711b AS MATERIALIZED (SELECT number AS id FROM numbers(3)) SELECT id FROM c_ins_113711b;
SELECT count(), sum(id) FROM ins_local_113711b;

SELECT '-- the CTE name belongs to a Distributed table';
TRUNCATE TABLE ins_local_113711b;
INSERT INTO ins_dist_113711b WITH c_ins_dist_113711b AS MATERIALIZED (SELECT number AS id FROM numbers(3)) SELECT id FROM c_ins_dist_113711b;
SELECT count(), sum(id) FROM ins_local_113711b;

SELECT '-- no table of the CTE name exists';
TRUNCATE TABLE ins_local_113711b;
INSERT INTO ins_dist_113711b WITH c_none_113711b AS MATERIALIZED (SELECT number AS id FROM numbers(3)) SELECT id FROM c_none_113711b;
SELECT count(), sum(id) FROM ins_local_113711b;

DROP TABLE c_ins_dist_113711b, c_ins_113711b, ins_dist_113711b, ins_local_113711b;
