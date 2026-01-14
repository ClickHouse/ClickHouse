SET enable_analyzer = 1;

DROP TABLE IF EXISTS tbl, distributed_table, mergetable_dist;

CREATE TABLE tbl
(
    `id` Int64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO `tbl` (id) VALUES (1),(2);

CREATE TABLE distributed_table AS tbl ENGINE=Distributed(test_shard_localhost, currentDatabase(), 'tbl');

CREATE TABLE mergetable_dist AS tbl ENGINE=Merge(currentDatabase(), 'distributed_table');

SELECT id, _database, _table FROM mergetable_dist LEFT JOIN (SELECT 1 AS id) t USING id;

DROP TABLE IF EXISTS tbl, distributed_table, mergetable_dist;
