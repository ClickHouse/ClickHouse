-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/84139
-- Fixed by https://github.com/ClickHouse/ClickHouse/pull/102304 and
-- https://github.com/ClickHouse/ClickHouse/pull/102471, which made `Merge`
-- always proxy virtual columns to the underlying storages, so `_database`
-- and `_table` are produced natively by the nested `Distributed` query.
--
-- Before the fix, with the analyzer enabled, selecting `_table` (and/or
-- `_database`) from a `Merge` over a `Distributed` table together with a
-- `LEFT JOIN` returned default (empty) values instead of the real ones,
-- and could also scramble other columns (e.g. `id` was returned as `0`).

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS distributed_table;
DROP TABLE IF EXISTS mergetable_dist;

CREATE TABLE tbl (id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO tbl (id) VALUES (1), (2);

CREATE TABLE distributed_table AS tbl
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'tbl');

CREATE TABLE mergetable_dist AS tbl
    ENGINE = Merge(currentDatabase(), 'distributed_table');

SELECT _table AS `_table`, *
FROM mergetable_dist
LEFT JOIN (SELECT 1 AS id) AS t USING (id)
ORDER BY id;

SELECT id, _database = currentDatabase(), _table
FROM mergetable_dist
LEFT JOIN (SELECT 1 AS id) AS t USING (id)
ORDER BY id;

SELECT id, m._database = currentDatabase(), m._table
FROM mergetable_dist AS m
LEFT JOIN (SELECT 1 AS id) AS t USING (id)
ORDER BY id;

DROP TABLE tbl;
DROP TABLE distributed_table;
DROP TABLE mergetable_dist;
