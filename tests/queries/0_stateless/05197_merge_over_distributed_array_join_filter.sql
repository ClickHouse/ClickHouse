-- `ARRAY JOIN` with a filter on the joined column over a `Merge` table whose child is a `Distributed`
-- table. `StorageMerge` builds the child query by replacing its join tree with the child table, which
-- rewires the columns of the removed `ARRAY JOIN` onto that table, so a filter over the array-joined
-- column used to look like a filter over the child's own columns and the child was asked to read
-- `__array_join_exp_1`. Only a `Distributed` child noticed, because it re-analyzes the child query.
-- The array join and the filter are performed on the initiator either way.

DROP TABLE IF EXISTS t_05197;
DROP TABLE IF EXISTS t_05197_dist;
DROP TABLE IF EXISTS t_05197_merge;
DROP TABLE IF EXISTS t_05197_merge_local;

CREATE TABLE t_05197 (id UInt16, data Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05197 SELECT number, if(number % 2, ['aaa'], ['bbb', 'ccc']) FROM numbers(200);

CREATE TABLE t_05197_dist AS t_05197 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05197);
CREATE TABLE t_05197_merge AS t_05197 ENGINE = Merge(currentDatabase(), '^t_05197_dist$');
CREATE TABLE t_05197_merge_local AS t_05197 ENGINE = Merge(currentDatabase(), '^t_05197$');

SELECT 'the local table', count() FROM t_05197 ARRAY JOIN data WHERE data IN ('aaa');
SELECT 'the Distributed table', count() FROM t_05197_dist ARRAY JOIN data WHERE data IN ('aaa');
SELECT 'Merge over the local table', count() FROM t_05197_merge_local ARRAY JOIN data WHERE data IN ('aaa');

SELECT 'Merge over Distributed, no filter', count() FROM t_05197_merge ARRAY JOIN data;
SELECT 'Merge over Distributed', count() FROM t_05197_merge ARRAY JOIN data WHERE data IN ('aaa');
SELECT 'an equality filter', count() FROM t_05197_merge ARRAY JOIN data WHERE data = 'ccc';
SELECT 'an aliased array join', count() FROM t_05197_merge ARRAY JOIN data AS d WHERE d = 'aaa';
SELECT 'LEFT ARRAY JOIN', count() FROM t_05197_merge LEFT ARRAY JOIN data WHERE data = 'aaa';

-- A filter on the table's own column is still a filter on the child table, and the two kinds combine.
SELECT 'a filter on a table column', count() FROM t_05197_merge ARRAY JOIN data WHERE id < 100;
SELECT 'both kinds of filter', count() FROM t_05197_merge ARRAY JOIN data WHERE id < 100 AND data = 'aaa';
SELECT 'both kinds, reversed', count() FROM t_05197_merge ARRAY JOIN data WHERE data = 'aaa' AND id < 100;

-- The array-joined values themselves, to show the filter selects the right rows.
SELECT 'the values', id, data FROM t_05197_merge ARRAY JOIN data WHERE data = 'ccc' AND id < 5 ORDER BY id;

DROP TABLE t_05197_merge_local;
DROP TABLE t_05197_merge;
DROP TABLE t_05197_dist;
DROP TABLE t_05197;
