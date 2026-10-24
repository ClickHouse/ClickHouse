-- Tags: shard

-- The one-argument form of `merge` reads the tables of the current database of the query, also when the query is
-- sent to other servers, whose own default database is different.

CREATE TABLE t_implicit_db_1 (x UInt64) ENGINE = Memory;
CREATE TABLE t_implicit_db_2 (x UInt64) ENGINE = Memory;
INSERT INTO t_implicit_db_1 VALUES (2);
INSERT INTO t_implicit_db_2 VALUES (3);

SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM merge('^t_implicit_db_'));

WITH t AS (SELECT x FROM merge('^t_implicit_db_'))
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM t);

SELECT sum(m.x) FROM remote('127.0.0.2', numbers(4)) AS r INNER JOIN merge('^t_implicit_db_') AS m ON r.number = m.x;

-- The condition is inside an aggregate function, evaluated on two shards.
SELECT countIf(number IN (SELECT x FROM merge('^t_implicit_db_'))) FROM remote('127.0.0.{2,3}', numbers(4));

INSERT INTO FUNCTION file(currentDatabase() || '_05300.tsv', 'TSV', 'n UInt64') SELECT number FROM numbers(4)
    SETTINGS engine_file_truncate_on_insert = 1;
SELECT sum(n), count()
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05300.tsv', 'TSV', 'n UInt64')
WHERE n IN (SELECT x FROM merge('^t_implicit_db_'));
