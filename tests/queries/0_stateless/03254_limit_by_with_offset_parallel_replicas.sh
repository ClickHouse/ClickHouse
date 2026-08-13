#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS limit_by;
DROP TABLE IF EXISTS ties;
DROP TABLE IF EXISTS test_fetch;

CREATE TABLE limit_by
(
    id Int,
    val Int
)
ENGINE = MergeTree
ORDER BY tuple();

insert into limit_by values(1, 100), (1, 110), (1, 120), (1, 130), (2, 200), (2, 210), (2, 220), (3, 300);

CREATE TABLE ties
(
    a Int
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO ties VALUES (1), (1), (2), (2), (2), (2) (3), (3);

CREATE TABLE test_fetch(a Int32, b Int32) Engine = MergeTree ORDER BY ();

INSERT INTO test_fetch VALUES(1, 1), (2, 1), (3, 4), (3, 3), (5, 4), (0, 6), (5, 7);
"

for enable_analyzer in {0..1}; do
  for enable_parallel_replicas in {0..1}; do
    ${CLICKHOUSE_CLIENT} --query="
    set enable_analyzer=${enable_analyzer};
    set allow_experimental_parallel_reading_from_replicas=${enable_parallel_replicas}, cluster_for_parallel_replicas='parallel_replicas', max_parallel_replicas=100, parallel_replicas_for_non_replicated_merge_tree=1;

    select * from limit_by order by id, val limit 2 by id;
    select * from limit_by order by id, val limit 3 by id;
    select * from limit_by order by id, val limit 2, 2 by id;
    select * from limit_by order by id, val limit 2 offset 1 by id;
    select * from limit_by order by id, val limit 1, 2 by id limit 3;
    select * from limit_by order by id, val limit 1, 2 by id limit 3 offset 1;

    SELECT a FROM ties order by a limit 1 with ties;
    SELECT a FROM ties order by a limit 1, 2 with ties;
    SELECT a FROM ties order by a limit 2, 3 with ties;
    SELECT a FROM ties order by a limit 4 with ties;

    SELECT * FROM (SELECT * FROM test_fetch ORDER BY a, b OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY) ORDER BY a, b;
    SELECT * FROM (SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS WITH TIES) ORDER BY a, b;

    select * from remote('127.0.0.{1,2}', currentDatabase(), limit_by) order by id, val limit 2 by id;
    select * from remote('127.0.0.{1,2}', currentDatabase(), limit_by) order by id, val limit 3 by id;
    select * from remote('127.0.0.{1,2}', currentDatabase(), limit_by) order by id, val limit 2, 2 by id;
    select * from remote('127.0.0.{1,2}', currentDatabase(), limit_by) order by id, val limit 2 offset 1 by id;
    select * from remote('127.0.0.{1,2}', currentDatabase(), limit_by) order by id, val limit 1, 2 by id limit 3;
    select * from remote('127.0.0.{1,2}', currentDatabase(), limit_by) order by id, val limit 1, 2 by id limit 3 offset 1;

    SELECT a from remote('127.0.0.{1,2}', currentDatabase(), ties) order by a limit 1 with ties;
    SELECT a from remote('127.0.0.{1,2}', currentDatabase(), ties) order by a limit 1, 2 with ties;
    SELECT a from remote('127.0.0.{1,2}', currentDatabase(), ties) order by a limit 2, 3 with ties;
    SELECT a from remote('127.0.0.{1,2}', currentDatabase(), ties) order by a limit 4 with ties;

    SELECT '-----';
    "
  done
done

${CLICKHOUSE_CLIENT} --query="
DROP TABLE limit_by;
DROP TABLE ties;
DROP TABLE test_fetch;
"
