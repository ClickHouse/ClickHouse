CREATE TABLE limit_by
(
    `id` Int,
    `val` Int
)
ENGINE = MergeTree
ORDER BY tuple();

insert into limit_by values(1, 100), (1, 110), (1, 120), (1, 130), (2, 200), (2, 210), (2, 220), (3, 300);

CREATE TABLE ties
(
    `a` Int
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO ties VALUES (1), (1), (2), (2), (2), (2) (3), (3);

set allow_experimental_parallel_reading_from_replicas=1, cluster_for_parallel_replicas='parallel_replicas', max_parallel_replicas=100, parallel_replicas_for_non_replicated_merge_tree=1;

set enable_analyzer=0;

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

set enable_analyzer=0;

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

