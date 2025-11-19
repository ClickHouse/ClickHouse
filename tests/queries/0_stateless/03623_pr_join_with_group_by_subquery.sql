drop table if exists users sync;
drop table if exists messages sync;

create table users (id Int64, name String) engine=ReplicatedMergeTree('/clickhouse/{database}/tables/03623_users', 'r1') order by tuple();
create table messages (id Int64, user_id Int64, text String) engine=ReplicatedMergeTree('/clickhouse/{database}/tables/03623_messages', 'r1') order by tuple();

insert into users select number, concat('user_', toString(number)) from numbers(10);
insert into users select 11, concat('user_', toString(11));
insert into messages select 100+number, number, concat('message_', toString(number)) from numbers(11);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT '-- subquery INNER JOIN table';
SELECT
    name,
    c
FROM
(
    SELECT
        user_id,
        count() AS c
    FROM messages
    GROUP BY user_id
) AS messages
INNER JOIN users ON messages.user_id = users.id
ORDER BY
    user_id ASC,
    c ASC;

SELECT '-- table INNER JOIN subquery';
SELECT
    name,
    c
FROM
users
INNER JOIN
(
    SELECT
        user_id,
        count() AS c
    FROM messages
    GROUP BY user_id
) AS messages
ON messages.user_id = users.id
ORDER BY
    user_id ASC,
    c ASC;

SELECT '-- subquery LEFT JOIN table';
SELECT
    name,
    c
FROM
(
    SELECT
        user_id,
        count() AS c
    FROM messages
    GROUP BY user_id
) AS messages
LEFT JOIN users ON messages.user_id = users.id
ORDER BY
    user_id ASC,
    c ASC;

SELECT '-- table LEFT JOIN subquery';
SELECT
    name,
    c
FROM
users
LEFT JOIN
(
    SELECT
        user_id,
        count() AS c
    FROM messages
    GROUP BY user_id
) AS messages
ON messages.user_id = users.id
ORDER BY
    user_id ASC,
    c ASC;

SELECT '-- subquery RIGHT JOIN table';
SELECT
    name,
    c
FROM
(
    SELECT
        user_id,
        count() AS c
    FROM messages
    GROUP BY user_id
) AS messages
RIGHT JOIN users ON messages.user_id = users.id
ORDER BY
    user_id ASC,
    c ASC;

SELECT '-- table RIGHT JOIN subquery';
SELECT
    name,
    c
FROM
users
RIGHT JOIN
(
    SELECT
        user_id,
        count() AS c
    FROM messages
    GROUP BY user_id
) AS messages
ON messages.user_id = users.id
ORDER BY
    user_id ASC,
    c ASC;

drop table users sync;
drop table messages sync;
