drop table if exists users sync;
drop table if exists messages sync;

create table users (id Int64, name String) engine=ReplicatedMergeTree('/clickhouse/{database}/tables/03277_users', 'r1') order by tuple();
create table messages (id Int64, user_id Int64, text String) engine=ReplicatedMergeTree('/clickhouse/{database}/tables/03277_messages', 'r1') order by tuple();

insert into users select number, concat('user_', toString(number)) from numbers(10);
insert into messages select 100+number, number, concat('message_', toString(number)) from numbers(10);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_1_shard_3_replicas_1_unavailable';

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
ANY INNER JOIN users ON messages.user_id = users.id
ORDER BY
    user_id ASC,
    c ASC;

drop table users sync;
drop table messages sync;
