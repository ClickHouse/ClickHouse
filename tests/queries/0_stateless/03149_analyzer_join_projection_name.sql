DROP DATABASE IF EXISTS db_for_dict_03149;
CREATE DATABASE db_for_dict_03149;

CREATE TABLE db_for_dict_03149.users (uid Int16, name String, gid LowCardinality(String), gname LowCardinality(String))
  ENGINE=MergeTree order by tuple();
CREATE TABLE db_for_dict_03149.groups (gid LowCardinality(String), gname LowCardinality(String))
  ENGINE=MergeTree order by tuple();

CREATE DICTIONARY db_for_dict_03149.groups_dict (
    gid String, gname String
)
PRIMARY KEY gid, gname
LAYOUT(COMPLEX_KEY_HASHED())
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() QUERY 'select * from db_for_dict_03149.groups'))
LIFETIME(MIN 0 MAX 0);


INSERT INTO db_for_dict_03149.groups VALUES ('1', 'Group1');

INSERT INTO db_for_dict_03149.users VALUES (1231, 'John', '1', 'Group1');

select 'analyzer=1, join with dictionary';

SELECT u.uid, u.name, u.gid, u.gname
FROM db_for_dict_03149.users u left join db_for_dict_03149.groups_dict g using gid, gname
format PrettyCompactMonoBlock;

select '';
select 'analyzer=1, join with table';

SELECT u.uid, u.name, u.gid, u.gname
FROM db_for_dict_03149.users u left join db_for_dict_03149.groups g using gid, gname
format PrettyCompactMonoBlock;


set allow_experimental_analyzer=0;

select '';
select 'analyzer=0, join with dictionary';

SELECT u.uid, u.name, u.gid, u.gname
FROM db_for_dict_03149.users u left join db_for_dict_03149.groups_dict g using gid, gname
format PrettyCompactMonoBlock;

select '';
select 'analyzer=0, join with table';
SELECT u.uid, u.name, u.gid, u.gname
FROM db_for_dict_03149.users u left join db_for_dict_03149.groups g using gid, gname
format PrettyCompactMonoBlock;

DROP DATABASE IF EXISTS db_for_dict_03149;
