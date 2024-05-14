-- Tags: no-fasttest

CREATE TABLE users (uid Int16, name String, gid LowCardinality(String), gname LowCardinality(String))
  ENGINE=MergeTree order by tuple();
CREATE TABLE groups (gid LowCardinality(String), gname LowCardinality(String)) 
  ENGINE=MergeTree order by tuple();

CREATE DICTIONARY groups_dict (
    gid String, gname String
)
PRIMARY KEY gid, gname
LAYOUT(COMPLEX_KEY_HASHED())
SOURCE(CLICKHOUSE(QUERY 'select * from groups'))
LIFETIME(MIN 0 MAX 0);


INSERT INTO groups VALUES ('1', 'Group1');

INSERT INTO users VALUES (1231, 'John', '1', 'Group1');

select 'analyzer=1, join with dictionary';

SELECT u.uid, u.name, u.gid, u.gname
FROM users u left join groups_dict g using gid, gname
format PrettyCompactMonoBlock;

select '';
select 'analyzer=1, join with table';

SELECT u.uid, u.name, u.gid, u.gname
FROM users u left join groups g using gid, gname
format PrettyCompactMonoBlock;


set allow_experimental_analyzer=0;

select '';
select 'analyzer=0, join with dictionary';

SELECT u.uid, u.name, u.gid, u.gname
FROM users u left join groups_dict g using gid, gname
format PrettyCompactMonoBlock;

select '';
select 'analyzer=0, join with table';
SELECT u.uid, u.name, u.gid, u.gname
FROM users u left join groups g using gid, gname
format PrettyCompactMonoBlock;
