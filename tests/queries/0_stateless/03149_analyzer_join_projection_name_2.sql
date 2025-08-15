DROP DATABASE IF EXISTS db_for_dict_03149;
CREATE DATABASE db_for_dict_03149;

CREATE TABLE db_for_dict_03149.users (uid Int16, name String, gid LowCardinality(String), gname LowCardinality(String))
  ENGINE=MergeTree order by tuple();
CREATE TABLE db_for_dict_03149.groups (gid LowCardinality(String), gname LowCardinality(String))
  ENGINE=MergeTree order by tuple();

CREATE TABLE db_for_dict_03149.target (uid Int16, name String, gid LowCardinality(String), gname LowCardinality(String))
  ENGINE=MergeTree order by tuple();

CREATE DICTIONARY db_for_dict_03149.groups_dict (
    gid String, gname String
)
PRIMARY KEY gid, gname
LAYOUT(COMPLEX_KEY_HASHED())
SOURCE(CLICKHOUSE(QUERY 'select * from db_for_dict_03149.groups'))
LIFETIME(MIN 0 MAX 0);

CREATE MATERIALIZED VIEW db_for_dict_03149.mv to db_for_dict_03149.target AS
SELECT u.uid, u.name, u.gid, u.gname
FROM db_for_dict_03149.users u left join db_for_dict_03149.groups_dict g using gid, gname;

INSERT INTO db_for_dict_03149.groups VALUES ('1', 'Group1');

INSERT INTO db_for_dict_03149.users VALUES (1231, 'John', '1', 'Group1');
INSERT INTO db_for_dict_03149.users VALUES (6666, 'Ksenia', '1', 'Group1');
INSERT INTO db_for_dict_03149.users VALUES (8888, 'Alice', '1', 'Group1');
INSERT INTO db_for_dict_03149.users VALUES (1234, 'Test', '2', 'Group1');

SELECT * FROM db_for_dict_03149.target ORDER BY uid format PrettyCompactMonoBlock;

DROP DATABASE IF EXISTS db_for_dict_03149;
