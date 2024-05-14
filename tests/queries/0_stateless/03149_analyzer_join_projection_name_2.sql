-- Tags: no-fasttest

CREATE TABLE users (uid Int16, name String, gid LowCardinality(String), gname LowCardinality(String))
  ENGINE=MergeTree order by tuple();
CREATE TABLE groups (gid LowCardinality(String), gname LowCardinality(String)) 
  ENGINE=MergeTree order by tuple();

CREATE TABLE target (uid Int16, name String, gid LowCardinality(String), gname LowCardinality(String)) 
  ENGINE=MergeTree order by tuple();

CREATE DICTIONARY groups_dict (
    gid String, gname String
)
PRIMARY KEY gid, gname
LAYOUT(COMPLEX_KEY_HASHED())
SOURCE(CLICKHOUSE(
    DB 'default'
    QUERY 'select * from groups'))
LIFETIME(MIN 0 MAX 0);

CREATE MATERIALIZED VIEW mv to target AS
SELECT u.uid, u.name, u.gid, u.gname
FROM users u left join groups_dict g using gid, gname;

INSERT INTO groups VALUES ('1', 'Group1');

INSERT INTO users VALUES (1231, 'John', '1', 'Group1');
INSERT INTO users VALUES (6666, 'Ksenia', '1', 'Group1');
INSERT INTO users VALUES (8888, 'Alice', '1', 'Group1');
INSERT INTO users VALUES (1234, 'Test', '2', 'Group1');

SELECT * FROM target ORDER BY uid format PrettyCompactMonoBlock;
