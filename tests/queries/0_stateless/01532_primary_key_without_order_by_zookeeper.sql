-- Tags: zookeeper, no-parallel

DROP TABLE IF EXISTS merge_tree_pk;

CREATE TABLE merge_tree_pk
(
    key UInt64,
    value String
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY key;

SHOW CREATE TABLE merge_tree_pk;

INSERT INTO merge_tree_pk VALUES (1, 'a');
INSERT INTO merge_tree_pk VALUES (2, 'b');

SELECT * FROM merge_tree_pk ORDER BY key, value;

INSERT INTO merge_tree_pk VALUES (1, 'c');

DETACH TABLE merge_tree_pk;
ATTACH TABLE merge_tree_pk;

SELECT * FROM merge_tree_pk FINAL ORDER BY key, value;

DROP TABLE IF EXISTS merge_tree_pk;

DROP TABLE IF EXISTS merge_tree_pk_sql;

CREATE TABLE merge_tree_pk_sql
(
    key UInt64,
    value String,
    PRIMARY KEY (key)
)
ENGINE = ReplacingMergeTree();

SHOW CREATE TABLE merge_tree_pk_sql;

INSERT INTO merge_tree_pk_sql VALUES (1, 'a');
INSERT INTO merge_tree_pk_sql VALUES (2, 'b');

SELECT * FROM merge_tree_pk_sql ORDER BY key, value;

INSERT INTO merge_tree_pk_sql VALUES (1, 'c');

DETACH TABLE merge_tree_pk_sql;
ATTACH TABLE merge_tree_pk_sql;

SELECT * FROM merge_tree_pk_sql FINAL ORDER BY key, value;

ALTER TABLE merge_tree_pk_sql ADD COLUMN key2 UInt64, MODIFY ORDER BY (key, key2);

INSERT INTO merge_tree_pk_sql VALUES (2, 'd', 555);

INSERT INTO merge_tree_pk_sql VALUES (2, 'e', 555);

SELECT * FROM merge_tree_pk_sql FINAL ORDER BY key, value;

SHOW CREATE TABLE merge_tree_pk_sql;

DROP TABLE IF EXISTS merge_tree_pk_sql;

DROP TABLE IF EXISTS replicated_merge_tree_pk_sql;

CREATE TABLE replicated_merge_tree_pk_sql
(
    key UInt64,
    value String,
    PRIMARY KEY (key)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/01532_primary_key_without', 'r1');

SHOW CREATE TABLE replicated_merge_tree_pk_sql;

INSERT INTO replicated_merge_tree_pk_sql VALUES (1, 'a');
INSERT INTO replicated_merge_tree_pk_sql VALUES (2, 'b');

SELECT * FROM replicated_merge_tree_pk_sql ORDER BY key, value;

INSERT INTO replicated_merge_tree_pk_sql VALUES (1, 'c');

DETACH TABLE replicated_merge_tree_pk_sql;
ATTACH TABLE replicated_merge_tree_pk_sql;

SELECT * FROM replicated_merge_tree_pk_sql FINAL ORDER BY key, value;

ALTER TABLE replicated_merge_tree_pk_sql ADD COLUMN key2 UInt64, MODIFY ORDER BY (key, key2);

INSERT INTO replicated_merge_tree_pk_sql VALUES (2, 'd', 555);

INSERT INTO replicated_merge_tree_pk_sql VALUES (2, 'e', 555);

SELECT * FROM replicated_merge_tree_pk_sql FINAL ORDER BY key, value;

DETACH TABLE replicated_merge_tree_pk_sql;
ATTACH TABLE replicated_merge_tree_pk_sql;

SHOW CREATE TABLE replicated_merge_tree_pk_sql;

DROP TABLE IF EXISTS replicated_merge_tree_pk_sql;
