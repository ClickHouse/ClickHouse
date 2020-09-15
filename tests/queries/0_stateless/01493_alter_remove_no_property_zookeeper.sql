DROP TABLE IF EXISTS no_prop_table;

CREATE TABLE no_prop_table
(
    some_column UInt64
)
ENGINE MergeTree()
ORDER BY tuple();

SHOW CREATE TABLE no_prop_table;

-- just nothing happened
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE DEFAULT;
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE MATERIALIZED;
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE ALIAS;
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE CODEC;
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE COMMENT;
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE TTL;

ALTER TABLE no_prop_table REMOVE TTL;

SHOW CREATE TABLE no_prop_table;

DROP TABLE IF EXISTS no_prop_table;

DROP TABLE IF EXISTS r_no_prop_table;

CREATE TABLE r_no_prop_table
(
  some_column UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/test/01493_r_no_prop_table', '1')
ORDER BY tuple();

SHOW CREATE TABLE r_no_prop_table;

ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE DEFAULT;
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE MATERIALIZED;
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE ALIAS;
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE CODEC;
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE COMMENT;
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE TTL;

ALTER TABLE r_no_prop_table REMOVE TTL;

SHOW CREATE TABLE r_no_prop_table;

ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE ttl;
ALTER TABLE r_no_prop_table remove TTL;

DROP TABLE IF EXISTS r_no_prop_table;
