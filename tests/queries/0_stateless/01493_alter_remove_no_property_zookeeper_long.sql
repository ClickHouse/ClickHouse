-- Tags: long, zookeeper

DROP TABLE IF EXISTS no_prop_table;

CREATE TABLE no_prop_table
(
    some_column UInt64
)
ENGINE MergeTree()
ORDER BY tuple();

SHOW CREATE TABLE no_prop_table;

-- just nothing happened
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE DEFAULT; --{serverError BAD_ARGUMENTS}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE MATERIALIZED; --{serverError BAD_ARGUMENTS}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE ALIAS; --{serverError BAD_ARGUMENTS}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE CODEC; --{serverError BAD_ARGUMENTS}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE COMMENT; --{serverError BAD_ARGUMENTS}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE TTL; --{serverError BAD_ARGUMENTS}

ALTER TABLE no_prop_table REMOVE TTL; --{serverError BAD_ARGUMENTS}

SHOW CREATE TABLE no_prop_table;

DROP TABLE IF EXISTS no_prop_table;

DROP TABLE IF EXISTS r_no_prop_table;

CREATE TABLE r_no_prop_table
(
  some_column UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/{database}/test/01493_r_no_prop_table', '1')
ORDER BY tuple();

SHOW CREATE TABLE r_no_prop_table;

ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE DEFAULT; --{serverError BAD_ARGUMENTS}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE MATERIALIZED; --{serverError BAD_ARGUMENTS}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE ALIAS; --{serverError BAD_ARGUMENTS}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE CODEC; --{serverError BAD_ARGUMENTS}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE COMMENT; --{serverError BAD_ARGUMENTS}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE TTL; --{serverError BAD_ARGUMENTS}

ALTER TABLE r_no_prop_table REMOVE TTL;  --{serverError BAD_ARGUMENTS}

SHOW CREATE TABLE r_no_prop_table;

ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE ttl;  --{serverError BAD_ARGUMENTS}
ALTER TABLE r_no_prop_table remove TTL;  --{serverError BAD_ARGUMENTS}

DROP TABLE IF EXISTS r_no_prop_table;
