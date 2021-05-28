DROP TABLE IF EXISTS no_prop_table;

CREATE TABLE no_prop_table
(
    some_column UInt64
)
ENGINE MergeTree()
ORDER BY tuple();

SHOW CREATE TABLE no_prop_table;

-- just nothing happened
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE DEFAULT; --{serverError 36}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE MATERIALIZED; --{serverError 36}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE ALIAS; --{serverError 36}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE CODEC; --{serverError 36}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE COMMENT; --{serverError 36}
ALTER TABLE no_prop_table MODIFY COLUMN some_column REMOVE TTL; --{serverError 36}

ALTER TABLE no_prop_table REMOVE TTL; --{serverError 36}

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

ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE DEFAULT; --{serverError 36}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE MATERIALIZED; --{serverError 36}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE ALIAS; --{serverError 36}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE CODEC; --{serverError 36}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE COMMENT; --{serverError 36}
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE TTL; --{serverError 36}

ALTER TABLE r_no_prop_table REMOVE TTL;  --{serverError 36}

SHOW CREATE TABLE r_no_prop_table;

ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE ttl;  --{serverError 36}
ALTER TABLE r_no_prop_table remove TTL;  --{serverError 36}

DROP TABLE IF EXISTS r_no_prop_table;
