DROP TABLE IF EXISTS default_table;

CREATE TABLE default_table (
  key UInt64 DEFAULT 42,
  value1 UInt64 MATERIALIZED key * key,
  value2 ALIAS value1 * key
)
ENGINE = MergeTree()
ORDER BY tuple();

ALTER TABLE default_table MODIFY COLUMN key REMOVE MATERIALIZED; --{serverError BAD_ARGUMENTS}
ALTER TABLE default_table MODIFY COLUMN key REMOVE ALIAS; --{serverError BAD_ARGUMENTS}

ALTER TABLE default_table MODIFY COLUMN value1 REMOVE DEFAULT; --{serverError BAD_ARGUMENTS}
ALTER TABLE default_table MODIFY COLUMN value1 REMOVE ALIAS; --{serverError BAD_ARGUMENTS}

ALTER TABLE default_table MODIFY COLUMN value2 REMOVE DEFAULT; --{serverError BAD_ARGUMENTS}
ALTER TABLE default_table MODIFY COLUMN value2 REMOVE MATERIALIZED; --{serverError BAD_ARGUMENTS}

SHOW CREATE TABLE default_table;

DROP TABLE IF EXISTS default_table;
