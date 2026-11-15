-- Tags: no-parallel

DROP DATABASE IF EXISTS 01902_db_params;
CREATE DATABASE 01902_db_params;
CREATE TABLE 01902_db_params.t(n Int8) ENGINE=MergeTree ORDER BY n;
INSERT INTO 01902_db_params.t SELECT * FROM numbers(3);
SELECT _database, _table, n FROM merge(REGEXP('^01902_db_params'), '^t') ORDER BY _database, _table, n;

SELECT _database, _table, n FROM merge() ORDER BY _database, _table, n; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT _database, _table, n FROM merge('^t') ORDER BY _database, _table, n; -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}

USE 01902_db_params;
SELECT _database, _table, n FROM merge('^t') ORDER BY _database, _table, n;

DROP DATABASE 01902_db_params;
