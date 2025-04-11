-- Tags: no-ordinary-database

-- Tests that various lifetime conditions are checked during creation of a dictionary

-- Github issue #78314

DROP DICTIONARY IF EXISTS dx;
DROP TABLE IF EXISTS tbl;

CREATE TABLE tbl (col Int) ENGINE = Memory;

SELECT 'MIN is a negative value.';
CREATE DICTIONARY dx (col Int DEFAULT 1) PRIMARY KEY (col) SOURCE(CLICKHOUSE(TABLE 'tbl')) LAYOUT(HASHED_ARRAY()) LIFETIME(MIN -1 MAX 0); -- { clientError SYNTAX_ERROR }

SELECT 'MAX is a negative value.';
CREATE DICTIONARY dx (col Int DEFAULT 1) PRIMARY KEY (col) SOURCE(CLICKHOUSE(TABLE 'tbl')) LAYOUT(HASHED_ARRAY()) LIFETIME(MIN 0 MAX -1); -- { clientError SYNTAX_ERROR }

SELECT 'MIN is greater than MAX.';
CREATE DICTIONARY dx (col Int DEFAULT 1) PRIMARY KEY (col) SOURCE(CLICKHOUSE(TABLE 'tbl')) LAYOUT(HASHED_ARRAY()) LIFETIME(MIN 1 MAX 0); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS dx;
DROP TABLE IF EXISTS tbl;
