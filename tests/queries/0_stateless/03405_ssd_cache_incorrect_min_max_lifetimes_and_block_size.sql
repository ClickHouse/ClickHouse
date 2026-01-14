-- Tags: no-ordinary-database

-- Tests that various conditions are checked during creation of 'ssd_cache' and 'complex_key_ssd_cache' dictionaries

-- Github issue #78314

DROP DICTIONARY IF EXISTS dict;

SELECT 'BLOCK_SIZE is a negative value.';
CREATE DICTIONARY dict (col Int64 default null) PRIMARY KEY (col) SOURCE(NULL()) LAYOUT(SSD_CACHE(BLOCK_SIZE -1)) LIFETIME(1); -- { serverError BAD_ARGUMENTS }
CREATE DICTIONARY dict (col Int64 default null) PRIMARY KEY (col) SOURCE(NULL()) LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE -1)) LIFETIME(1); -- { serverError BAD_ARGUMENTS }

SELECT 'BLOCK_SIZE is zero.';
CREATE DICTIONARY dict (col Int64 default null) PRIMARY KEY (col) SOURCE(NULL()) LAYOUT(SSD_CACHE(BLOCK_SIZE 0)) LIFETIME(1); -- { serverError BAD_ARGUMENTS }
CREATE DICTIONARY dict (col Int64 default null) PRIMARY KEY (col) SOURCE(NULL()) LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 0)) LIFETIME(1); -- { serverError BAD_ARGUMENTS }

SELECT 'WRITE_BUFFER_SIZE is zero.';
CREATE DICTIONARY dict (col Int64 default null) PRIMARY KEY (col) SOURCE(NULL()) LAYOUT(SSD_CACHE(WRITE_BUFFER_SIZE 0)) LIFETIME(1); -- { serverError BAD_ARGUMENTS }
CREATE DICTIONARY dict (col Int64 default null) PRIMARY KEY (col) SOURCE(NULL()) LAYOUT(COMPLEX_KEY_SSD_CACHE(WRITE_BUFFER_SIZE 0)) LIFETIME(1); -- { serverError BAD_ARGUMENTS }
