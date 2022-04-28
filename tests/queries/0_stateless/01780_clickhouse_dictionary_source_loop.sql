-- Tags: no-parallel

DROP DATABASE IF EXISTS 01780_db;
CREATE DATABASE 01780_db;

DROP DICTIONARY IF EXISTS dict1;
CREATE DICTIONARY dict1
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 TABLE 'dict1'))
LAYOUT(DIRECT());

SELECT * FROM dict1; --{serverError 36}

DROP DICTIONARY dict1;

DROP DICTIONARY IF EXISTS dict2;
CREATE DICTIONARY 01780_db.dict2
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DATABASE '01780_db' TABLE 'dict2'))
LAYOUT(DIRECT());

SELECT * FROM 01780_db.dict2; --{serverError 36}
DROP DICTIONARY 01780_db.dict2;

DROP TABLE IF EXISTS 01780_db.dict3_source;
CREATE TABLE 01780_db.dict3_source
(
    id UInt64,
    value String
) ENGINE = TinyLog;

INSERT INTO 01780_db.dict3_source VALUES (1, '1'), (2, '2'), (3, '3');

CREATE DICTIONARY 01780_db.dict3
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 TABLE 'dict3_source' DATABASE '01780_db'))
LAYOUT(DIRECT());

SELECT * FROM 01780_db.dict3;

DROP DICTIONARY 01780_db.dict3;

DROP TABLE IF EXISTS 01780_db.dict4_source;
CREATE TABLE 01780_db.dict4_source
(
    id UInt64,
    value String
) ENGINE = TinyLog;

DROP TABLE IF EXISTS 01780_db.dict4_view; 
CREATE VIEW 01780_db.dict4_view
(
    id UInt64,
    value String
) AS SELECT id, value FROM 01780_db.dict4_source WHERE id = (SELECT max(id) FROM 01780_db.dict4_source);

INSERT INTO 01780_db.dict4_source VALUES (1, '1'), (2, '2'), (3, '3');

DROP DICTIONARY IF EXISTS 01780_db.dict4;
CREATE DICTIONARY 01780_db.dict4
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 TABLE 'dict4_view' DATABASE '01780_db'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT * FROM 01780_db.dict4;

INSERT INTO 01780_db.dict4_source VALUES (4, '4');

SELECT sleep(3);
SELECT sleep(3);

SELECT * FROM 01780_db.dict4;

DROP DICTIONARY 01780_db.dict4;

DROP DATABASE 01780_db;
