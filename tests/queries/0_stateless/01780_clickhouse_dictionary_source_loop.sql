-- Tags: no-parallel

DROP DATABASE IF EXISTS _01780_db;
CREATE DATABASE _01780_db;

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
CREATE DICTIONARY _01780_db.dict2
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DATABASE '_01780_db' TABLE 'dict2'))
LAYOUT(DIRECT());

SELECT * FROM _01780_db.dict2; --{serverError 36}
DROP DICTIONARY _01780_db.dict2;

DROP TABLE IF EXISTS _01780_db.dict3_source;
CREATE TABLE _01780_db.dict3_source
(
    id UInt64,
    value String
) ENGINE = TinyLog;

INSERT INTO _01780_db.dict3_source VALUES (1, '1'), (2, '2'), (3, '3');

CREATE DICTIONARY _01780_db.dict3
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 TABLE 'dict3_source' DATABASE '_01780_db'))
LAYOUT(DIRECT());

SELECT * FROM _01780_db.dict3;

DROP DICTIONARY _01780_db.dict3;

DROP DATABASE _01780_db;
