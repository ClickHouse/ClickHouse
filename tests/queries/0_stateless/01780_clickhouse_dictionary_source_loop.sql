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
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dict1'))
LAYOUT(DIRECT());

SELECT * FROM dict1; --{serverError BAD_ARGUMENTS}

DROP DICTIONARY dict1;

DROP DICTIONARY IF EXISTS dict2;
CREATE DICTIONARY 01780_db.dict2
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DATABASE '01780_db' TABLE 'dict2'))
LAYOUT(DIRECT());

SELECT * FROM 01780_db.dict2; --{serverError BAD_ARGUMENTS}
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
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dict3_source' DATABASE '01780_db'))
LAYOUT(DIRECT());

SELECT * FROM 01780_db.dict3;

DROP DICTIONARY 01780_db.dict3;

DROP DATABASE 01780_db;
