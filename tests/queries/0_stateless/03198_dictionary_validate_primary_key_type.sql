CREATE DICTIONARY `test_dictionary0` (
    `n1` String,
    `n2` UInt32
)
PRIMARY KEY n1
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'test_db' TABLE 'table_01' USER 'default'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SET dictionary_validate_primary_key_type=1;

CREATE DICTIONARY `test_dictionary1` (
    `n1` String,
    `n2` UInt32
)
PRIMARY KEY n1
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'test_db' TABLE 'table_01' USER 'default'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());  -- { serverError BAD_ARGUMENTS }

CREATE DICTIONARY `test_dictionary2` (
    `n1` UInt32,
    `n2` UInt32
)
PRIMARY KEY n1
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'test_db' TABLE 'table_01' USER 'default'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT()); -- { serverError BAD_ARGUMENTS }

CREATE DICTIONARY `test_dictionary3` (
    `n1` UInt64,
    `n2` UInt32
)
PRIMARY KEY n1
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'test_db' TABLE 'table_01' USER 'default'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

DESCRIBE `test_dictionary0`;
DESCRIBE `test_dictionary3`;

