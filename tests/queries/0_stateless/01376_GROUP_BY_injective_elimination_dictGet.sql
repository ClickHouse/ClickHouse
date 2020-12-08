-- https://github.com/ClickHouse/ClickHouse/issues/11469
SELECT dictGet('default.countryId', 'country', toUInt64(number)) AS country FROM numbers(2) GROUP BY country; -- { serverError 36; }


-- with real dictionary
CREATE TABLE table_for_dict
(
  key_column UInt64,
  value Float64
)
ENGINE = Memory();

INSERT INTO table_for_dict VALUES (1, 1.1);

CREATE DICTIONARY IF NOT EXISTS dict_exists
(
  key_column UInt64,
  value Float64 DEFAULT 77.77
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB currentDatabase()))
LIFETIME(1)
LAYOUT(FLAT());

SELECT dictGet('dictdb_01376.dict_exists', 'value', toUInt64(1)) as val FROM numbers(2) GROUP BY val;

DROP DICTIONARY dictdb_01376.dict_exists;
DROP TABLE dictdb_01376.table_for_dict;
DROP DATABASE dictdb_01376;
