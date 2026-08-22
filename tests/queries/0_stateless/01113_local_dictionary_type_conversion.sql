CREATE TABLE table_for_dict (
  CompanyID String,
  OSType Enum('UNKNOWN' = 0, 'WINDOWS' = 1, 'LINUX' = 2, 'ANDROID' = 3, 'MAC' = 4),
  SomeID Int32
)
ENGINE = Memory();

INSERT INTO table_for_dict VALUES ('First', 'WINDOWS', 1), ('Second', 'LINUX', 2);

CREATE DICTIONARY dict_with_conversion
(
  CompanyID String DEFAULT '',
  OSType String DEFAULT '',
  SomeID Int32 DEFAULT 0
)
PRIMARY KEY CompanyID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB currentDatabase()))
LIFETIME(MIN 1 MAX 20)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT * FROM dict_with_conversion ORDER BY CompanyID;
