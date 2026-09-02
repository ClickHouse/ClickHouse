DROP DICTIONARY IF EXISTS range_dictionary;
DROP TABLE IF EXISTS range_dictionary_nullable_source_table;


CREATE TABLE range_dictionary_nullable_source_table
(
  key UInt64,
  start_date Date,
  end_date Date,
  value Nullable(UInt64)
)
ENGINE = TinyLog;

INSERT INTO range_dictionary_nullable_source_table VALUES (0, toDate('2019-05-05'), toDate('2019-05-20'), 0), (1, toDate('2019-05-05'), toDate('2019-05-20'), NULL);

CREATE DICTIONARY range_dictionary
(
  key UInt64,
  start_date Date,
  end_date Date,
  value Nullable(UInt64) DEFAULT NULL
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_dictionary_nullable_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);

SELECT dictGetOrDefault('range_dictionary', 'value', toUInt64(2), toDate(toLowCardinality(materialize('2019-05-15'))), 2);


DROP DICTIONARY IF EXISTS range_dictionary;
DROP TABLE IF EXISTS range_dictionary_nullable_source_table;

