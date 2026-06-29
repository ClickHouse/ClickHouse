CREATE TABLE date_table
(
  id UInt32,
  val String,
  start Date,
  end Date
) Engine = Memory();

INSERT INTO date_table VALUES(1, '1', toDate('2019-01-05'), toDate('2020-01-10'));

CREATE DICTIONARY somedict
(
  id UInt32,
  val String,
  start Date,
  end Date
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'date_table' DB currentDatabase()))
LAYOUT(RANGE_HASHED())
RANGE (MIN start MAX end)
LIFETIME(MIN 300 MAX 360);

SELECT * from somedict;

-- No dictionary columns
SELECT 1 FROM somedict;

SHOW TABLES;
