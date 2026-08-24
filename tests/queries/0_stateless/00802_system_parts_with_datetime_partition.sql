DROP TABLE IF EXISTS datetime_table;

-- Create a table with DateTime column, but not used in partition key
CREATE TABLE datetime_table
  (
    t DateTime('UTC'),
    name String,
    value UInt32
  ) ENGINE = MergeTree()
    ORDER BY (t, name)
	PARTITION BY value;

INSERT INTO datetime_table VALUES ('2016-01-01 00:00:00','name1',2);
INSERT INTO datetime_table VALUES ('2016-01-02 00:00:00','name2',2);
INSERT INTO datetime_table VALUES ('2016-01-03 00:00:00','name1',4);

-- min_time and max_time are not filled

SELECT partition, toTimeZone(MIN(min_time), 'UTC') as min_time, toTimeZone(MAX(max_time), 'UTC') as max_time
FROM system.parts
WHERE database = currentDatabase() and table = 'datetime_table' AND active = 1
GROUP BY partition
ORDER BY partition ASC
FORMAT CSV;

DROP TABLE IF EXISTS datetime_table;

-- Create a table with DateTime column, this time used in partition key
CREATE TABLE datetime_table
  (
    t DateTime('UTC'),
    name String,
    value UInt32
  ) ENGINE = MergeTree()
    ORDER BY (t, name)
	PARTITION BY toStartOfDay(t);

INSERT INTO datetime_table VALUES ('2016-01-01 00:00:00','name1',2);
INSERT INTO datetime_table VALUES ('2016-01-01 02:00:00','name1',3);
INSERT INTO datetime_table VALUES ('2016-01-02 01:00:00','name2',2);
INSERT INTO datetime_table VALUES ('2016-01-02 23:00:00','name2',5);
INSERT INTO datetime_table VALUES ('2016-01-03 04:00:00','name1',4);

-- min_time and max_time are now filled

SELECT partition, toTimeZone(MIN(min_time), 'UTC') as min_time, toTimeZone(MAX(max_time), 'UTC') as max_time
FROM system.parts
WHERE database = currentDatabase() and table = 'datetime_table' AND active = 1
GROUP BY partition
ORDER BY partition ASC
FORMAT CSV;

DROP TABLE IF EXISTS datetime_table;

-- Create a table with DateTime column, this time used in partition key, but not at the first level
CREATE TABLE datetime_table
  (
    t DateTime('UTC'),
    name String,
    value UInt32
  ) ENGINE = MergeTree()
    ORDER BY (t, name)
        PARTITION BY (name, toUInt32(toUnixTimestamp(t)/(60*60*24)) );

-- We are using a daily aggregation that is independant of the timezone, add data also

INSERT INTO datetime_table VALUES (1451606400,'name1',2);
INSERT INTO datetime_table VALUES (1451613600,'name1',3);
INSERT INTO datetime_table VALUES (1451696400,'name2',2);
INSERT INTO datetime_table VALUES (1451775600,'name2',5);
INSERT INTO datetime_table VALUES (1451793600,'name1',4);

-- min_time and max_time are now filled

SELECT partition, toUnixTimestamp(MIN(min_time)) as min_unix_time, toUnixTimestamp(MAX(max_time)) as max_unix_time
FROM system.parts
WHERE database = currentDatabase() and table = 'datetime_table' AND active = 1
GROUP BY partition
ORDER BY partition ASC
FORMAT CSV;

DROP TABLE IF EXISTS datetime_table;
