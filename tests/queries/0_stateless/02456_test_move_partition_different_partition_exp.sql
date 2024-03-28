-- { echoOn }
SET send_logs_level='error';
-- Should be allowed since destination partition expr is monotonically increasing and compatible
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE source MOVE PARTITION ID '20100302' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE source;
TRUNCATE TABLE destination;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE source MOVE PARTITION '20100302' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed since destination partition expr is monotonically increasing and compatible. Note that even though
-- the destination partition expression is more granular, the data would still fall in the same partition. Thus, it is valid
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE source MOVE PARTITION ID '201003' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE source;
TRUNCATE TABLE destination;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE source MOVE PARTITION '201003' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed since destination partition expr is monotonically increasing and compatible for those specific values
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY intDiv(A, 6);

CREATE TABLE destination (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY A;

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01', 1), ('2010-03-02 02:01:03', 1);

ALTER TABLE source MOVE PARTITION ID '0' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE source;
TRUNCATE TABLE destination;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01', 1), ('2010-03-02 02:01:03', 1);

ALTER TABLE source MOVE PARTITION '0' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed because dst partition exp is monot inc and data is not split
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY cityHash64(category);
CREATE TABLE destination (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY toString(category);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('spaghetti', 'food'), ('mop', 'general');
INSERT INTO TABLE source VALUES ('rice', 'food');

ALTER TABLE source MOVE PARTITION ID '17908065610379824077' TO TABLE destination;

SELECT * FROM source ORDER BY productName;
SELECT * FROM destination ORDER BY productName;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed, extra test case to validate https://github.com/ClickHouse/ClickHouse/pull/39507#issuecomment-1747574133
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp Int64) engine=MergeTree ORDER BY (timestamp) PARTITION BY intDiv(timestamp, 86400000);
CREATE TABLE destination (timestamp Int64) engine=MergeTree ORDER BY (timestamp) PARTITION BY toYear(toDateTime(intDiv(timestamp, 1000)));

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (1267495261123);

ALTER TABLE source MOVE PARTITION ID '14670' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE source;
TRUNCATE TABLE destination;

INSERT INTO TABLE source VALUES (1267495261123);

ALTER TABLE source MOVE PARTITION '14670' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed, extra test case to validate https://github.com/ClickHouse/ClickHouse/pull/39507#issuecomment-1747511726

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime('UTC'), key Int64, f Float64) engine=MergeTree ORDER BY (key, timestamp) PARTITION BY toYear(timestamp);
CREATE TABLE destination (timestamp DateTime('UTC'), key Int64, f Float64) engine=MergeTree ORDER BY (key, timestamp) PARTITION BY (intDiv(toUInt32(timestamp),86400));

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01',1,1),('2010-03-02 02:01:01',1,1),('2011-02-02 02:01:03',1,1);

ALTER TABLE source MOVE PARTITION ID '2010' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed, partitioned table to unpartitioned. Since the destination is unpartitioned, parts would ultimately
-- fall into the same partition.
-- Destination partition by expression is omitted, which causes StorageMetadata::getPartitionKeyAST() to be nullptr.
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;
CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE source MOVE PARTITION ID '201003' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Same as above, but destination partition by expression is explicitly defined. Test case required to validate that
-- partition by tuple() is accepted.
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;
CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE source MOVE PARTITION ID '201003' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed because the destination partition expression columns are a subset of the source partition expression columns
-- Columns in this case refer to the expression elements, not to the actual table columns
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;
CREATE TABLE source (a Int, b Int, c Int) engine=MergeTree ORDER BY tuple() PARTITION BY (a, b, c);
CREATE TABLE destination (a Int, b Int, c Int) engine=MergeTree ORDER BY tuple() PARTITION BY (a, b);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (1, 2, 3), (1, 2, 4);

ALTER TABLE source MOVE PARTITION ID '1-2-3' TO TABLE destination;

SELECT * FROM source ORDER BY (a, b, c);
SELECT * FROM destination ORDER BY (a, b, c);
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed because the destination partition expression columns are a subset of the source partition expression columns
-- Columns in this case refer to the expression elements, not to the actual table columns
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;
CREATE TABLE source (a Int, b Int, c Int) engine=MergeTree ORDER BY tuple() PARTITION BY (a, b, c);
CREATE TABLE destination (a Int, b Int, c Int) engine=MergeTree ORDER BY tuple() PARTITION BY a;

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (1, 2, 3), (1, 2, 4);

ALTER TABLE source MOVE PARTITION ID '1-2-3' TO TABLE destination;

SELECT * FROM source ORDER BY (a, b, c);
SELECT * FROM destination ORDER BY (a, b, c);
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed. Special test case, tricky to explain. First column of source partition expression is
-- timestamp, while first column of destination partition expression is `A`. One of the previous implementations
-- would not match the columns, which could lead to `timestamp` min max being used to calculate monotonicity of `A`.
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (`timestamp` DateTime, `A` Int64) ENGINE = MergeTree PARTITION BY tuple(toYYYYMM(timestamp), intDiv(A, 6)) ORDER BY timestamp;
CREATE TABLE destination (`timestamp` DateTime, `A` Int64) ENGINE = MergeTree PARTITION BY A ORDER BY timestamp;

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01', 5);

ALTER TABLE source MOVE PARTITION ID '201003-0' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Same as above, but slightly different
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (b String, a Int) ENGINE = MergeTree PARTITION BY tuple(b, a) ORDER BY tuple();
CREATE TABLE destination (a Int, b String) ENGINE = MergeTree PARTITION BY tuple(a) ORDER BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('abc', 1);

ALTER TABLE source MOVE PARTITION ('abc', 1) TO TABLE destination;

SELECT * FROM source;
SELECT * FROM destination;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed. Destination partition expression contains multiple expressions, but all of them are monotonically
-- increasing in the source partition min max indexes.
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (A Int, B Int) ENGINE = MergeTree PARTITION BY tuple(A, B) ORDER BY tuple();
CREATE TABLE destination (A Int, B Int) ENGINE = MergeTree PARTITION BY tuple(intDiv(A, 2), intDiv(B, 2)) ORDER BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (6, 12);

ALTER TABLE source MOVE PARTITION ID '6-12' TO TABLE destination;

SELECT * FROM source ORDER BY A;
SELECT * FROM destination ORDER BY A;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed, validate tuple() to non-partitioned (should be the same).
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY tuple();
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE source MOVE PARTITION ID 'all' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed, validate non-partitioned to tuple() (should be the same).
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple();
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE source MOVE PARTITION ID 'all' TO TABLE destination;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed. The same scenario as above, but partition expressions inverted.
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (A Int, B Int) ENGINE = MergeTree PARTITION BY tuple(intDiv(A, 2), intDiv(B, 2)) ORDER BY tuple();
CREATE TABLE destination (A Int, B Int) ENGINE = MergeTree PARTITION BY tuple(A, B) ORDER BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (6, 12);

ALTER TABLE source MOVE PARTITION ID '3-6' TO TABLE destination;

SELECT * FROM source ORDER BY A;
SELECT * FROM destination ORDER BY A;
SELECT partition_id FROM system.parts where table='source' AND database = currentDatabase() AND active = 1;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;
SET send_logs_level='warning';
