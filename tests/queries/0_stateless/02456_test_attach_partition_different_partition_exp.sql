-- { echoOn }
-- Should be allowed since destination partition expr is monotonically increasing and compatible
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE destination ATTACH PARTITION ID '20100302' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION '20100302' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed since destination partition expr is monotonically increasing and compatible. Note that even though
-- the destination partition expression is more granular, the data would still fall in the same partition. Thus, it is valid
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE destination ATTACH PARTITION ID '201003' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION '201003' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed since destination partition expr is monotonically increasing and compatible for those specific values
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY intDiv(A, 6);

CREATE TABLE destination (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY A;

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01', 1), ('2010-03-02 02:01:03', 1);

ALTER TABLE destination ATTACH PARTITION ID '0' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION 0 FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed because dst partition exp is monot inc and data is not split
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY cityHash64(category);
CREATE TABLE destination (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY toString(category);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('spaghetti', 'food'), ('mop', 'general');
INSERT INTO TABLE source VALUES ('rice', 'food');

ALTER TABLE destination ATTACH PARTITION ID '17908065610379824077' from source;

SELECT * FROM source ORDER BY productName;
SELECT * FROM destination ORDER BY productName;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION '17908065610379824077' from source;

SELECT * FROM source ORDER BY productName;
SELECT * FROM destination ORDER BY productName;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed, extra test case to validate https://github.com/ClickHouse/ClickHouse/pull/39507#issuecomment-1747574133

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp Int64) engine=MergeTree ORDER BY (timestamp) PARTITION BY intDiv(timestamp, 86400000);
CREATE TABLE destination (timestamp Int64) engine=MergeTree ORDER BY (timestamp) PARTITION BY toYear(toDateTime(intDiv(timestamp, 1000)));

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (1267495261123);

ALTER TABLE destination ATTACH PARTITION ID '14670' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION '14670' from source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed, extra test case to validate https://github.com/ClickHouse/ClickHouse/pull/39507#issuecomment-1747511726

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime('UTC'), key Int64, f Float64) engine=MergeTree ORDER BY (key, timestamp) PARTITION BY toYear(timestamp);
CREATE TABLE destination (timestamp DateTime('UTC'), key Int64, f Float64) engine=MergeTree ORDER BY (key, timestamp) PARTITION BY (intDiv(toUInt32(timestamp),86400));

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01',1,1),('2010-03-02 02:01:01',1,1),('2011-02-02 02:01:03',1,1);

ALTER TABLE destination ATTACH PARTITION ID '2010' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION '2010' from source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
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

ALTER TABLE destination ATTACH PARTITION ID '201003' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION '201003' from source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Same as above, but destination partition by expression is explicitly defined. Test case required to validate that
-- partition by tuple() is accepted.
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;
CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE destination ATTACH PARTITION ID '201003' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION '201003' from source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed because the destination partition expression columns are a subset of the source partition expression columns
-- Columns in this case refer to the expression elements, not to the actual table columns
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;
CREATE TABLE source (a Int, b Int, c Int) engine=MergeTree ORDER BY tuple() PARTITION BY (a, b, c);
CREATE TABLE destination (a Int, b Int, c Int) engine=MergeTree ORDER BY tuple() PARTITION BY (a, b);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (1, 2, 3), (1, 2, 4);

ALTER TABLE destination ATTACH PARTITION ID '1-2-3' FROM source;

SELECT * FROM source ORDER BY (a, b, c);
SELECT * FROM destination ORDER BY (a, b, c);
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION (1, 2, 3) from source;

SELECT * FROM source ORDER BY (a, b, c);
SELECT * FROM destination ORDER BY (a, b, c);
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed because the destination partition expression columns are a subset of the source partition expression columns
-- Columns in this case refer to the expression elements, not to the actual table columns
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;
CREATE TABLE source (a Int, b Int, c Int) engine=MergeTree ORDER BY tuple() PARTITION BY (a, b, c);
CREATE TABLE destination (a Int, b Int, c Int) engine=MergeTree ORDER BY tuple() PARTITION BY a;

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (1, 2, 3), (1, 2, 4);

ALTER TABLE destination ATTACH PARTITION ID '1-2-3' FROM source;

SELECT * FROM source ORDER BY (a, b, c);
SELECT * FROM destination ORDER BY (a, b, c);
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION (1, 2, 3) from source;

SELECT * FROM source ORDER BY (a, b, c);
SELECT * FROM destination ORDER BY (a, b, c);
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

ALTER TABLE destination ATTACH PARTITION ID '201003-0' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION (201003, 0) from source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed. Destination partition expression contains multiple expressions, but all of them are monotonically
-- increasing in the source partition min max indexes.
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (A Int, B Int) ENGINE = MergeTree PARTITION BY tuple(A, B) ORDER BY tuple();
CREATE TABLE destination (A Int, B Int) ENGINE = MergeTree PARTITION BY tuple(intDiv(A, 2), intDiv(B, 2)) ORDER BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (6, 12);

ALTER TABLE destination ATTACH PARTITION ID '6-12' FROM source;

SELECT * FROM source ORDER BY A;
SELECT * FROM destination ORDER BY A;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION (6, 12) from source;

SELECT * FROM source ORDER BY A;
SELECT * FROM destination ORDER BY A;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed. The same scenario as above, but partition expressions inverted.
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (A Int, B Int) ENGINE = MergeTree PARTITION BY tuple(intDiv(A, 2), intDiv(B, 2)) ORDER BY tuple();
CREATE TABLE destination (A Int, B Int) ENGINE = MergeTree PARTITION BY tuple(A, B) ORDER BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES (6, 12);

ALTER TABLE destination ATTACH PARTITION ID '3-6' FROM source;

SELECT * FROM source ORDER BY A;
SELECT * FROM destination ORDER BY A;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION (3, 6) from source;

SELECT * FROM source ORDER BY A;
SELECT * FROM destination ORDER BY A;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed, it is a local operation, no different than regular attach. Replicated to replicated.
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;
CREATE TABLE
    source(timestamp DateTime)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/source_replicated_to_replicated_distinct_expression', '1')
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY tuple();

CREATE TABLE
    destination(timestamp DateTime)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/destination_replicated_to_replicated_distinct_expression', '1')
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE destination ATTACH PARTITION ID '20100302' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION '20100302' from source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should be allowed, it is a local operation, no different than regular attach. Non replicated to replicated
DROP TABLE IF EXISTS source SYNC;
DROP TABLE IF EXISTS destination SYNC;
CREATE TABLE source(timestamp DateTime) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(timestamp) ORDER BY tuple();

CREATE TABLE
    destination(timestamp DateTime)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/destination_non_replicated_to_replicated_distinct_expression', '1')
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY tuple();

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE destination ATTACH PARTITION ID '20100302' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

TRUNCATE TABLE destination;

ALTER TABLE destination ATTACH PARTITION '20100302' from source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Should not be allowed because data would be split into two different partitions
DROP TABLE IF EXISTS source SYNC;
DROP TABLE IF EXISTS destination SYNC;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-03 02:01:03');

ALTER TABLE destination ATTACH PARTITION ID '201003' FROM source; -- { serverError 248 }
ALTER TABLE destination ATTACH PARTITION '201003' from source; -- { serverError 248 }

-- Should not be allowed because data would be split into two different partitions
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY intDiv(A, 6);

CREATE TABLE destination (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY A;

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01', 1), ('2010-03-02 02:01:03', 2);

ALTER TABLE destination ATTACH PARTITION ID '0' FROM source; -- { serverError 248 }
ALTER TABLE destination ATTACH PARTITION 0 FROM source; -- { serverError 248 }

-- Should not be allowed because dst partition exp takes more than two arguments, so it's not considered monotonically inc
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY toString(category);
CREATE TABLE destination (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY substring(category, 1, 2);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('spaghetti', 'food'), ('mop', 'general');
INSERT INTO TABLE source VALUES ('rice', 'food');

ALTER TABLE destination ATTACH PARTITION ID '4590ba78048910b74a47d5bfb308abed' from source; -- { serverError 36 }
ALTER TABLE destination ATTACH PARTITION 'food' from source; -- { serverError 36 }

-- Should not be allowed because dst partition exp depends on a different set of columns
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY toString(category);
CREATE TABLE destination (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY toString(productName);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('spaghetti', 'food'), ('mop', 'general');
INSERT INTO TABLE source VALUES ('rice', 'food');

ALTER TABLE destination ATTACH PARTITION ID '4590ba78048910b74a47d5bfb308abed' from source; -- { serverError 36 }
ALTER TABLE destination ATTACH PARTITION 'food' from source; -- { serverError 36 }

-- Should not be allowed because dst partition exp is not monotonically increasing
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (productName String) engine=MergeTree ORDER BY tuple() PARTITION BY left(productName, 2);
CREATE TABLE destination (productName String) engine=MergeTree ORDER BY tuple() PARTITION BY cityHash64(productName);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE source VALUES ('bread'), ('mop');
INSERT INTO TABLE source VALUES ('broccoli');

ALTER TABLE destination ATTACH PARTITION ID '4589453b7ee96ce9de1265bd57674496' from source; -- { serverError 36 }
ALTER TABLE destination ATTACH PARTITION 'br' from source; -- { serverError 36 }

-- Empty/ non-existent partition, same partition expression. Nothing should happen
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

ALTER TABLE destination ATTACH PARTITION ID '1' FROM source;
ALTER TABLE destination ATTACH PARTITION 1 FROM source;

SELECT * FROM destination;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Empty/ non-existent partition, different partition expression. Nothing should happen
-- https://github.com/ClickHouse/ClickHouse/pull/39507#discussion_r1399839045
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

ALTER TABLE destination ATTACH PARTITION ID '1' FROM source;
ALTER TABLE destination ATTACH PARTITION 1 FROM source;

SELECT * FROM destination;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Replace instead of attach. Empty/ non-existent partition, same partition expression. Nothing should happen
-- https://github.com/ClickHouse/ClickHouse/pull/39507#discussion_r1399839045
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

ALTER TABLE destination REPLACE PARTITION '1' FROM source;

SELECT * FROM destination;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;

-- Replace instead of attach. Empty/ non-existent partition to non-empty partition, same partition id.
-- https://github.com/ClickHouse/ClickHouse/pull/39507#discussion_r1399839045
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (A Int) engine=MergeTree ORDER BY tuple() PARTITION BY A;
CREATE TABLE destination (A Int) engine=MergeTree ORDER BY tuple() PARTITION BY A;

ALTER TABLE destination MODIFY SETTING allow_experimental_alter_partition_with_different_key = 1;

INSERT INTO TABLE destination VALUES (1);

ALTER TABLE destination REPLACE PARTITION '1' FROM source;

SELECT * FROM destination;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase() AND active = 1;
