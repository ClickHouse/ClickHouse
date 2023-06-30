-- { echoOn }
-- Should be allowed since destination partition expr is monotonically increasing and compatible
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE destination ATTACH PARTITION ID '20100302' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase();

-- Should be allowed since destination partition expr is monotonically increasing and compatible. Note that even though
-- the destination partition expression is more granular, the data would still fall in the same partition. Thus, it is valid
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

ALTER TABLE destination ATTACH PARTITION ID '201003' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase();

-- Should be allowed since destination partition expr is monotonically increasing and compatible for those specific values
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY intDiv(A, 6);

CREATE TABLE destination (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY A;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01', 1), ('2010-03-02 02:01:03', 1);

ALTER TABLE destination ATTACH PARTITION ID '0' FROM source;

SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase();

-- Should be allowed because dst partition exp is monot inc and data is not split
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY cityHash64(category);
CREATE TABLE destination (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY toString(category);

INSERT INTO TABLE source VALUES ('spaghetti', 'food'), ('mop', 'general');
INSERT INTO TABLE source VALUES ('rice', 'food');

ALTER TABLE destination ATTACH PARTITION ID '17908065610379824077' from source;

SELECT * FROM source ORDER BY productName;
SELECT * FROM destination ORDER BY productName;
SELECT partition_id FROM system.parts where table='destination' AND database = currentDatabase();

-- Should not be allowed because data would be split into two different partitions
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-03 02:01:03');

ALTER TABLE destination ATTACH PARTITION ID '201003' FROM source; -- { serverError 248 }

-- Should not be allowed because data would be split into two different partitions
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY intDiv(A, 6);

CREATE TABLE destination (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY A;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01', 1), ('2010-03-02 02:01:03', 2);

ALTER TABLE destination ATTACH PARTITION ID '0' FROM source; -- { serverError 248 }

-- Should not be allowed because dst partition exp takes more than two arguments, so it's not considered monotonically inc
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY toString(category);
CREATE TABLE destination (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY substring(category, 1, 2);

INSERT INTO TABLE source VALUES ('spaghetti', 'food'), ('mop', 'general');
INSERT INTO TABLE source VALUES ('rice', 'food');

ALTER TABLE destination ATTACH PARTITION ID '4590ba78048910b74a47d5bfb308abed' from source; -- { serverError 36 }

-- Should not be allowed because dst partition exp depends on a different set of columns
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY toString(category);
CREATE TABLE destination (productName String, category String) engine=MergeTree ORDER BY tuple() PARTITION BY toString(productName);

INSERT INTO TABLE source VALUES ('spaghetti', 'food'), ('mop', 'general');
INSERT INTO TABLE source VALUES ('rice', 'food');

ALTER TABLE destination ATTACH PARTITION ID '4590ba78048910b74a47d5bfb308abed' from source; -- { serverError 36 }

-- Should not be allowed because dst partition exp is not monotonically increasing
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (productName String) engine=MergeTree ORDER BY tuple() PARTITION BY left(productName, 2);
CREATE TABLE destination (productName String) engine=MergeTree ORDER BY tuple() PARTITION BY cityHash64(productName);

INSERT INTO TABLE source VALUES ('bread'), ('mop');
INSERT INTO TABLE source VALUES ('broccoli');

ALTER TABLE destination ATTACH PARTITION ID '4589453b7ee96ce9de1265bd57674496' from source; -- { serverError 36 }
