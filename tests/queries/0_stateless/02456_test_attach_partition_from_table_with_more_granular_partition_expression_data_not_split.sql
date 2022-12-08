DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

SET send_logs_level='error';
ALTER TABLE destination ATTACH PARTITION '20100302' FROM source;
SET send_logs_level='warning';

-- { echoOn }

SELECT * FROM source;
SELECT * FROM destination;

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY intDiv(A, 6);

CREATE TABLE destination (timestamp DateTime, A Int64) engine=MergeTree ORDER BY timestamp PARTITION BY A;

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01', 1), ('2010-03-02 02:01:03', 1);
INSERT INTO TABLE destination VALUES ('2010-01-01', 1);

SET send_logs_level='error';
ALTER TABLE destination ATTACH PARTITION ID '0' FROM source;
SET send_logs_level='warning';


SELECT * FROM source ORDER BY timestamp;
SELECT * FROM destination ORDER BY timestamp;
