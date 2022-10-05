DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03');

SET send_logs_level='error';
ALTER TABLE destination ATTACH PARTITION '20100302' FROM source;
SET send_logs_level='warning';

SELECT * FROM source;
SELECT 1;
SELECT * FROM destination;