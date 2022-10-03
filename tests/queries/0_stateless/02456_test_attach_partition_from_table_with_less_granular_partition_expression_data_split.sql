DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS destination;

CREATE TABLE source (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp);
CREATE TABLE destination (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp);

INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-03 02:01:03');

ALTER TABLE destination ATTACH PARTITION '201003' FROM source; -- { serverError 248 }