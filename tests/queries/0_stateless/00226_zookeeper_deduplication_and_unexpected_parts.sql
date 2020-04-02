DROP TABLE IF EXISTS deduplication;
CREATE TABLE deduplication (d Date DEFAULT '2015-01-01', x Int8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/deduplication', 'r1', d, x, 1);

INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);

SELECT * FROM deduplication;

DETACH TABLE deduplication;
ATTACH TABLE deduplication (d Date DEFAULT '2015-01-01', x Int8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/deduplication', 'r1', d, x, 1);

SELECT * FROM deduplication;

DROP TABLE deduplication;
