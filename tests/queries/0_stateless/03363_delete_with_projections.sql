
SET lightweight_deletes_sync = 2, alter_sync = 2, mutations_sync = 2;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select count(), age group by age),
    projection p2 (select age, name group by age, name)
) ENGINE = MergeTree order by uid
SETTINGS lightweight_mutation_projection_mode='drop';

INSERT INTO t0 VALUES (123, 'John', 33);
INSERT INTO t0 VALUES (456, 'Pol', 44);

DELETE FROM t0 WHERE uid = 123;

ALTER TABLE t0 DELETE WHERE uid = 456;

SELECT * FROM t0 ORDER BY uid;
