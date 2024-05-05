CREATE TABLE source1 (id UInt32) ORDER BY id;
CREATE TABLE super (id UInt32) ENGINE = StreamQueue(source1, id);
CREATE TABLE result (id UInt32) ORDER BY id;
CREATE MATERIALIZED VIEW consumer TO result AS SELECT id FROM super;
SELECT COUNT(*) FROM result;

INSERT INTO source1(id) VALUES(5);
SELECT sleep(1);
SELECT COUNT(*) FROM result;

INSERT INTO source1(id) VALUES(10);
INSERT INTO source1(id) VALUES(15);
SELECT sleep(1);
SELECT COUNT(*) FROM result;

INSERT INTO source1(id) VALUES(1);
SELECT sleep(1);
SELECT COUNT(*) FROM result;
