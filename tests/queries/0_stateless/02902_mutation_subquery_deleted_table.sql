-- Mutations containing subqueries to deleted tables don't retry indefinitely and don't flood logs
DROP TABLE IF EXISTS main;
DROP TABLE IF EXISTS sub;
CREATE TABLE main (id Int8) ENGINE=MergeTree() ORDER BY id;
CREATE TABLE sub as main;
INSERT INTO main SELECT * FROM system.numbers LIMIT 3;
INSERT INTO sub SELECT * FROM system.numbers LIMIT 3;

ALTER TABLE main DELETE WHERE id IN (SELECT id + sleepEachRow(1) FROM sub);
DROP TABLE sub;

SELECT count() FROM system.mutations WHERE table='main' AND NOT is_done;
SELECT sleep(3) FORMAT Null;

SELECT value < 10 FROM system.errors WHERE code = 60 AND last_error_message LIKE '%.sub does not exist';
SELECT count() FROM system.mutations WHERE table='main' AND NOT is_done;

KILL MUTATION WHERE table = 'main' FORMAT Null;
DROP TABLE main;
