-- Mutations containing subqueries to nonexistent tables don't retry indefinitely and don't flood logs
DROP TABLE IF EXISTS main;
CREATE TABLE main (id Int8) ENGINE=MergeTree() ORDER BY id;
INSERT INTO main SELECT * FROM system.numbers LIMIT 2;

ALTER TABLE main DELETE WHERE id IN (SELECT id FROM nonexistent);
SELECT sleep(3) FORMAT Null;
SELECT value < 2 FROM system.errors WHERE code = 60 AND last_error_message LIKE '%.nonexistent does not exist';
SELECT count() FROM system.mutations WHERE table='main' and database=currentDatabase() AND NOT is_done;
KILL MUTATION WHERE table = 'main' FORMAT Null;
DROP TABLE main;
