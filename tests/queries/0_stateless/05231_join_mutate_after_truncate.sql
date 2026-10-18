-- A mutation of a `Join` table tells the files it replaces from the ones inserted after it was
-- committed by their number, and the numbers stay monotonic over the lifetime of the table, so a
-- `TRUNCATE` in between cannot make an old number look like a post-commit insert.

DROP TABLE IF EXISTS j;
CREATE TABLE j (id UInt64, v String) ENGINE = Join(ANY, LEFT, id);

INSERT INTO j SELECT number, toString(number) FROM numbers(10);
TRUNCATE TABLE j;
SELECT 'after truncate', count() FROM (SELECT id FROM j);

INSERT INTO j SELECT number, toString(number) FROM numbers(5);
ALTER TABLE j DELETE WHERE id < 2;
INSERT INTO j SELECT number, toString(number) FROM numbers(100, 3);
SELECT 'in memory', count(), min(id), max(id) FROM (SELECT id FROM j);

DETACH TABLE j;
ATTACH TABLE j;
SELECT 'after a reload', count(), min(id), max(id) FROM (SELECT id FROM j);

DROP TABLE j;
