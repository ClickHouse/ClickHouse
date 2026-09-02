DROP TABLE IF EXISTS tab;

CREATE TABLE tab (col FixedString(2)) engine = MergeTree() ORDER BY col;

INSERT INTO tab VALUES ('AA') ('Aa');

SELECT col, col LIKE '%a', col ILIKE '%a' FROM tab WHERE col = 'AA';
SELECT col, col LIKE '%a', col ILIKE '%a' FROM tab WHERE col = 'Aa';

DROP TABLE IF EXISTS tab;
