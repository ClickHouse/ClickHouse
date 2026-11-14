-- Tests that a column of both copies of a self comma join resolves to the copy aliased by the qualifier, else to the unaliased copy.
DROP TABLE IF EXISTS self_comma;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS self_comma;
CREATE TABLE self_comma (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO self_comma VALUES (1), (2);

SELECT countIf(b.id != self_comma.id) FROM self_comma AS b, self_comma AS self_comma;
SELECT countIf(self_comma.id != b.id) FROM self_comma AS self_comma, self_comma AS b;
SELECT countIf(a.id != self_comma.id) FROM self_comma AS a, self_comma;
SELECT countIf(a.id != id) FROM self_comma AS a CROSS JOIN self_comma;

DROP TABLE self_comma;
