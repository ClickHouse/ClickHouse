CREATE TABLE IF NOT EXISTS left_table
(
    `id` UInt32,
    `value` String
)
ENGINE = Memory;

CREATE TABLE IF NOT EXISTS right_table
(
    `id` UInt32,
    `description` String
)
ENGINE = Memory;

INSERT INTO left_table VALUES (1, 'A'), (2, 'B'), (3, 'C');
INSERT INTO right_table VALUES (2, 'Second'), (3, 'Third'), (4, 'Fourth');

INSERT INTO left_table VALUES (1, 'A'), (2, 'B'), (3, 'C');
INSERT INTO right_table VALUES (2, 'Second'), (3, 'Third'), (4, 'Fourth'), (5, 'Fifth'), (6, 'Sixth');

SELECT l.id, l.value, r.description
FROM left_table l
RIGHT JOIN right_table r ON l.id = r.id;

explain actions=1, keep_logical_steps=0 SELECT l.id, l.value, r.description
FROM left_table l
RIGHT JOIN right_table r ON l.id = r.id;

SELECT l.id, l.value, r.description
FROM left_table l
FULL OUTER JOIN right_table r ON l.id = r.id;

explain actions=1, keep_logical_steps=0 SELECT l.id, l.value, r.description
FROM left_table l
FULL OUTER JOIN right_table r ON l.id = r.id;

DROP TABLE left_table;
DROP TABLE right_table;
