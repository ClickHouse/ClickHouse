-- Tags: no-old-analyzer

DROP TABLE IF EXISTS unnest_cross_src;
CREATE TABLE unnest_cross_src (id UInt8, arr Array(UInt8)) ENGINE = Memory;
INSERT INTO unnest_cross_src VALUES (1, [10, 20]), (2, [30]), (3, []);

SELECT id, unnest, arr FROM unnest_cross_src AS t CROSS JOIN unnest(t.arr) ORDER BY id, unnest;
SELECT id, x, arr FROM unnest_cross_src AS t CROSS JOIN unnest(t.arr) AS x ORDER BY id, x;
SELECT id, unnest FROM unnest_cross_src AS t, unnest(t.arr) ORDER BY id, unnest;
SELECT id, unnest FROM unnest_cross_src AS t CROSS JOIN UNNEST(t.arr) ORDER BY id, unnest;
SELECT id, unnest FROM unnest_cross_src AS t CROSS JOIN unnest([10, 20]) ORDER BY id, unnest;

DROP TABLE unnest_cross_src;
