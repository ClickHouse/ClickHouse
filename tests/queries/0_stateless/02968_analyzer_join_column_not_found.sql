DROP TABLE IF EXISTS im;
CREATE TABLE im (id Int32, dd Int32) ENGINE = Memory();
INSERT INTO im VALUES (1, 1);

DROP TABLE IF EXISTS ts;
CREATE TABLE ts (tid Int32, id Int32) ENGINE = Memory();
INSERT INTO ts VALUES (1, 1);

SELECT *
FROM im AS m
INNER JOIN (
    SELECT tid, dd, t.id
    FROM im AS m
    INNER JOIN ts AS t ON m.id = t.id
) AS t ON m.dd = t.dd
;
