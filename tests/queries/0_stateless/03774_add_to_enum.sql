DROP TABLE IF EXISTS enum;

CREATE TABLE enum (x Enum16('One' = 1, 'Two', 'Three')) ENGINE = Memory;
INSERT INTO enum VALUES (0);
# SELECT * FROM enum ORDER BY x;
ALTER TABLE enum MODIFY COLUMN x addToEnum('Zero' = 0);
ALTER TABLE enum MODIFY COLUMN x addToEnum('Four' = 4);
INSERT INTO enum VALUES (4);
SELECT * FROM enum ORDER BY x;

DROP TABLE enum;
