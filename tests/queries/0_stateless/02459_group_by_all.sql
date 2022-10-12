DROP TABLE IF EXISTS group_by_all;

CREATE TABLE group_by_all
(
    a int,
    b int
)
engine = Memory;

INSERT INTO group_by_all VALUES (1, 2), (1, 2), (2, 4), (2, 5), (3, 5);

SELECT a, COUNT(b) FROM group_by_all GROUP BY ALL;
SELECT 1, a, COUNT(b) FROM group_by_all GROUP BY ALL;
SELECT (a * 2) + 1, COUNT(b) FROM group_by_all GROUP BY ALL;
