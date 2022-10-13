DROP TABLE IF EXISTS group_by_all;

CREATE TABLE group_by_all
(
    a int,
    b int,
    ALL int
)
engine = Memory;

INSERT INTO group_by_all VALUES (1, 2, 3), (1, 2, 3), (2, 4, 4), (2, 5, 4), (3, 5, 5), (3, 5, 6);

SELECT a, COUNT(b) FROM group_by_all GROUP BY ALL;
SELECT 1, a, COUNT(b) FROM group_by_all GROUP BY ALL;
SELECT (a * 2) + 1, COUNT(b) FROM group_by_all GROUP BY ALL;
SELECT `ALL`, COUNT(b) FROM group_by_all GROUP BY `ALL`;
SELECT a, `ALL`, COUNT(b) FROM group_by_all GROUP BY ALL;
