DROP TABLE IF EXISTS enums;
CREATE TABLE enums AS VALUES('x Enum8(\'hello\' = 0, \'world\' = 1, \'foo\' = -1), y String', ('hello', 'find me'), (0, 'and me'), (-1, 'also me'), ('world', 'don\'t find me'));
SELECT y FROM enums WHERE x IN (0, -1);
SELECT y FROM enums WHERE x IN ('hello', -1);
DROP TABLE enums;
