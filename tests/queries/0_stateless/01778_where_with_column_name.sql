DROP TABLE IF EXISTS ttt01778;
CREATE TABLE ttt01778 (`1` String, `2` INT) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO ttt01778 values('1',1),('2',2),('3',3);
select * from ttt01778 where 1=2; -- no server error
DROP TABLE ttt01778;
