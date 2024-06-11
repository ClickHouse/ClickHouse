DROP TABLE IF EXISTS alter_column_02126;
CREATE TABLE alter_column_02126 (a Int, x Int, y Int) ENGINE = MergeTree ORDER BY a;
SHOW CREATE TABLE alter_column_02126;
ALTER TABLE alter_column_02126 ALTER COLUMN x TYPE Float32;
SHOW CREATE TABLE alter_column_02126;
ALTER TABLE alter_column_02126 ALTER COLUMN x TYPE Float64, MODIFY COLUMN y Float32;
SHOW CREATE TABLE alter_column_02126;
ALTER TABLE alter_column_02126 MODIFY COLUMN y TYPE Float32; -- { clientError 62 }
ALTER TABLE alter_column_02126 ALTER COLUMN y Float32; -- { clientError 62 }
