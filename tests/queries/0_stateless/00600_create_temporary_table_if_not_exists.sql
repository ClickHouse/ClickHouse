CREATE TEMPORARY TABLE IF NOT EXISTS temporary_table (column UInt32) ENGINE = Memory;
CREATE TEMPORARY TABLE IF NOT EXISTS temporary_table (column UInt32) ENGINE = Memory;
INSERT INTO temporary_table VALUES (1), (2), (3);
SELECT column FROM temporary_table ORDER BY column;