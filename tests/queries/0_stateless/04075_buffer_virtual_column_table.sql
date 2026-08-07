-- Test _table virtual column for Buffer storage engine

DROP TABLE IF EXISTS buffer_virtual_dst;
DROP TABLE IF EXISTS buffer_virtual_buf;

CREATE TABLE buffer_virtual_dst (col1 String, col2 UInt64) ENGINE = MergeTree ORDER BY col1;
CREATE TABLE buffer_virtual_buf (col1 String, col2 UInt64) ENGINE = Buffer(currentDatabase(), buffer_virtual_dst, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);

-- Insert into destination directly so it's flushed
INSERT INTO buffer_virtual_dst VALUES ('a', 1), ('b', 2);
-- Insert into buffer (stays in memory due to high thresholds)
INSERT INTO buffer_virtual_buf VALUES ('c', 3), ('d', 4);

-- Data comes from both destination table and buffer shards
SELECT _table FROM buffer_virtual_buf ORDER BY col1;
SELECT col1, _table FROM buffer_virtual_buf ORDER BY col1;
SELECT *, _table FROM buffer_virtual_buf ORDER BY col1;

-- Buffer without destination table
DROP TABLE IF EXISTS buffer_no_dst;
CREATE TABLE buffer_no_dst (col1 String, col2 UInt64) ENGINE = Buffer('', '', 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);

INSERT INTO buffer_no_dst VALUES ('x', 10), ('y', 20);

SELECT _table FROM buffer_no_dst ORDER BY col1;
SELECT col1, _table FROM buffer_no_dst ORDER BY col1;

DROP TABLE buffer_no_dst;
DROP TABLE buffer_virtual_buf;
DROP TABLE buffer_virtual_dst;
