DROP TABLE IF EXISTS t_in_order;
DROP TABLE IF EXISTS t_in_order_neg;
DROP TABLE IF EXISTS t_no_order;

CREATE TABLE t_in_order (timestamp Int64, id String) ENGINE = MergeTree ORDER BY timestamp;
CREATE TABLE t_in_order_neg (timestamp Int64, id String) ENGINE = MergeTree ORDER BY timestamp;
CREATE TABLE t_no_order (timestamp Int64, id String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_in_order VALUES (-50, 'a'), (50, 'b'), (60, 'c'), (100, 'd');
INSERT INTO t_in_order_neg VALUES (-100, 'a'), (-60, 'c'), (-50, 'b'), (50, 'd');
INSERT INTO t_no_order VALUES (-60, 'e'), (70, 'f');

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1;

SELECT '-- union of in-order branches, DESC';
WITH A AS
(
    SELECT * FROM t_in_order WHERE timestamp != 60
    UNION ALL
    SELECT * FROM t_in_order WHERE timestamp = 60
)
SELECT timestamp, id FROM A ORDER BY timestamp DESC LIMIT 10;

SELECT '-- union of in-order branches, DESC, virtual row per block';
WITH A AS
(
    SELECT * FROM t_in_order WHERE timestamp != 60
    UNION ALL
    SELECT * FROM t_in_order WHERE timestamp = 60
)
SELECT timestamp, id FROM A ORDER BY timestamp DESC LIMIT 10
SETTINGS read_in_order_use_virtual_row_per_block = 1;

SELECT '-- union of in-order branches with negative boundary, ASC';
WITH A AS
(
    SELECT * FROM t_in_order_neg WHERE timestamp != -60
    UNION ALL
    SELECT * FROM t_in_order_neg WHERE timestamp = -60
)
SELECT timestamp, id FROM A ORDER BY timestamp ASC LIMIT 10;

SELECT '-- union of in-order branches with negative boundary, ASC, virtual row per block';
WITH A AS
(
    SELECT * FROM t_in_order_neg WHERE timestamp != -60
    UNION ALL
    SELECT * FROM t_in_order_neg WHERE timestamp = -60
)
SELECT timestamp, id FROM A ORDER BY timestamp ASC LIMIT 10
SETTINGS read_in_order_use_virtual_row_per_block = 1;

SELECT '-- union with a branch sorted additionally, DESC';
WITH A AS
(
    SELECT * FROM t_in_order
    UNION ALL
    SELECT * FROM t_no_order
)
SELECT timestamp, id FROM A ORDER BY timestamp DESC LIMIT 10;

SELECT '-- union with a branch sorted additionally, ASC';
WITH A AS
(
    SELECT * FROM t_in_order
    UNION ALL
    SELECT * FROM t_no_order
)
SELECT timestamp, id FROM A ORDER BY timestamp ASC LIMIT 10;

DROP TABLE t_in_order;
DROP TABLE t_in_order_neg;
DROP TABLE t_no_order;
