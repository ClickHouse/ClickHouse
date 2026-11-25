-- Tags: distributed
-- An `IN` subquery used only inside `INTERPOLATE` must be built (it used to fail with
-- LOGICAL_ERROR "Not-ready Set is passed as the second argument for function 'in'").

DROP TABLE IF EXISTS t_interpolate_in_subquery;

CREATE TABLE t_interpolate_in_subquery (id UInt32, g UInt16) ENGINE = MergeTree ORDER BY g;
INSERT INTO t_interpolate_in_subquery VALUES (10, 1), (20, 2), (30, 3), (40, 5);

SET prefer_localhost_replica = 0;

SELECT g, id FROM t_interpolate_in_subquery
ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + (id IN (SELECT number FROM numbers(3))));

-- A single shard runs the whole query, WITH FILL included, on the remote side.
SELECT g, id FROM remote('127.0.0.2', currentDatabase(), t_interpolate_in_subquery)
ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + (id IN (SELECT number FROM numbers(3))));

DROP TABLE t_interpolate_in_subquery;
