-- `LIMIT WITH TIES` runs after `LIMIT BY` and compares the `ORDER BY` columns of the last row with
-- its successors, so those columns have to reach it. A constant `ORDER BY` key was dropped from the
-- block before `LIMIT BY` - the projection above can produce the constant on its own - and the query
-- failed with `NOT_FOUND_COLUMN_IN_BLOCK`.

SELECT 'a constant order by key';
SELECT 1 FROM numbers(10) ORDER BY ALL LIMIT 1 BY number LIMIT 4 WITH TIES;
SELECT 1 FROM numbers(10) ORDER BY 1 LIMIT 1 BY number LIMIT 4 WITH TIES;
SELECT 1 FROM numbers(10) ORDER BY ALL LIMIT 1 BY number LIMIT 2 OFFSET 1 WITH TIES;

SELECT 'ties of an order by key that is not selected';
SELECT 1 FROM numbers(10) ORDER BY intDiv(number, 4) LIMIT 2 BY intDiv(number, 4) LIMIT 3 WITH TIES;

SELECT 'a table column';
DROP TABLE IF EXISTS t_ties_limit_by;
CREATE TABLE t_ties_limit_by (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ties_limit_by SELECT number % 3, number FROM numbers(9);

SELECT a FROM t_ties_limit_by ORDER BY a LIMIT 2 BY a LIMIT 3 WITH TIES;
SELECT 1 FROM t_ties_limit_by ORDER BY a LIMIT 2 BY a LIMIT 3 WITH TIES;

DROP TABLE t_ties_limit_by;
