-- GROUP BY over zero input rows must return no rows, even when the key is constant at run time.

DROP TABLE IF EXISTS t_group_by_zero_rows;
CREATE TABLE t_group_by_zero_rows (id UInt64, kind Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_group_by_zero_rows SELECT number, ['intent', 'cause'] FROM numbers(1000);

-- ARRAY JOIN over empty arrays sends a zero-row block to the aggregator, and the empty IN set makes the key constant.
WITH facts AS
(
    SELECT t AS raw_key
    FROM (SELECT arrayFilter(x -> (x = 'request'), kind) AS arr FROM t_group_by_zero_rows) AS raw
    ARRAY JOIN arr AS t
)
SELECT key, count() AS volume
FROM (SELECT *, if(raw_key IN (SELECT raw_key FROM facts), toString(raw_key), 'OTHER') AS key FROM facts)
GROUP BY key;

-- An injective function of a constant must not be dropped from GROUP BY either.
SELECT materialize('OTHER') AS key, count() AS volume FROM numbers(0) GROUP BY key;
SELECT toString(materialize('OTHER')) AS key, count() AS volume FROM numbers(0) GROUP BY key;

-- A non-empty input still gives exactly one group.
SELECT materialize('OTHER') AS key, count() AS volume FROM numbers(5) GROUP BY key;

DROP TABLE t_group_by_zero_rows;
