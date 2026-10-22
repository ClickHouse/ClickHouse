-- Tags: no-old-analyzer

-- What projection analysis decided is captured under `Projections` on the `ReadFromMergeTree` node,
-- the same data `EXPLAIN projections = 1` renders.
--
-- The projection has to cover the table only in part for there to be anything to report: when it
-- covers the query whole, the read of the table is replaced by a read of the projection and the
-- choice shows up as the step's name instead. So the projection is added between two inserts, which
-- leaves the first part without it.

DROP TABLE IF EXISTS t_05326;

CREATE TABLE t_05326 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_05326 SELECT number, number % 10 FROM numbers(50000);
ALTER TABLE t_05326 ADD PROJECTION p_by_b (SELECT b, count() GROUP BY b);
INSERT INTO t_05326 SELECT number, number % 10 FROM numbers(50000, 50000);

SET log_query_plans = 1;

SELECT b, count() FROM t_05326 WHERE '05326_projection' != '' GROUP BY b ORDER BY b FORMAT Null;

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;

-- The projection is named on the step that read the table, and its counters are present.
SELECT
    'projections',
    anyLast(JSONExtractString(node, 'Node Type')),
    anyLast(JSONExtractString(projection, 'Name')),
    anyLast(JSONHas(projection, 'Selected Parts')),
    anyLast(JSONHas(projection, 'Selected Marks')),
    anyLast(JSONHas(projection, 'Selected Rows'))
FROM
(
    SELECT
        arrayJoin(JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes')) AS node,
        arrayJoin(JSONExtractArrayRaw(node, 'Projections')) AS projection
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND position(query, '05326_projection') > 0
);

DROP TABLE t_05326;
