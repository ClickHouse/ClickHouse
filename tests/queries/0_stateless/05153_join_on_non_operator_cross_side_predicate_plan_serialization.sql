-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `JoinStepLogical::serializeSettings` asks `JoinOperator` about the shape of the join to decide whether
-- the temporary-file settings and the spill-codec opt-in have to go on the wire. A condition of the `ON`
-- expression that spans both inputs but is not one of the binary join operators - `startsWith` here - is
-- reported by `asBinaryPredicate` as `Unknown` with null operands, and inspecting those operands threw
-- `Cannot get data for JoinActionRef` for an otherwise valid join. Such a condition becomes part of the
-- mixed join expression, so it is classified without looking at the operands.

DROP TABLE IF EXISTS t_left_05153;
DROP TABLE IF EXISTS t_right_05153;

CREATE TABLE t_left_05153 (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_right_05153 (k UInt64, p String) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_left_05153 VALUES (1, 'abcdef'), (2, 'ghijkl'), (3, 'mnopqr');
INSERT INTO t_right_05153 VALUES (1, 'abc'), (2, 'xyz'), (3, 'mno');

-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0, as the other
-- `make_distributed_plan` tests do.
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
    max_rows_to_group_by = 0;

SELECT l.k, l.s, r.p
FROM t_left_05153 AS l INNER JOIN t_right_05153 AS r ON l.k = r.k AND startsWith(l.s, r.p)
ORDER BY l.k;

SELECT l.k, r.p
FROM t_left_05153 AS l LEFT JOIN t_right_05153 AS r ON l.k = r.k AND startsWith(l.s, r.p)
ORDER BY l.k;

DROP TABLE t_left_05153;
DROP TABLE t_right_05153;
