-- Test that PruneArrayJoinColumnsPass correctly rewrites numeric tupleElement
-- indices after pruning unused nested subcolumns.
-- https://github.com/ClickHouse/ClickHouse/issues/100026

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (`n.a` Array(Int64), `n.b` Array(Int64), `n.c` Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES ([1, 2], [3, 4], [5, 6]);

-- Numeric tupleElement index > 1: n.b is at position 2, n.a (position 1) gets pruned.
-- The index must be rewritten from 2 to 1 after pruning.
SELECT tupleElement(n, 2) FROM t_nested ARRAY JOIN n ORDER BY tupleElement(n, 2);

-- Numeric tupleElement index 3: n.c is the last element, n.a and n.b get pruned.
-- The index must be rewritten from 3 to 1 after pruning.
SELECT tupleElement(n, 3) FROM t_nested ARRAY JOIN n ORDER BY tupleElement(n, 3);

-- Mixed numeric indices: positions 1 and 3. Position 2 (n.b) gets pruned.
-- Index 1 stays 1, index 3 must be rewritten to 2.
SELECT tupleElement(n, 1), tupleElement(n, 3) FROM t_nested ARRAY JOIN n ORDER BY tupleElement(n, 1);

DROP TABLE t_nested;
