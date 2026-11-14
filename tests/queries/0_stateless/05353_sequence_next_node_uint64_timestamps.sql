-- sequenceNextNode orders UInt64 timestamps by their full value, also across a 2^32 boundary.
-- No table has a sequenceNextNode column: it could not be loaded back without the setting.

SET enable_funnel_functions = 1;

-- In memory: the events straddle 396 * 2^32 (forward, head), 2^32 (backward, tail) and the window path.
SELECT sequenceNextNode('forward', 'head')(t, page, page = 'A', page = 'A')
FROM values('t UInt64, page String', (1700807049215, 'A'), (1700807049217, 'B'));
SELECT sequenceNextNode('backward', 'tail')(t, page, page = 'C', page = 'C')
FROM values('t UInt64, page String', (4294967295, 'A'), (4294967296, 'B'), (4294967297, 'C'));
SELECT sequenceNextNode('forward', 'head')(t, page, page = 'A', page = 'A') OVER ()
FROM values('t UInt64, page String', (1700807049215, 'A'), (1700807049217, 'B'));

-- Only a UInt64 timestamp gives a versioned state.
SELECT toTypeName(sequenceNextNodeState('forward', 'head')(toUInt64(1), 'a', 1, 1));
SELECT toTypeName(sequenceNextNodeState('forward', 'head')(toDateTime(1), 'a', 1, 1));

-- The full timestamps survive the wire.
SELECT sequenceNextNodeMerge('forward', 'head')(s) FROM
(
    SELECT s FROM remote('127.0.0.2', view(
        SELECT sequenceNextNodeState('forward', 'head')(t, page, page = 'A', page = 'A') AS s
        FROM values('t UInt64, page String', (1700807049217, 'B'), (1700807049218, 'C'))))
    UNION ALL
    SELECT sequenceNextNodeState('forward', 'head')(t, page, page = 'A', page = 'A')
    FROM values('t UInt64, page String', (1700807049215, 'A'))
)
SETTINGS prefer_localhost_replica = 0;
SELECT sequenceNextNode('forward', 'head')(t, page, page = 'A', page = 'A')
FROM remote('127.0.0.{1,2}', view(
    SELECT * FROM values('t UInt64, page String', (1700807049215, 'A'), (1700807049217, 'B'))
    WHERE (t = 1700807049215) = (shardNum() = 1)));

-- The version 0 state of the rows below is the one older servers write: D(0) and A(1), ordered by the low 32 bits.
SELECT hex(CAST(sequenceNextNodeState('forward', 'head')(t, page, page = 'D', page = 'D')
    AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8)))
FROM values('t UInt64, page String', (4294967297, 'A'), (5, 'B'), (6, 'C'), (4294967296, 'D'));

-- A version 0 state of B and C (low 32 bits 2 and 3) merged with a full state of A, in both orders.
SELECT sequenceNextNodeMerge('forward', 'head')(s) FROM
(
    SELECT CAST(unhex('0102014202000000000000000000000000014303000000000000000000000000')
        AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8)) AS s
    UNION ALL
    SELECT CAST(s AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8))
    FROM remote('127.0.0.2', view(
        SELECT sequenceNextNodeState('forward', 'head')(t, page, page = 'A', page = 'A') AS s
        FROM values('t UInt64, page String', (1700807049217, 'A'))))
)
SETTINGS prefer_localhost_replica = 0;
SELECT sequenceNextNodeMerge('forward', 'head')(s) FROM
(
    SELECT CAST(s AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8)) AS s
    FROM remote('127.0.0.2', view(
        SELECT sequenceNextNodeState('forward', 'head')(t, page, page = 'A', page = 'A') AS s
        FROM values('t UInt64, page String', (1700807049217, 'A'))))
    UNION ALL
    SELECT CAST(unhex('0102014202000000000000000000000000014303000000000000000000000000')
        AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8))
)
SETTINGS prefer_localhost_replica = 0;

SELECT finalizeAggregation(CAST(unhex('020100')
    AS AggregateFunction(1, sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8))); -- { serverError INCORRECT_DATA }

-- A merge with version 0 data orders by the low 32 bits, whether the full state was sent over the wire or not.
SELECT sequenceNextNodeMerge('forward', 'head')(s) FROM
(
    SELECT CAST(s AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8)) AS s
    FROM remote('127.0.0.2', view(
        SELECT sequenceNextNodeState('forward', 'head')(t, page, page = 'D', page = 'D') AS s
        FROM values('t UInt64, page String', (4294967297, 'A'), (5, 'B'), (6, 'C'), (4294967296, 'D'))))
    UNION ALL
    SELECT CAST(unhex('0101014507000000000000000000000000')
        AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8))
)
SETTINGS prefer_localhost_replica = 0;
SELECT sequenceNextNodeMerge('forward', 'head')(s) FROM
(
    SELECT CAST(sequenceNextNodeState('forward', 'head')(t, page, page = 'D', page = 'D')
        AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8)) AS s
    FROM values('t UInt64, page String', (4294967297, 'A'), (5, 'B'), (6, 'C'), (4294967296, 'D'))
    UNION ALL
    SELECT CAST(unhex('0101014507000000000000000000000000')
        AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8))
);

-- Timestamps 2^32 apart with the same value: the earlier one is the head, whatever the order the states are merged in.
SELECT groupArray(k), sequenceNextNodeMerge('forward', 'head')(s) FROM
(
    SELECT k, s FROM
    (
        SELECT 1 AS k, CAST(unhex('0101014307000000000000000000000000')
            AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8)) AS s
        UNION ALL
        SELECT 2, CAST(sequenceNextNodeState('forward', 'head')(t, page, b, page = 'A')
            AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8))
        FROM values('t UInt64, page String, b UInt8', (4294967301, 'A', 0), (5, 'A', 1), (6, 'B', 0))
    )
    ORDER BY k
)
SETTINGS max_threads = 1;
SELECT groupArray(k), sequenceNextNodeMerge('forward', 'head')(s) FROM
(
    SELECT k, s FROM
    (
        SELECT 1 AS k, CAST(unhex('0101014307000000000000000000000000')
            AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8)) AS s
        UNION ALL
        SELECT 2, CAST(sequenceNextNodeState('forward', 'head')(t, page, b, page = 'A')
            AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8))
        FROM values('t UInt64, page String, b UInt8', (4294967301, 'A', 0), (5, 'A', 1), (6, 'B', 0))
    )
    ORDER BY k DESC
)
SETTINGS max_threads = 1;
SELECT sequenceNextNodeMerge('forward', 'head')(s) FROM
(
    SELECT CAST(s AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8)) AS s
    FROM remote('127.0.0.2', view(
        SELECT sequenceNextNodeState('forward', 'head')(t, page, b, page = 'A') AS s
        FROM values('t UInt64, page String, b UInt8', (4294967301, 'A', 0), (5, 'A', 1), (6, 'B', 0))))
    UNION ALL
    SELECT CAST(unhex('0101014307000000000000000000000000')
        AS AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8, UInt8))
)
SETTINGS prefer_localhost_replica = 0;

-- States that differ only in the high 32 bits of a timestamp hash differently, so the second set is not taken for the first.
SELECT
    materialize(arrayReduce('sequenceNextNodeState(\'forward\', \'head\')', [4294967297, 2]::Array(UInt64), ['A', 'B'], [1, 0]::Array(UInt8), [1, 0]::Array(UInt8)))
        IN (arrayReduce('sequenceNextNodeState(\'forward\', \'head\')', [1, 2]::Array(UInt64), ['A', 'B'], [1, 0]::Array(UInt8), [1, 0]::Array(UInt8))),
    materialize(arrayReduce('sequenceNextNodeState(\'forward\', \'head\')', [4294967297, 2]::Array(UInt64), ['A', 'B'], [1, 0]::Array(UInt8), [1, 0]::Array(UInt8)))
        IN (arrayReduce('sequenceNextNodeState(\'forward\', \'head\')', [4294967297, 2]::Array(UInt64), ['A', 'B'], [1, 0]::Array(UInt8), [1, 0]::Array(UInt8)));
