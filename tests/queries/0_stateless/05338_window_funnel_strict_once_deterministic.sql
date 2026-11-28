-- Tags: no-parallel: false
-- windowFunnel with 'strict_deduplication', 'strict_once' should produce deterministic results
-- independent of input row arrival order or state merging.
-- See https://github.com/ClickHouse/ClickHouse/issues/124071

-- Case 1 & Case 2: Permutations of the issue reproducer rows
-- Permutation 1: R1, R2, R3
SELECT windowFunnel(1, 'strict_deduplication', 'strict_once')(t, c1, c2, c3)
FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
    (1, 2, 0, 1, 0), (2, 2, 1, 1, 1), (3, 3, 0, 0, 1)) ORDER BY i);

-- Permutation 2: R2, R1, R3
SELECT windowFunnel(1, 'strict_deduplication', 'strict_once')(t, c1, c2, c3)
FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
    (1, 2, 1, 1, 1), (2, 2, 0, 1, 0), (3, 3, 0, 0, 1)) ORDER BY i);

-- Permutation 3: R3, R1, R2
SELECT windowFunnel(1, 'strict_deduplication', 'strict_once')(t, c1, c2, c3)
FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
    (1, 3, 0, 0, 1), (2, 2, 0, 1, 0), (3, 2, 1, 1, 1)) ORDER BY i);

-- Case 3: Mode controls
-- control A: strict_once only (should reach 3 in both permutations)
SELECT windowFunnel(1, 'strict_once')(t, c1, c2, c3)
FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
    (1, 2, 0, 1, 0), (2, 2, 1, 1, 1), (3, 3, 0, 0, 1)) ORDER BY i);

SELECT windowFunnel(1, 'strict_once')(t, c1, c2, c3)
FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
    (1, 2, 1, 1, 1), (2, 2, 0, 1, 0), (3, 3, 0, 0, 1)) ORDER BY i);

-- control B: strict_deduplication only (should reach 2 in both permutations due to duplicate c2 at t=2)
SELECT windowFunnel(1, 'strict_deduplication')(t, c1, c2, c3)
FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
    (1, 2, 0, 1, 0), (2, 2, 1, 1, 1), (3, 3, 0, 0, 1)) ORDER BY i);

SELECT windowFunnel(1, 'strict_deduplication')(t, c1, c2, c3)
FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
    (1, 2, 1, 1, 1), (2, 2, 0, 1, 0), (3, 3, 0, 0, 1)) ORDER BY i);

-- Case 4: Genuine duplicate control with strict_deduplication and strict_once (should halt at 2)
SELECT windowFunnel(1, 'strict_deduplication', 'strict_once')(t, c1, c2, c3)
FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
    (1, 2, 0, 1, 0), (2, 2, 1, 1, 1), (3, 2, 0, 1, 0), (4, 3, 0, 0, 1)) ORDER BY i);

-- Case 5: Partial / merged state using windowFunnelState and windowFunnelMerge
SELECT windowFunnelMerge(1, 'strict_deduplication', 'strict_once')(state)
FROM (
    SELECT windowFunnelState(1, 'strict_deduplication', 'strict_once')(t, c1, c2, c3) AS state
    FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
        (1, 2, 0, 1, 0), (2, 2, 1, 1, 1)) ORDER BY i)
    UNION ALL
    SELECT windowFunnelState(1, 'strict_deduplication', 'strict_once')(t, c1, c2, c3) AS state
    FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
        (3, 3, 0, 0, 1)) ORDER BY i)
);

SELECT windowFunnelMerge(1, 'strict_deduplication', 'strict_once')(state)
FROM (
    SELECT windowFunnelState(1, 'strict_deduplication', 'strict_once')(t, c1, c2, c3) AS state
    FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
        (1, 2, 1, 1, 1), (2, 2, 0, 1, 0)) ORDER BY i)
    UNION ALL
    SELECT windowFunnelState(1, 'strict_deduplication', 'strict_once')(t, c1, c2, c3) AS state
    FROM (SELECT * FROM values('i UInt8, t DateTime, c1 UInt8, c2 UInt8, c3 UInt8',
        (3, 3, 0, 0, 1)) ORDER BY i)
);
