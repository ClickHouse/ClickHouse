-- A later condition must read the prefix from before its timestamp group.
SELECT 'repeated groups',
    windowFunnel(100, 'strict_increase')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B', e = 'C', e = 'D')
FROM values('t UInt32, e String',
    (1, 'A'), (2, 'B'), (2, 'C'), (2, 'D'),
    (3, 'B'), (3, 'C'), (3, 'D'), (4, 'B'), (4, 'C'), (4, 'D'));

-- Same-group order evidence does not advance an actual prefix.
SELECT 'equal time',
    windowFunnel(100, 'strict_increase')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B', e = 'C', e = 'D')
FROM values('t UInt32, e String', (1, 'A'), (1, 'B'), (1, 'C'), (1, 'D'));

SELECT 'same-group new start',
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C')
FROM values('t UInt32, e String', (1, 'A'), (2, 'A'), (2, 'B'), (3, 'C'));

-- A newer start and B at the final timestamp must not overwrite the older valid AB prefix.
SELECT 'finite-window old prefix',
    windowFunnel(5, 'strict_increase')(t, e = 'A', e = 'B', e = 'C'),
    windowFunnel(5, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C'),
    windowFunnel(5, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B', e = 'C')
FROM values('t UInt32, e String', (1, 'A'), (2, 'B'), (5, 'A'), (6, 'B'), (6, 'C'));

-- A genuinely missing predecessor still stops the funnel unless reentry is enabled.
SELECT 'missing predecessor',
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B', e = 'C', e = 'D')
FROM values('t UInt32, e String', (1, 'A'), (2, 'B'), (3, 'D'), (4, 'C'), (5, 'D'));

-- An early exit must include the valid B staged in this group.
SELECT 'pending early exit',
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D')
FROM values('t UInt32, e String', (1, 'A'), (2, 'B'), (2, 'D'), (3, 'C'), (4, 'D'));

SELECT 'first event inside group',
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C'),
    windowFunnel(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B', e = 'C')
FROM values('t UInt32, e String', (1, 'A'), (1, 'C'), (2, 'B'), (3, 'C'));

-- C in the first group cannot protect D in the next group.
SELECT 'order evidence resets',
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D')
FROM values('t UInt32, e String',
    (1, 'A'), (1, 'B'), (1, 'C'), (2, 'D'), (3, 'B'), (4, 'C'), (5, 'D'));

-- The no-match sentinel sorts before conditions at the same timestamp.
SELECT 'sentinel after start',
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B'),
    windowFunnel(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B')
FROM values('t UInt32, e String', (1, 'A'), (2, 'X'), (2, 'B'));

SELECT 'sentinel before start',
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B'),
    windowFunnel(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B')
FROM values('t UInt32, e String', (1, 'X'), (1, 'A'), (2, 'B'));

-- Expired occurrences must not create reachable same-group order evidence.
SELECT 'expired A prefix',
    windowFunnel(3, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C')
FROM values('t UInt32, e String', (1, 'A'), (10, 'B'), (10, 'C'), (11, 'A'), (12, 'B'), (13, 'C'));

SELECT 'expired B prefix',
    windowFunnel(3, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D')
FROM values('t UInt32, e String',
    (1, 'A'), (2, 'B'), (10, 'C'), (10, 'D'), (11, 'A'), (12, 'B'), (13, 'C'), (14, 'D'));

-- Historical committed presence still prevents a missing-predecessor violation.
SELECT 'expired committed presence',
    windowFunnel(3, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C')
FROM values('t UInt32, e String', (1, 'A'), (2, 'B'), (10, 'C'), (11, 'A'), (12, 'B'), (13, 'C'));

SELECT 'sliding start',
    windowFunnel(1, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B')
FROM values('t UInt32, e String', (1, 'A'), (2, 'A'), (3, 'B'));

SELECT 'latest feasible prefix',
    windowFunnel(2, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C')
FROM values('t UInt32, e String', (1, 'A'), (2, 'B'), (3, 'A'), (4, 'B'), (5, 'C'));

SELECT 'empty',
    windowFunnel(100, 'strict_increase')(toUInt32(number), number = 0),
    windowFunnel(100, 'strict_increase', 'strict_order')(toUInt32(number), number = 0)
FROM numbers(0);

SELECT 'no match and singleton at zero',
    windowFunnel(100, 'strict_increase')(toUInt32(number), number = 1),
    windowFunnel(100, 'strict_increase', 'strict_order')(toUInt32(number), number = 1),
    windowFunnel(100, 'strict_increase')(toUInt32(number), number = 0),
    windowFunnel(100, 'strict_increase', 'strict_order')(toUInt32(number), number = 0)
FROM numbers(1);

SELECT 'zero window',
    windowFunnel(0, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B')
FROM values('t UInt32, e String', (0, 'A'), (1, 'B'));

-- Sorting must make the result independent of deterministic input permutations.
SELECT 'reversed groups',
    windowFunnel(100, 'strict_increase')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B', e = 'C', e = 'D')
FROM values('t UInt32, e String',
    (4, 'D'), (4, 'C'), (4, 'B'), (3, 'D'), (3, 'C'), (3, 'B'),
    (2, 'D'), (2, 'C'), (2, 'B'), (1, 'A'));

SELECT 'shuffled groups',
    windowFunnel(100, 'strict_increase')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B', e = 'C', e = 'D')
FROM values('t UInt32, e String',
    (3, 'C'), (1, 'A'), (4, 'B'), (2, 'D'), (3, 'B'),
    (4, 'D'), (2, 'B'), (4, 'C'), (2, 'C'), (3, 'D'));

-- Equal-time events are split between states; all mode parameters match on merge.
SELECT 'merged groups',
    windowFunnelMerge(100, 'strict_increase')(s_i),
    windowFunnelMerge(100, 'strict_increase', 'strict_order')(s_io),
    windowFunnelMerge(100, 'strict_increase', 'strict_order', 'allow_reentry')(s_ior)
FROM
(
    SELECT
        windowFunnelState(100, 'strict_increase')(t, e = 'A', e = 'B', e = 'C', e = 'D') AS s_i,
        windowFunnelState(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D') AS s_io,
        windowFunnelState(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'B', e = 'C', e = 'D') AS s_ior
    FROM values('part UInt8, t UInt32, e String',
        (0, 1, 'A'), (1, 2, 'B'), (0, 2, 'C'), (1, 2, 'D'),
        (0, 3, 'B'), (1, 3, 'C'), (0, 3, 'D'), (1, 4, 'B'), (0, 4, 'C'), (1, 4, 'D'))
    GROUP BY part
);

-- One input row may produce several conditions, still at only one timestamp.
SELECT 'overlapping conditions',
    windowFunnel(100, 'strict_increase')(t, e = 'A', e = 'BC', e = 'BC', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'BC', e = 'BC', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_order', 'allow_reentry')(t, e = 'A', e = 'BC', e = 'BC', e = 'D')
FROM values('t UInt32, e String',
    (1, 'A'), (2, 'BC'), (2, 'D'), (3, 'BC'), (3, 'D'), (4, 'BC'), (4, 'D'));

-- `strict_deduplication` and `strict_once` retain their existing evaluation paths.
SELECT 'legacy mode compatibility',
    windowFunnel(100, 'strict_increase', 'strict_deduplication')(t, e = 'A', e = 'B', e = 'C', e = 'D'),
    windowFunnel(100, 'strict_increase', 'strict_once')(t, e = 'A', e = 'B', e = 'C', e = 'D')
FROM values('t UInt32, e String',
    (1, 'A'), (2, 'B'), (2, 'C'), (2, 'D'),
    (3, 'B'), (3, 'C'), (3, 'D'), (4, 'B'), (4, 'C'), (4, 'D'));

-- Serialized state roundtrip exercises the new route without changing its layout.
SELECT 'serialized groups', finalizeAggregation(CAST(toString(s) AS
    AggregateFunction(windowFunnel(100, 'strict_increase', 'strict_order'), UInt32, UInt8, UInt8, UInt8, UInt8)))
FROM
(
    SELECT windowFunnelState(100, 'strict_increase', 'strict_order')(t, e = 'A', e = 'B', e = 'C', e = 'D') AS s
    FROM values('t UInt32, e String',
        (1, 'A'), (2, 'B'), (2, 'C'), (2, 'D'),
        (3, 'B'), (3, 'C'), (3, 'D'), (4, 'B'), (4, 'C'), (4, 'D'))
);

-- The final timestamp stages all 32 conditions, including the highest evidence bit and pending slot.
SELECT 'maximum conditions', windowFunnel(100, 'strict_increase', 'strict_order')
(
    number,
    number >= 0, number >= 1, number >= 2, number >= 3,
    number >= 4, number >= 5, number >= 6, number >= 7,
    number >= 8, number >= 9, number >= 10, number >= 11,
    number >= 12, number >= 13, number >= 14, number >= 15,
    number >= 16, number >= 17, number >= 18, number >= 19,
    number >= 20, number >= 21, number >= 22, number >= 23,
    number >= 24, number >= 25, number >= 26, number >= 27,
    number >= 28, number >= 29, number >= 30, number >= 31
)
FROM numbers(32);
