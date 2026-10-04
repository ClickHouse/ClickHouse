-- Tags: no-old-analyzer

-- Byte-cap accounting on every probe path: the residual mini-batch flush stops at the byte
-- cap too (not only at 1024 candidates or the row cap), unmatched LEFT/ANTI rows count the
-- interval-side default bytes they materialize, and a heavily fragmented index still
-- regroups the output correctly through the densified counting sort.
-- `cityHash64(p.t, i.payload)` depends on both sides, so the fat payload has to flow
-- through the join instead of being recomputed from the keys above it.

SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET max_threads = 4;

DROP TABLE IF EXISTS bj_cap_iv;
CREATE TABLE bj_cap_iv (lo Int64, hi Int64, tag UInt8, payload String) ENGINE = Memory;
INSERT INTO bj_cap_iv SELECT number, number + 200, 1, concat(toString(number), repeat('x', 10000)) FROM numbers(1000);

-- Pin the shape to the band join, so the caps below exercise its probe and not a fallback
SELECT 'plan', count() > 0 FROM (EXPLAIN
    SELECT count()
    FROM (SELECT number :: Int64 AS t FROM numbers(1000)) p
    JOIN bj_cap_iv i ON p.t >= i.lo AND p.t <= i.hi)
WHERE explain LIKE '%BandJoin%';

-- Fat interval rows under the byte cap: chunks must stay near cap / avg_row_bytes (~26
-- rows), far below the row cap
SELECT 'fat rows byte cap', max(bs) <= 64, count(), sum(h) > 0
FROM (
    SELECT blockSize() AS bs, cityHash64(p.t, i.payload) AS h
    FROM (SELECT number :: Int64 AS t FROM numbers(1000)) p
    JOIN bj_cap_iv i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS max_joined_block_size_rows = 65536, max_joined_block_size_bytes = 262144
);

-- The same under a residual ON conjunct: the mini-batch flush must respect the byte cap
-- too, not run to the 1024-candidate batch
SELECT 'residual byte cap', max(bs) <= 64, count(), sum(h) > 0
FROM (
    SELECT blockSize() AS bs, cityHash64(p.t, i.payload) AS h
    FROM (SELECT number :: Int64 AS t, 1 :: UInt8 AS sel FROM numbers(1000)) p
    LEFT JOIN bj_cap_iv i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag
    SETTINGS max_joined_block_size_rows = 65536, max_joined_block_size_bytes = 262144
);

-- ANTI emits every row padded: the interval-side default bytes count toward the cap, so
-- chunks stay near cap / (point + padded bytes), not cap / point bytes (~8192 rows)
SELECT 'anti padded byte cap', max(bs) <= 4096, count()
FROM (
    SELECT blockSize() AS bs, cityHash64(p.t, i.payload) AS h
    FROM (SELECT number :: Int64 AS t FROM numbers(100000)) p
    LEFT ANTI JOIN bj_cap_iv i ON p.t >= i.lo + 2000000 AND p.t <= i.hi + 3000000
    SETTINGS max_joined_block_size_rows = 65536, max_joined_block_size_bytes = 65536
);

-- Result parity of the capped residual query against the uncapped one
SELECT 'residual cap parity', (
    SELECT sum(cityHash64(p.t, i.lo, i.payload))
    FROM (SELECT number :: Int64 AS t, 1 :: UInt8 AS sel FROM numbers(1000)) p
    LEFT JOIN bj_cap_iv i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag
    SETTINGS max_joined_block_size_rows = 65536, max_joined_block_size_bytes = 262144
) = (
    SELECT sum(cityHash64(p.t, i.lo, i.payload))
    FROM (SELECT number :: Int64 AS t, 1 :: UInt8 AS sel FROM numbers(1000)) p
    LEFT JOIN bj_cap_iv i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag
    SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0
);

-- An index fragmented into more blocks than an output chunk holds rows (6250 blocks, row
-- cap 7): the regrouping densifies to the touched blocks; results must match the oracle
SELECT 'fragmented index vs oracle', (
    SELECT arraySort(groupArray((t2, lo2)))
    FROM (
        SELECT p.t AS t2, i.lo AS lo2
        FROM (SELECT (number * 4979) :: Int64 AS t FROM numbers(20)) p
        JOIN (SELECT number :: Int64 AS lo, (number + 3) :: Int64 AS hi FROM numbers(100000)) i
        ON p.t >= i.lo AND p.t <= i.hi
        SETTINGS max_block_size = 16, max_joined_block_size_rows = 7
    )
) = (
    SELECT arraySort(groupArray((t2, lo2)))
    FROM (
        SELECT p.t AS t2, i.lo AS lo2
        FROM (SELECT (number * 4979) :: Int64 AS t FROM numbers(20)) p
        JOIN (SELECT number :: Int64 AS lo, (number + 3) :: Int64 AS hi FROM numbers(100000)) i
        ON p.t >= i.lo AND p.t <= i.hi
    )
    SETTINGS join_algorithm = 'hash'
);

-- The same fragmentation with a residual LEFT (padded rows cross the densified path too)
SELECT 'fragmented residual vs oracle', (
    SELECT arraySort(groupArray((t2, lo2)))
    FROM (
        SELECT p.t AS t2, i.lo AS lo2
        FROM (SELECT (number * 4979) :: Int64 AS t, number % 2 :: UInt8 AS sel FROM numbers(20)) p
        LEFT JOIN (SELECT number :: Int64 AS lo, (number + 3) :: Int64 AS hi, number % 2 :: UInt8 AS tag FROM numbers(100000)) i
        ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag
        SETTINGS max_block_size = 16, max_joined_block_size_rows = 7
    )
) = (
    SELECT arraySort(groupArray((t2, lo2)))
    FROM (
        SELECT p.t AS t2, i.lo AS lo2
        FROM (SELECT (number * 4979) :: Int64 AS t, number % 2 :: UInt8 AS sel FROM numbers(20)) p
        LEFT JOIN (SELECT number :: Int64 AS lo, (number + 3) :: Int64 AS hi, number % 2 :: UInt8 AS tag FROM numbers(100000)) i
        ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag
    )
    SETTINGS join_algorithm = 'hash'
);

DROP TABLE bj_cap_iv;
