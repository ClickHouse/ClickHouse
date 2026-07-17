-- Tags: no-old-analyzer

-- OUTER IEJoin with residual ON conditions (conjuncts beyond the two inequalities, evaluated
-- inside the operator) on pseudo-random duplicate-heavy data with NULL values, verified against
-- an oracle built from parts: the INNER pairs come from a cross join with a filter over all
-- conditions, the unmatched rows of each side are derived with NOT IN, padded by hand.
-- Multisets are compared with the arraySort(groupArray(...)) idiom; NULLs are mapped to
-- sentinels (-1 for values, 0 for ids: real ids start at 1) so that tuple comparison is exact.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS tlr;
DROP TABLE IF EXISTS trr;
DROP TABLE IF EXISTS residual_oracle;

CREATE TABLE tlr (id UInt32, x Nullable(Int32), y Nullable(Int32), z Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE trr (id UInt32, x Nullable(Int32), y Nullable(Int32), z Nullable(Int32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tlr SELECT
    number + 1,
    if(cityHash64(number, 1) % 8 = 0, NULL, toInt32(cityHash64(number, 2) % 30)),
    if(cityHash64(number, 3) % 8 = 0, NULL, toInt32(cityHash64(number, 4) % 30)),
    if(cityHash64(number, 9) % 8 = 0, NULL, toInt32(cityHash64(number, 10) % 10))
FROM numbers(500);

INSERT INTO trr SELECT
    number + 1,
    if(cityHash64(number, 5) % 8 = 0, NULL, toInt32(cityHash64(number, 6) % 30)),
    if(cityHash64(number, 7) % 8 = 0, NULL, toInt32(cityHash64(number, 8) % 30)),
    if(cityHash64(number, 11) % 8 = 0, NULL, toInt32(cityHash64(number, 12) % 10))
FROM numbers(300);

-- The plan carries the residual inside the IEJoin step, not as a filter above it
SELECT 'routed', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tlr l LEFT JOIN trr r ON l.x < r.x AND l.y > r.y AND l.z <> r.z) WHERE explain LIKE '%IEJoin%';
SELECT 'residual in step', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tlr l LEFT JOIN trr r ON l.x < r.x AND l.y > r.y AND l.z <> r.z) WHERE explain LIKE '%Residual filter%';

CREATE TABLE residual_oracle ENGINE = Memory AS
SELECT l.id AS lid, ifNull(l.x, -1) AS lx, ifNull(l.y, -1) AS ly, ifNull(l.z, -1) AS lz,
       r.id AS rid, ifNull(r.x, -1) AS rx, ifNull(r.y, -1) AS ry, ifNull(r.z, -1) AS rz
FROM tlr l, trr r WHERE l.x < r.x AND l.y > r.y AND l.z <> r.z
SETTINGS join_algorithm = 'direct,parallel_hash,hash';

SELECT 'inner pairs', count() FROM residual_oracle;
SELECT 'matched left rows', count() FROM tlr WHERE id IN (SELECT lid FROM residual_oracle);
SELECT 'matched right rows', count() FROM trr WHERE id IN (SELECT rid FROM residual_oracle);

SELECT 'left', (
    SELECT arraySort(groupArray((lid, lx, ly, lz, rid, rx, ry, rz))) FROM (
        SELECT l.id AS lid, ifNull(l.x, -1) AS lx, ifNull(l.y, -1) AS ly, ifNull(l.z, -1) AS lz,
               r.id AS rid, ifNull(r.x, -1) AS rx, ifNull(r.y, -1) AS ry, ifNull(r.z, -1) AS rz
        FROM tlr l LEFT JOIN trr r ON l.x < r.x AND l.y > r.y AND l.z <> r.z
    )
) = (
    SELECT arraySort(groupArray((lid, lx, ly, lz, rid, rx, ry, rz))) FROM (
        SELECT lid, lx, ly, lz, rid, rx, ry, rz FROM residual_oracle
        UNION ALL
        SELECT id, ifNull(x, -1), ifNull(y, -1), ifNull(z, -1), toUInt32(0), toInt32(-1), toInt32(-1), toInt32(-1)
        FROM tlr WHERE id NOT IN (SELECT lid FROM residual_oracle)
    )
);

SELECT 'right', (
    SELECT arraySort(groupArray((lid, lx, ly, lz, rid, rx, ry, rz))) FROM (
        SELECT l.id AS lid, ifNull(l.x, -1) AS lx, ifNull(l.y, -1) AS ly, ifNull(l.z, -1) AS lz,
               r.id AS rid, ifNull(r.x, -1) AS rx, ifNull(r.y, -1) AS ry, ifNull(r.z, -1) AS rz
        FROM tlr l RIGHT JOIN trr r ON l.x < r.x AND l.y > r.y AND l.z <> r.z
    )
) = (
    SELECT arraySort(groupArray((lid, lx, ly, lz, rid, rx, ry, rz))) FROM (
        SELECT lid, lx, ly, lz, rid, rx, ry, rz FROM residual_oracle
        UNION ALL
        SELECT toUInt32(0), toInt32(-1), toInt32(-1), toInt32(-1), id, ifNull(x, -1), ifNull(y, -1), ifNull(z, -1)
        FROM trr WHERE id NOT IN (SELECT rid FROM residual_oracle)
    )
);

SELECT 'full', (
    SELECT arraySort(groupArray((lid, lx, ly, lz, rid, rx, ry, rz))) FROM (
        SELECT l.id AS lid, ifNull(l.x, -1) AS lx, ifNull(l.y, -1) AS ly, ifNull(l.z, -1) AS lz,
               r.id AS rid, ifNull(r.x, -1) AS rx, ifNull(r.y, -1) AS ry, ifNull(r.z, -1) AS rz
        FROM tlr l FULL JOIN trr r ON l.x < r.x AND l.y > r.y AND l.z <> r.z
    )
) = (
    SELECT arraySort(groupArray((lid, lx, ly, lz, rid, rx, ry, rz))) FROM (
        SELECT lid, lx, ly, lz, rid, rx, ry, rz FROM residual_oracle
        UNION ALL
        SELECT id, ifNull(x, -1), ifNull(y, -1), ifNull(z, -1), toUInt32(0), toInt32(-1), toInt32(-1), toInt32(-1)
        FROM tlr WHERE id NOT IN (SELECT lid FROM residual_oracle)
        UNION ALL
        SELECT toUInt32(0), toInt32(-1), toInt32(-1), toInt32(-1), id, ifNull(x, -1), ifNull(y, -1), ifNull(z, -1)
        FROM trr WHERE id NOT IN (SELECT rid FROM residual_oracle)
    )
);

-- A one-sided residual condition affects matching for outer kinds: rows failing it are
-- emitted padded, not dropped
SELECT 'one-sided left', (
    SELECT arraySort(groupArray((lid, rid))) FROM (
        SELECT l.id AS lid, r.id AS rid
        FROM tlr l LEFT JOIN trr r ON l.x < r.x AND l.y > r.y AND l.z > 5
    )
) = (
    SELECT arraySort(groupArray((lid, rid))) FROM (
        SELECT l.id AS lid, r.id AS rid FROM tlr l, trr r WHERE l.x < r.x AND l.y > r.y AND l.z > 5
        UNION ALL
        SELECT id, toUInt32(0) FROM tlr
        WHERE id NOT IN (SELECT l.id FROM tlr l, trr r WHERE l.x < r.x AND l.y > r.y AND l.z > 5)
    )
);

-- An expression over both sides as the residual
SELECT 'both-sides expression', (
    SELECT arraySort(groupArray((lid, rid))) FROM (
        SELECT l.id AS lid, r.id AS rid
        FROM tlr l FULL JOIN trr r ON l.x < r.x AND l.y > r.y AND l.z + r.z > 8
    )
) = (
    SELECT arraySort(groupArray((lid, rid))) FROM (
        SELECT l.id AS lid, r.id AS rid FROM tlr l, trr r WHERE l.x < r.x AND l.y > r.y AND l.z + r.z > 8
        UNION ALL
        SELECT id, toUInt32(0) FROM tlr
        WHERE id NOT IN (SELECT l.id FROM tlr l, trr r WHERE l.x < r.x AND l.y > r.y AND l.z + r.z > 8)
        UNION ALL
        SELECT toUInt32(0), id FROM trr
        WHERE id NOT IN (SELECT r.id FROM tlr l, trr r WHERE l.x < r.x AND l.y > r.y AND l.z + r.z > 8)
    )
);

-- With join_use_nulls the padded side is NULL; compare against the hash-executable shape
-- with an equality residual (both routes must agree)
SET join_use_nulls = 1;
SET join_algorithm = 'ie_join,hash';
SELECT 'join_use_nulls equality residual', (
    SELECT arraySort(groupArray((ifNull(l.id, 0), ifNull(r.id, 0), ifNull(r.z, -1))))
    FROM tlr l LEFT JOIN trr r ON l.z = r.z AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((ifNull(l.id, 0), ifNull(r.id, 0), ifNull(r.z, -1))))
    FROM tlr l LEFT JOIN trr r ON l.z = r.z AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);
SET join_use_nulls = 0;

DROP TABLE tlr;
DROP TABLE trr;
DROP TABLE residual_oracle;
