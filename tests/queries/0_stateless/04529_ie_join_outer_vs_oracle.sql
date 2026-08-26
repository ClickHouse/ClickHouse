-- Tags: no-old-analyzer

-- OUTER/SEMI/ANTI IEJoin on pseudo-random duplicate-heavy data with NULL keys, verified
-- against an oracle built from parts: the INNER pairs come from a cross join with a filter
-- (IEJoin disabled), the unmatched rows of each side are derived with NOT IN, padded by hand.
-- Multisets are compared with the arraySort(groupArray(...)) idiom; NULLs are mapped to
-- sentinels (-1 for keys, 0 for ids: real ids start at 1) so that tuple comparison is exact.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS tlo;
DROP TABLE IF EXISTS tro;
DROP TABLE IF EXISTS inner_oracle;

CREATE TABLE tlo (id UInt32, x Nullable(Int32), y Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tro (id UInt32, x Nullable(Int32), y Nullable(Int32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tlo SELECT
    number + 1,
    if(cityHash64(number, 1) % 8 = 0, NULL, toInt32(cityHash64(number, 2) % 30)),
    if(cityHash64(number, 3) % 8 = 0, NULL, toInt32(cityHash64(number, 4) % 30))
FROM numbers(500);

INSERT INTO tro SELECT
    number + 1,
    if(cityHash64(number, 5) % 8 = 0, NULL, toInt32(cityHash64(number, 6) % 30)),
    if(cityHash64(number, 7) % 8 = 0, NULL, toInt32(cityHash64(number, 8) % 30))
FROM numbers(300);

CREATE TABLE inner_oracle ENGINE = Memory AS
SELECT l.id AS lid, ifNull(l.x, -1) AS lx, ifNull(l.y, -1) AS ly,
       r.id AS rid, ifNull(r.x, -1) AS rx, ifNull(r.y, -1) AS ry
FROM tlo l, tro r WHERE l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'direct,parallel_hash,hash';

SELECT 'inner pairs', count() FROM inner_oracle;
SELECT 'matched left rows', count() FROM tlo WHERE id IN (SELECT lid FROM inner_oracle);
SELECT 'matched right rows', count() FROM tro WHERE id IN (SELECT rid FROM inner_oracle);

SELECT 'left', (
    SELECT arraySort(groupArray((lid, lx, ly, rid, rx, ry))) FROM (
        SELECT l.id AS lid, ifNull(l.x, -1) AS lx, ifNull(l.y, -1) AS ly,
               r.id AS rid, ifNull(r.x, -1) AS rx, ifNull(r.y, -1) AS ry
        FROM tlo l LEFT JOIN tro r ON l.x < r.x AND l.y > r.y
    )
) = (
    SELECT arraySort(groupArray((lid, lx, ly, rid, rx, ry))) FROM (
        SELECT lid, lx, ly, rid, rx, ry FROM inner_oracle
        UNION ALL
        SELECT id, ifNull(x, -1), ifNull(y, -1), toUInt32(0), toInt32(-1), toInt32(-1)
        FROM tlo WHERE id NOT IN (SELECT lid FROM inner_oracle)
    )
);

SELECT 'right', (
    SELECT arraySort(groupArray((lid, lx, ly, rid, rx, ry))) FROM (
        SELECT l.id AS lid, ifNull(l.x, -1) AS lx, ifNull(l.y, -1) AS ly,
               r.id AS rid, ifNull(r.x, -1) AS rx, ifNull(r.y, -1) AS ry
        FROM tlo l RIGHT JOIN tro r ON l.x < r.x AND l.y > r.y
    )
) = (
    SELECT arraySort(groupArray((lid, lx, ly, rid, rx, ry))) FROM (
        SELECT lid, lx, ly, rid, rx, ry FROM inner_oracle
        UNION ALL
        SELECT toUInt32(0), toInt32(-1), toInt32(-1), id, ifNull(x, -1), ifNull(y, -1)
        FROM tro WHERE id NOT IN (SELECT rid FROM inner_oracle)
    )
);

SELECT 'full', (
    SELECT arraySort(groupArray((lid, lx, ly, rid, rx, ry))) FROM (
        SELECT l.id AS lid, ifNull(l.x, -1) AS lx, ifNull(l.y, -1) AS ly,
               r.id AS rid, ifNull(r.x, -1) AS rx, ifNull(r.y, -1) AS ry
        FROM tlo l FULL JOIN tro r ON l.x < r.x AND l.y > r.y
    )
) = (
    SELECT arraySort(groupArray((lid, lx, ly, rid, rx, ry))) FROM (
        SELECT lid, lx, ly, rid, rx, ry FROM inner_oracle
        UNION ALL
        SELECT id, ifNull(x, -1), ifNull(y, -1), toUInt32(0), toInt32(-1), toInt32(-1)
        FROM tlo WHERE id NOT IN (SELECT lid FROM inner_oracle)
        UNION ALL
        SELECT toUInt32(0), toInt32(-1), toInt32(-1), id, ifNull(x, -1), ifNull(y, -1)
        FROM tro WHERE id NOT IN (SELECT rid FROM inner_oracle)
    )
);

-- SEMI/ANTI: the emitted rows of the driving side are exactly the matched/unmatched ids
SELECT 'semi', (
    SELECT arraySort(groupArray((id, x, y))) FROM (
        SELECT l.id AS id, ifNull(l.x, -1) AS x, ifNull(l.y, -1) AS y
        FROM tlo l LEFT SEMI JOIN tro r ON l.x < r.x AND l.y > r.y
    )
) = (
    SELECT arraySort(groupArray((id, ifNull(x, -1), ifNull(y, -1))))
    FROM tlo WHERE id IN (SELECT lid FROM inner_oracle)
);

SELECT 'anti', (
    SELECT arraySort(groupArray((id, x, y))) FROM (
        SELECT l.id AS id, ifNull(l.x, -1) AS x, ifNull(l.y, -1) AS y
        FROM tlo l LEFT ANTI JOIN tro r ON l.x < r.x AND l.y > r.y
    )
) = (
    SELECT arraySort(groupArray((id, ifNull(x, -1), ifNull(y, -1))))
    FROM tlo WHERE id NOT IN (SELECT lid FROM inner_oracle)
);

SELECT 'right semi', (
    SELECT arraySort(groupArray((id, x, y))) FROM (
        SELECT r.id AS id, ifNull(r.x, -1) AS x, ifNull(r.y, -1) AS y
        FROM tlo l RIGHT SEMI JOIN tro r ON l.x < r.x AND l.y > r.y
    )
) = (
    SELECT arraySort(groupArray((id, ifNull(x, -1), ifNull(y, -1))))
    FROM tro WHERE id IN (SELECT rid FROM inner_oracle)
);

SELECT 'right anti', (
    SELECT arraySort(groupArray((id, x, y))) FROM (
        SELECT r.id AS id, ifNull(r.x, -1) AS x, ifNull(r.y, -1) AS y
        FROM tlo l RIGHT ANTI JOIN tro r ON l.x < r.x AND l.y > r.y
    )
) = (
    SELECT arraySort(groupArray((id, ifNull(x, -1), ifNull(y, -1))))
    FROM tro WHERE id NOT IN (SELECT rid FROM inner_oracle)
);

DROP TABLE tlo;
DROP TABLE tro;
DROP TABLE inner_oracle;
