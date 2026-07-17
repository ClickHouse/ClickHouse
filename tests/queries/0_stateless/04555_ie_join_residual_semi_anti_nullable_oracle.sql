-- Tags: no-old-analyzer

-- SEMI/ANTI IEJoin with residual ON conditions over Nullable inequality keys, verified against
-- a cross-join oracle on pseudo-random duplicate-heavy data. Rows with a NULL inequality key
-- never enter the sorted union, so they must always land on the ANTI side; the residual
-- (rarely-passing equality or a both-sides expression) exercises the mini-batch candidate scan.
-- The equality residual makes the join hash-executable, so ie_join must lead the priority list.

SET join_algorithm = 'ie_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS tnl;
DROP TABLE IF EXISTS tnr;

CREATE TABLE tnl (id UInt32, x Nullable(Int32), y Nullable(Int32), z Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tnr (id UInt32, x Nullable(Int32), y Nullable(Int32), z Nullable(Int32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tnl SELECT
    number + 1,
    if(cityHash64(number, 21) % 8 = 0, NULL, toInt32(cityHash64(number, 22) % 30)),
    if(cityHash64(number, 23) % 8 = 0, NULL, toInt32(cityHash64(number, 24) % 30)),
    if(cityHash64(number, 25) % 8 = 0, NULL, toInt32(cityHash64(number, 26) % 10))
FROM numbers(500);

INSERT INTO tnr SELECT
    number + 1,
    if(cityHash64(number, 27) % 8 = 0, NULL, toInt32(cityHash64(number, 28) % 30)),
    if(cityHash64(number, 29) % 8 = 0, NULL, toInt32(cityHash64(number, 30) % 30)),
    if(cityHash64(number, 31) % 8 = 0, NULL, toInt32(cityHash64(number, 32) % 10))
FROM numbers(300);

SELECT 'routed', count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM tnl l LEFT SEMI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z = r.z) WHERE explain LIKE '%IEJoin%';

-- Rarely-passing equality residual: many candidates are scanned before the first pass
SELECT 'semi eq', (
    SELECT arraySort(groupArray(id)) FROM (SELECT l.id AS id FROM tnl l LEFT SEMI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z = r.z)
) = (
    SELECT arraySort(groupArray(id)) FROM tnl WHERE id IN (SELECT l.id FROM tnl l, tnr r WHERE l.x < r.x AND l.y > r.y AND l.z = r.z)
);
SELECT 'anti eq', (
    SELECT arraySort(groupArray(id)) FROM (SELECT l.id AS id FROM tnl l LEFT ANTI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z = r.z)
) = (
    SELECT arraySort(groupArray(id)) FROM tnl WHERE id NOT IN (SELECT l.id FROM tnl l, tnr r WHERE l.x < r.x AND l.y > r.y AND l.z = r.z)
);

-- Both-sides expression residual (NULL z folds to no-match)
SELECT 'semi expr', (
    SELECT arraySort(groupArray(id)) FROM (SELECT l.id AS id FROM tnl l LEFT SEMI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z + r.z > 8)
) = (
    SELECT arraySort(groupArray(id)) FROM tnl WHERE id IN (SELECT l.id FROM tnl l, tnr r WHERE l.x < r.x AND l.y > r.y AND l.z + r.z > 8)
);
SELECT 'anti expr', (
    SELECT arraySort(groupArray(id)) FROM (SELECT l.id AS id FROM tnl l LEFT ANTI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z + r.z > 8)
) = (
    SELECT arraySort(groupArray(id)) FROM tnl WHERE id NOT IN (SELECT l.id FROM tnl l, tnr r WHERE l.x < r.x AND l.y > r.y AND l.z + r.z > 8)
);

-- NULL-keyed left rows never match: they must all be on the ANTI side and none on the SEMI side
SELECT 'null keys stay anti', (
    SELECT count() FROM (SELECT l.id AS id FROM tnl l LEFT SEMI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z = r.z) WHERE id IN (SELECT id FROM tnl WHERE x IS NULL OR y IS NULL)
) = 0 AND (
    SELECT count() FROM (SELECT l.id AS id FROM tnl l LEFT ANTI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z = r.z) WHERE id IN (SELECT id FROM tnl WHERE x IS NULL OR y IS NULL)
) = (
    SELECT count() FROM tnl WHERE x IS NULL OR y IS NULL
);

-- The SEMI row's right-side companion must itself satisfy all conditions
SELECT 'semi pairs valid', count() = countIf(l.x < r.x AND l.y > r.y AND l.z = r.z)
FROM tnl l LEFT SEMI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z = r.z;

-- RIGHT SEMI/ANTI: the swapped mirror with the residual's sides flipped
SELECT 'right semi eq', (
    SELECT arraySort(groupArray(id)) FROM (SELECT r.id AS id FROM tnl l RIGHT SEMI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z = r.z)
) = (
    SELECT arraySort(groupArray(id)) FROM tnr WHERE id IN (SELECT r.id FROM tnl l, tnr r WHERE l.x < r.x AND l.y > r.y AND l.z = r.z)
);
SELECT 'right anti eq', (
    SELECT arraySort(groupArray(id)) FROM (SELECT r.id AS id FROM tnl l RIGHT ANTI JOIN tnr r ON l.x < r.x AND l.y > r.y AND l.z = r.z)
) = (
    SELECT arraySort(groupArray(id)) FROM tnr WHERE id NOT IN (SELECT r.id FROM tnl l, tnr r WHERE l.x < r.x AND l.y > r.y AND l.z = r.z)
);

DROP TABLE tnl;
DROP TABLE tnr;
