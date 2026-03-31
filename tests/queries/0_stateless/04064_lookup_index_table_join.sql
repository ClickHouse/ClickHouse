SET enable_analyzer = 1;
SET join_algorithm = 'direct,hash';

DROP TABLE IF EXISTS table_lookup_dim_join SYNC;
DROP TABLE IF EXISTS table_lookup_fact_join SYNC;

CREATE TABLE table_lookup_dim_join
(
    id UInt64,
    subid UInt64,
    value String,
    LOOKUP INDEX idx_join (id, subid) TYPE table_join
)
ENGINE = MergeTree
ORDER BY (id, subid);

CREATE TABLE table_lookup_fact_join
(
    id UInt64,
    subid UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY (id, subid);

INSERT INTO table_lookup_dim_join VALUES (1, 1, 'a'), (1, 2, 'b'), (3, 1, 'c'), (4, 1, 'd1'), (4, 1, 'd2');
INSERT INTO table_lookup_fact_join VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z'), (3, 1, 'w'), (4, 1, 'q');

SELECT id, subid, value
FROM table_lookup_fact_join
LEFT ANY JOIN table_lookup_dim_join
    ON table_lookup_fact_join.id = table_lookup_dim_join.id
    AND table_lookup_fact_join.subid = table_lookup_dim_join.subid
ORDER BY id, subid;

SELECT '--';

SELECT id, subid, value
FROM table_lookup_fact_join
INNER ALL JOIN table_lookup_dim_join
    ON table_lookup_fact_join.id = table_lookup_dim_join.id
    AND table_lookup_fact_join.subid = table_lookup_dim_join.subid
WHERE table_lookup_fact_join.id = 4
ORDER BY value;

SELECT '--';

SELECT
    countIf(explain like '%Algorithm: DirectKeyValueJoin%'),
    countIf(explain like '%ReadFromMergeTree (%table_lookup_dim_join%)%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT id, subid, value
    FROM table_lookup_fact_join
    LEFT ANY JOIN table_lookup_dim_join
        ON table_lookup_fact_join.id = table_lookup_dim_join.id
        AND table_lookup_fact_join.subid = table_lookup_dim_join.subid
    ORDER BY id, subid
);

SELECT '--';

DROP TABLE table_lookup_dim_join SYNC;
DROP TABLE table_lookup_fact_join SYNC;

SET join_use_nulls = 1;
SET apply_mutations_on_fly = 1;
SET mutations_sync = 0;

DROP TABLE IF EXISTS table_lookup_dim_join_cache SYNC;
DROP TABLE IF EXISTS table_lookup_fact_join_cache SYNC;

CREATE TABLE table_lookup_dim_join_cache
(
    id UInt64,
    subid UInt64,
    value String,
    LOOKUP INDEX idx_join (id, subid) TYPE table_join
)
ENGINE = MergeTree
ORDER BY (id, subid);

CREATE TABLE table_lookup_fact_join_cache
(
    id UInt64,
    subid UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY (id, subid);

INSERT INTO table_lookup_dim_join_cache VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
INSERT INTO table_lookup_fact_join_cache VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z'), (4, 1, 'w');

SELECT id, subid, toTypeName(value), value
FROM table_lookup_fact_join_cache
LEFT ANY JOIN table_lookup_dim_join_cache
    ON table_lookup_fact_join_cache.id = table_lookup_dim_join_cache.id
    AND table_lookup_fact_join_cache.subid = table_lookup_dim_join_cache.subid
ORDER BY id, subid;

SELECT '--';

INSERT INTO table_lookup_dim_join_cache VALUES (4, 1, 'd');

SELECT id, subid, value
FROM table_lookup_fact_join_cache
LEFT ANY JOIN table_lookup_dim_join_cache
    ON table_lookup_fact_join_cache.id = table_lookup_dim_join_cache.id
    AND table_lookup_fact_join_cache.subid = table_lookup_dim_join_cache.subid
ORDER BY id, subid;

SELECT '--';

ALTER TABLE table_lookup_dim_join_cache UPDATE value = 'b2' WHERE id = 1 AND subid = 2;

SELECT id, subid, value
FROM table_lookup_fact_join_cache
LEFT ANY JOIN table_lookup_dim_join_cache
    ON table_lookup_fact_join_cache.id = table_lookup_dim_join_cache.id
    AND table_lookup_fact_join_cache.subid = table_lookup_dim_join_cache.subid
ORDER BY id, subid;

SELECT '--';

ALTER TABLE table_lookup_dim_join_cache DELETE WHERE id = 2 AND subid = 1;

SELECT id, subid
FROM table_lookup_fact_join_cache
LEFT SEMI JOIN table_lookup_dim_join_cache
    ON table_lookup_fact_join_cache.id = table_lookup_dim_join_cache.id
    AND table_lookup_fact_join_cache.subid = table_lookup_dim_join_cache.subid
ORDER BY id, subid;

SELECT '--';

SELECT id, subid
FROM table_lookup_fact_join_cache
LEFT ANTI JOIN table_lookup_dim_join_cache
    ON table_lookup_fact_join_cache.id = table_lookup_dim_join_cache.id
    AND table_lookup_fact_join_cache.subid = table_lookup_dim_join_cache.subid
ORDER BY id, subid;

SELECT '--';

ALTER TABLE table_lookup_dim_join_cache ADD COLUMN extra String DEFAULT 'extra';

SELECT id, subid, value, extra
FROM table_lookup_fact_join_cache
LEFT ANY JOIN table_lookup_dim_join_cache
    ON table_lookup_fact_join_cache.id = table_lookup_dim_join_cache.id
    AND table_lookup_fact_join_cache.subid = table_lookup_dim_join_cache.subid
ORDER BY id, subid;

DROP TABLE table_lookup_dim_join_cache SYNC;
DROP TABLE table_lookup_fact_join_cache SYNC;
