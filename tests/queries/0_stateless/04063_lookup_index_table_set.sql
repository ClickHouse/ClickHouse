SET enable_analyzer = 1;
SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS table_lookup_dim_only SYNC;
DROP TABLE IF EXISTS table_lookup_dim_extra SYNC;
DROP TABLE IF EXISTS table_lookup_fact_set SYNC;

CREATE TABLE table_lookup_dim_only
(
    id UInt64,
    LOOKUP INDEX idx_set (id) TYPE table_set
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE table_lookup_dim_extra
(
    id UInt64,
    tag String,
    LOOKUP INDEX idx_set (id) TYPE table_set
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE table_lookup_fact_set
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO table_lookup_dim_only VALUES (1), (3);
INSERT INTO table_lookup_dim_extra VALUES (1, 'a'), (3, 'b');
INSERT INTO table_lookup_fact_set VALUES (1, 'x'), (2, 'y'), (3, 'z');

SELECT id
FROM table_lookup_fact_set
WHERE id IN table_lookup_dim_only
ORDER BY id;

SELECT '--';

SELECT id
FROM table_lookup_fact_set
WHERE id IN (SELECT id FROM table_lookup_dim_extra)
ORDER BY id;

SELECT '--';

SELECT
    countIf(explain like '%ReadFromMergeTree (default.table_lookup_dim_only)%'),
    countIf(explain like '%ReadFromMergeTree (default.table_lookup_dim_extra)%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT id
    FROM table_lookup_fact_set
    WHERE id IN table_lookup_dim_only OR id IN (SELECT id FROM table_lookup_dim_extra)
    ORDER BY id
);

SELECT '--';

DROP TABLE table_lookup_dim_only SYNC;
DROP TABLE table_lookup_dim_extra SYNC;
DROP TABLE table_lookup_fact_set SYNC;

SET apply_mutations_on_fly = 1;
SET mutations_sync = 0;

DROP TABLE IF EXISTS table_lookup_dim_set_cache SYNC;
DROP TABLE IF EXISTS table_lookup_fact_set_cache SYNC;

CREATE TABLE table_lookup_dim_set_cache
(
    id UInt64,
    subid UInt64,
    LOOKUP INDEX idx_set (id, subid) TYPE table_set
)
ENGINE = MergeTree
ORDER BY (id, subid);

CREATE TABLE table_lookup_fact_set_cache
(
    id UInt64,
    subid UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY (id, subid);

INSERT INTO table_lookup_dim_set_cache VALUES (1, 1), (1, 2), (3, 1);
INSERT INTO table_lookup_fact_set_cache VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c'), (3, 1, 'd');

SELECT id, subid
FROM table_lookup_fact_set_cache
WHERE (id, subid) IN table_lookup_dim_set_cache
ORDER BY id, subid;

SELECT '--';

INSERT INTO table_lookup_dim_set_cache VALUES (2, 1);

SELECT id, subid
FROM table_lookup_fact_set_cache
WHERE (id, subid) IN table_lookup_dim_set_cache
ORDER BY id, subid;

SELECT '--';

ALTER TABLE table_lookup_dim_set_cache DELETE WHERE id = 1 AND subid = 2;

SELECT id, subid
FROM table_lookup_fact_set_cache
WHERE (id, subid) IN table_lookup_dim_set_cache
ORDER BY id, subid;

DROP TABLE table_lookup_dim_set_cache SYNC;
DROP TABLE table_lookup_fact_set_cache SYNC;
