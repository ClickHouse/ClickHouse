-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Test for issue #84508 (incorrect results caused by query condition cache when used with IN functions on non-const sets)

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;

CREATE TABLE tab1 (
    id UInt32 DEFAULT 0,
)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE tab2 (
    filter_id UInt32 DEFAULT 0
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO tab1 SELECT number AS id FROM numbers(1000000);

INSERT INTO tab2 VALUES(1);

-- Should return 1
SELECT count()
FROM tab1
WHERE id IN (
    SELECT filter_id
    FROM tab2
);

INSERT INTO tab2 VALUES(100001);

-- Should return 2
SELECT count()
FROM tab1
WHERE id IN (
    SELECT filter_id
    FROM tab2
);

DROP TABLE tab1;
DROP TABLE tab2;
