-- These test checks whether the used cache only valid per query.
-- Tests should fail if the used cache is shared across queries.

SELECT 'Cache Persistence Only Within A Single Query';

DROP DICTIONARY IF EXISTS colors;
DROP TABLE IF EXISTS dict_src;

CREATE TABLE dict_src
(
    id  UInt64,
    grp String
) ENGINE = Memory;

INSERT INTO dict_src VALUES (1, 'blue');

CREATE DICTIONARY colors
(
    id  UInt64,
    grp String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_src'))
LAYOUT(HASHED())
LIFETIME(0);

SELECT dictGetKeys('colors', 'grp', 'blue') AS keys
FROM numbers(1);

TRUNCATE TABLE dict_src;
INSERT INTO dict_src VALUES (2, 'blue');

SYSTEM RELOAD DICTIONARY colors;

SELECT 'After INSERT and RELOAD';

SELECT dictGetKeys('colors', 'grp', 'blue') AS keys
FROM numbers(1);

DROP DICTIONARY IF EXISTS colors;
DROP TABLE IF EXISTS dict_src;

SELECT 'Cache invalidation after dictionary reload with DELETE';

DROP DICTIONARY IF EXISTS dict_products;
DROP TABLE IF EXISTS src_products;
DROP TABLE IF EXISTS inputs;

CREATE TABLE src_products
(
    id UInt64,
    category String,
    brand String
)
ENGINE = Memory;

INSERT INTO src_products VALUES
    (1, 'catA', 'brandX'),
    (2, 'catA', 'brandY'),
    (3, 'catB', 'brandX'),
    (4, 'catC', 'brandZ'),
    (5, 'catB', 'brandZ');

CREATE DICTIONARY dict_products
(
    id UInt64,
    category String,
    brand String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src_products'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

CREATE TABLE inputs
(
    target_category String,
    target_brand String,
    target_timezone String
)
ENGINE = Memory;

INSERT INTO inputs VALUES
    ('catA', 'brandX', 'UTC+1'),
    ('catB', 'brandZ', 'UTC+9'),
    ('catX', 'brandX', 'UTC-6');

SELECT
    target_category,
    dictGetKeys('dict_products', 'category', target_category) AS product_ids_by_category_before
FROM inputs
ORDER BY target_category, target_brand, target_timezone;

ALTER TABLE src_products DELETE WHERE category = 'catA'
    SETTINGS mutations_sync = 1;

SYSTEM RELOAD DICTIONARY dict_products;

SELECT 'After DELETE mutation and RELOAD';

SELECT
    target_category,
    dictGetKeys('dict_products', 'category', target_category) AS product_ids_by_category_after
FROM inputs
ORDER BY target_category, target_brand, target_timezone;
