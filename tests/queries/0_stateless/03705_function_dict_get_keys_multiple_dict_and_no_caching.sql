DROP DICTIONARY IF EXISTS dict_products;
DROP DICTIONARY IF EXISTS dict_geo;
DROP TABLE IF EXISTS src_products;
DROP TABLE IF EXISTS src_geo;
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

CREATE TABLE src_geo
(
    country String,
    city String,
    timezone String,
    code UInt32
)
ENGINE = Memory;

INSERT INTO src_geo VALUES
    ('US', 'NYC',     'UTC-5', 10001),
    ('US', 'Chicago', 'UTC-6', 60601),
    ('FR', 'Paris',   'UTC+1', 75000),
    ('DE', 'Berlin',  'UTC+1', 10115),
    ('JP', 'Tokyo',   'UTC+9', 100000);

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

CREATE DICTIONARY dict_geo
(
    country String,
    city String,
    timezone String,
    code UInt32
)
PRIMARY KEY country, city
SOURCE(CLICKHOUSE(TABLE 'src_geo'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(COMPLEX_KEY_HASHED());

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
    target_brand,
    target_timezone,
    dictGetKeys('dict_products', 'category', target_category) AS product_ids_by_category,
    dictGetKeys('dict_products', 'brand',    target_brand)    AS product_ids_by_brand,
    dictGetKeys('dict_geo',      'timezone', target_timezone) AS country_city_by_tz
FROM inputs
ORDER BY target_category, target_brand, target_timezone;

SELECT dictGetKeys('dict_products', 'category', 'catA');

SELECT 'Composite value expressions';

SELECT dictGetKeys('dict_products', 'category', concat('cat', 'A'));

SELECT
    target_category,
    dictGetKeys('dict_geo', 'timezone', concat('UTC', substring(target_timezone, 4))) AS country_city_by_tz_expr
FROM inputs
ORDER BY target_category, target_brand, target_timezone;

SELECT 'Caching disabled';

SET max_reverse_dictionary_lookup_cache_size_bytes = 0;

SELECT
    target_category,
    target_brand,
    target_timezone,
    dictGetKeys('dict_products', 'category', target_category) AS product_ids_by_category,
    dictGetKeys('dict_products', 'brand',    target_brand)    AS product_ids_by_brand,
    dictGetKeys('dict_geo',      'timezone', target_timezone) AS country_city_by_tz
FROM inputs
ORDER BY target_category, target_brand, target_timezone;
