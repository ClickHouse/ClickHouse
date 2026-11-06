-- Tags: no-parallel-replicas
-- StorageDictionary::read used to call IDictionary::read while the query was being planned.
-- For HASHED dictionaries that walks every key, even when a direct JOIN never reads the pipe.
-- Reading is now delayed until the pipe is pulled. Results must stay the same, and the direct
-- JOIN must still be picked.

DROP DICTIONARY IF EXISTS dim_dict;
DROP TABLE IF EXISTS dim;
DROP TABLE IF EXISTS fact;

CREATE TABLE dim (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO dim SELECT number, toString(number) FROM numbers(1000);

-- Keys 0..900 exist in the dictionary, 1050, 1200 and 1350 do not.
CREATE TABLE fact (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO fact SELECT number, number * 150 FROM numbers(10);

CREATE DICTIONARY dim_dict (id UInt64, name String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dim' DB currentDatabase()))
LAYOUT(HASHED())
LIFETIME(0);

SELECT 'Direct join is still chosen';
SELECT count() > 0 FROM (EXPLAIN actions = 1
    SELECT f.id, d.name FROM fact AS f LEFT JOIN dim_dict AS d ON f.k = d.id)
WHERE explain ILIKE '%Algorithm: DirectKeyValueJoin%';

SELECT 'Direct join result';
SELECT f.id, d.name FROM fact AS f LEFT JOIN dim_dict AS d ON f.k = d.id ORDER BY f.id
SETTINGS join_algorithm = 'direct', join_use_nulls = 0;

SELECT 'Reading the dictionary directly still works';
SELECT count(), sum(toUInt64(name)) FROM dim_dict;
SELECT id, name FROM dim_dict WHERE id IN (3, 500, 999) ORDER BY id;

SELECT 'Dictionary joined as the left side goes through a read';
SELECT count() FROM dim_dict AS d INNER JOIN fact AS f ON d.id = f.k
SETTINGS join_algorithm = 'hash';

DROP DICTIONARY dim_dict;
DROP TABLE fact;
DROP TABLE dim;
