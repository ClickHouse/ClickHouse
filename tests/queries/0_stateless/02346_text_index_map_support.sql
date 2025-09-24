-- Tags: no-fasttest, no-parallel-replicas

SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 0; --- for EXPLAIN indexes = 1 <query>

SELECT 'text index with mapKeys';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_keys_idx mapKeys(map) TYPE text(tokenizer = 'default'),
    INDEX map_fixed_keys_idx mapKeys(map_fixed) TYPE text(tokenizer = 'default'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES (0, {'K0':'V0', 'K1':'V1'}, {'K0':'V0', 'K1':'V1'});
INSERT INTO tab VALUES (1, {'K1':'V1', 'K2':'V2'}, {'K1':'V1', 'K2':'V2'});

SELECT '-- mapContainsKey support';

SELECT '---- query with String';

SELECT count() FROM tab WHERE mapContains(map, 'K0');
SELECT count() FROM tab WHERE mapContains(map, 'K1');
SELECT count() FROM tab WHERE mapContains(map, 'K2');
SELECT count() FROM tab WHERE mapContains(map, 'K3');

SELECT '---- query with FixedString';

SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K0', 2));
SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K1', 2));
SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K2', 2));
SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K3', 2));

SELECT '---- index analyzer with String';

SELECT 'key exists only in the first granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE mapContains(map, 'K0')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in the second granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE mapContains(map, 'K2')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in both granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE mapContains(map, 'K1')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE mapContains(map, 'K3')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '---- index analyzer with FixedString';

SELECT 'key exists only in the first granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K0', 2))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in the second granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K2', 2))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in both granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K1', 2))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K3', 2))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- arrayElement(map, key) support';

SELECT '---- query with String';

SELECT count() FROM tab WHERE map['K0'] = 'V0';
SELECT count() FROM tab WHERE map['K1'] = 'V1';
SELECT count() FROM tab WHERE map['K2'] = 'V2';
SELECT count() FROM tab WHERE map['K3'] = 'V3';

SELECT '---- query with FixedString';

SELECT count() FROM tab WHERE map_fixed[toFixedString('K0', 2)] = 'V0';
SELECT count() FROM tab WHERE map_fixed[toFixedString('K1', 2)] = 'V1';
SELECT count() FROM tab WHERE map_fixed[toFixedString('K2', 2)] = 'V2';
SELECT count() FROM tab WHERE map_fixed[toFixedString('K3', 2)] = 'V3';

SELECT '---- index analyzer with String';

SELECT 'key exists only in the first granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map['K0'] = 'V0'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in the second granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map['K2'] = 'V2'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in both granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map['K1'] = 'V1'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map['K3'] = 'V3'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '---- index analyzer with FixedString';

SELECT 'key exists only in the first granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map_fixed[toFixedString('K0', 2)] = 'V0'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in the second granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map_fixed[toFixedString('K2', 2)] = 'V2'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in both granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map_fixed[toFixedString('K1', 2)] = 'V1'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map_fixed[toFixedString('K3', 2)] = 'V3'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- has support';

SELECT '---- query with String';

SELECT count() FROM tab WHERE has(map, 'K0');
SELECT count() FROM tab WHERE has(map, 'K1');
SELECT count() FROM tab WHERE has(map, 'K2');
SELECT count() FROM tab WHERE has(map, 'K3');

SELECT '---- query with FixedString';

SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K0', 2));
SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K1', 2));
SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K2', 2));
SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K3', 2));

SELECT '---- index analyzer with String';

SELECT 'key exists only in the first granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(map, 'K0')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in the second granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(map, 'K2')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in both granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(map, 'K1')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(map, 'K3')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '---- index analyzer with FixedString';

SELECT 'key exists only in the first granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K0', 2))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in the second granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K2', 2))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in both granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K1', 2))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K3', 2))
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
DROP TABLE tab;

SELECT 'text index with mapValues';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    map Map(String, String),
    map_fixed Map(String, FixedString(2)),
    INDEX map_values_idx mapValues(map) TYPE text(tokenizer = 'default'),
    INDEX map_fixed_values_idx mapValues(map_fixed) TYPE text(tokenizer = 'default'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES (0, {'K0':'V0', 'K1':'V1'}, {'K0':'V0', 'K1':'V1'});
INSERT INTO tab VALUES (1, {'K1':'V1', 'K2':'V2'}, {'K1':'V1', 'K2':'V2'});

SELECT '-- arrayElement(map, key) support';

SELECT '---- query with String';

SELECT count() FROM tab WHERE map['K0'] = 'V0';
SELECT count() FROM tab WHERE map['K1'] = 'V1';
SELECT count() FROM tab WHERE map['K2'] = 'V2';
SELECT count() FROM tab WHERE map['K3'] = 'V3';

SELECT '---- query with FixedString';

SELECT count() FROM tab WHERE map_fixed['K0'] = toFixedString('V0', 2);
SELECT count() FROM tab WHERE map_fixed['K1'] = toFixedString('V1', 2);
SELECT count() FROM tab WHERE map_fixed['K2'] = toFixedString('V2', 2);
SELECT count() FROM tab WHERE map_fixed['K3'] = toFixedString('V3', 2);

SELECT '---- index analyzer with String';

SELECT 'key exists only in the first granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map['K0'] = 'V0'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in the second granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map['K2'] = 'V2'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in both granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map['K1'] = 'V1'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map['K3'] = 'V3'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '---- index analyzer with FixedString';

SELECT 'key exists only in the first granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map_fixed['K0'] = toFixedString('V0', 2)
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in the second granule';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map_fixed['K2'] = toFixedString('V2', 2)
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key exists only in both granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map_fixed['K1'] = toFixedString('V1', 2)
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'key does not exist in granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE map_fixed['K3'] = toFixedString('V3', 2)
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;
