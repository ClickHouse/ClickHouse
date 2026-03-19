-- Tags: no-parallel-replicas

-- Tests that text indexes can be build on and used with Map columns.

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;

SELECT 'Function mapKeys';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_keys_idx mapKeys(map) TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX map_fixed_keys_idx mapKeys(map_fixed) TYPE text(tokenizer = 'splitByNonAlpha'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES (0, {'K0':'V0', 'K1':'V1'}, {'K0':'V0', 'K1':'V1'});
INSERT INTO tab VALUES (1, {'K1':'V1', 'K2':'V2'}, {'K1':'V1', 'K2':'V2'});

SELECT '-- mapContains support';

SELECT '-- -- query with String';

SELECT count() FROM tab WHERE mapContains(map, 'K0');
SELECT count() FROM tab WHERE mapContains(map, 'K1');
SELECT count() FROM tab WHERE mapContains(map, 'K2');
SELECT count() FROM tab WHERE mapContains(map, 'K3');

SELECT '-- -- query with FixedString';

SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K0', 2));
SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K1', 2));
SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K2', 2));
SELECT count() FROM tab WHERE mapContains(map_fixed, toFixedString('K3', 2));

SELECT '-- -- Check that the text index actually gets used (String)';

DROP VIEW IF EXISTS explain_index_mapContains;
CREATE VIEW explain_index_mapContains AS (
    SELECT trimLeft(explain) AS explain FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM tab WHERE (
            CASE
                WHEN {use_idx_fixed:boolean} = 1 THEN mapContains(map_fixed, {filter:FixedString(2)})
                ELSE mapContains(map, {filter:String})
            END
        )
    )
    WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
    LIMIT 2, 3
);

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_mapContains(use_idx_fixed = 0, filter = 'K0');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_mapContains(use_idx_fixed = 0, filter = 'K2');

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_mapContains(use_idx_fixed = 0, filter = 'K1');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_mapContains(use_idx_fixed = 0, filter = 'K3');

SELECT '-- -- Check that the text index actually gets used (FixedString)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_mapContains(use_idx_fixed = 1, filter = toFixedString('K0', 2));

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_mapContains(use_idx_fixed = 1, filter = toFixedString('K2', 2));

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_mapContains(use_idx_fixed = 1, filter = toFixedString('K1', 2));

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_mapContains(use_idx_fixed = 1, filter = toFixedString('K3', 2));

SELECT '-- operator[] support';

SELECT '-- -- query with String';

SELECT count() FROM tab WHERE map['K0'] = 'V0';
SELECT count() FROM tab WHERE map['K1'] = 'V1';
SELECT count() FROM tab WHERE map['K2'] = 'V2';
SELECT count() FROM tab WHERE map['K3'] = 'V3';

SELECT '-- -- query with FixedString';

SELECT count() FROM tab WHERE map_fixed[toFixedString('K0', 2)] = 'V0';
SELECT count() FROM tab WHERE map_fixed[toFixedString('K1', 2)] = 'V1';
SELECT count() FROM tab WHERE map_fixed[toFixedString('K2', 2)] = 'V2';
SELECT count() FROM tab WHERE map_fixed[toFixedString('K3', 2)] = 'V3';

SELECT '-- -- Check that the text index actually gets used (String)';

DROP VIEW IF EXISTS explain_index_equals;
CREATE VIEW explain_index_equals AS (
    SELECT trimLeft(explain) AS explain FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM tab WHERE (
            CASE
                WHEN {use_idx_fixed:boolean} = 1 THEN map_fixed[{filter:FixedString(2)}] = {value:String}
                ELSE map[{filter:String}] = {value:String}
            END
        )
    )
    WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
    LIMIT 2, 3
);

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_equals(use_idx_fixed = 0, filter = 'K0', value = 'V3');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_equals(use_idx_fixed = 0, filter = 'K2', value = 'V3');

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_equals(use_idx_fixed = 0, filter = 'K1', value = 'V3');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_equals(use_idx_fixed = 0, filter = 'K3', value = 'V3');

SELECT '-- -- Check that the text index actually gets used (FixedString)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_equals(use_idx_fixed = 1, filter = 'K0', value = 'V3');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_equals(use_idx_fixed = 1, filter = 'K2', value = 'V3');

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_equals(use_idx_fixed = 1, filter = 'K1', value = 'V3');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_equals(use_idx_fixed = 1, filter = 'K3', value = 'V3');

SELECT '-- has support';

SELECT '-- -- query with String';

SELECT count() FROM tab WHERE has(map, 'K0');
SELECT count() FROM tab WHERE has(map, 'K1');
SELECT count() FROM tab WHERE has(map, 'K2');
SELECT count() FROM tab WHERE has(map, 'K3');

SELECT '-- -- query with FixedString';

SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K0', 2));
SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K1', 2));
SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K2', 2));
SELECT count() FROM tab WHERE has(map_fixed, toFixedString('K3', 2));

SELECT '-- -- Check that the text index actually gets used (String)';

DROP VIEW IF EXISTS explain_index_has;
CREATE VIEW explain_index_has AS (
    SELECT trimLeft(explain) AS explain FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM tab WHERE (
            CASE
                WHEN {use_idx_fixed:boolean} = 1 THEN has(map_fixed, {filter:FixedString(2)})
                ELSE has(map, {filter:String})
            END
        )
    )
    WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
    LIMIT 2, 3
);

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'K0');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'K2');

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'K1');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'K3');

SELECT '-- -- Check that the text index actually gets used (FixedString)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = toFixedString('K0', 2));

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = toFixedString('K2', 2));

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = toFixedString('K1', 2));

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = toFixedString('K3', 2));

SELECT '-- hasAnyTokens support';

SELECT '-- -- query with String';

SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map), 'K0 K1');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map), 'K1 K2');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map), 'K2 K3');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map), 'K3 K4');

SELECT '-- -- query with FixedString';

SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map_fixed), 'K0 K1');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map_fixed), 'K1 K2');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map_fixed), 'K2 K3');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(map_fixed), 'K3 K4');

DROP VIEW IF EXISTS explain_index_has_any_tokens;
CREATE VIEW explain_index_has_any_tokens AS (
    SELECT trimLeft(explain) AS explain FROM (
        EXPLAIN indexes=1
        SELECT count() FROM tab WHERE (
            CASE 
                WHEN {use_idx_fixed:boolean} = 1 THEN hasAnyTokens(mapKeys(map_fixed), {filter:String})
                ELSE hasAnyTokens(mapKeys(map), {filter:String})
            END
        )
    )
    WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
    LIMIT 2, 3
);

SELECT '-- -- Check that the text index actually gets used (String)';

SELECT '-- -- -- keys exist in both granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 0, filter = 'K0 K1');

SELECT '-- -- -- keys exist in both granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 0, filter = 'K1 K2');

SELECT '-- -- -- keys exist only in the first granule';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 0, filter = 'K2 K3');

SELECT '-- -- -- keys do not exist in granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 0, filter = 'K3 K4');

SELECT '-- -- Check that the text index actually gets used (FixedString)';

SELECT '-- -- -- keys exist in both granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 1, filter = 'K0 K1');

SELECT '-- -- -- keys exist in both granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 1, filter = 'K1 K2');

SELECT '-- -- -- keys exist only in the first granule';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 1, filter = 'K2 K3');

SELECT '-- -- -- keys do not exist in granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 1, filter = 'K3 K4');

SELECT '-- hasAllTokens support';

SELECT '-- -- query with String';

SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), 'K0 K1');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), 'K1 K2');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), 'K2 K3');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), 'K3 K4');

SELECT '-- -- query with FixedString';

SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map_fixed), 'K0 K1');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map_fixed), 'K1 K2');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map_fixed), 'K2 K3');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map_fixed), 'K3 K4');

DROP VIEW IF EXISTS explain_index_has_all_tokens;
CREATE VIEW explain_index_has_all_tokens AS (
    SELECT trimLeft(explain) AS explain FROM (
        EXPLAIN indexes=1
        SELECT count() FROM tab WHERE (
            CASE 
                WHEN {use_idx_fixed:boolean} = 1 THEN hasAllTokens(mapKeys(map_fixed), {filter:String})
                ELSE hasAllTokens(mapKeys(map), {filter:String})
            END
        )
    )
    WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
    LIMIT 2, 3
);

SELECT '-- -- Check that the text index actually gets used (String)';

SELECT '-- -- -- keys exist in the first granule';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 0, filter = 'K0 K1');

SELECT '-- -- -- keys exist in the second granule';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 0, filter = 'K1 K2');

SELECT '-- -- -- keys do not exist in granules';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 0, filter = 'K2 K3');

SELECT '-- -- -- keys do not exist in granules';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 0, filter = 'K3 K4');

SELECT '-- -- Check that the text index actually gets used (FixedString)';

SELECT '-- -- -- keys exist in the first granule';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 1, filter = 'K0 K1');

SELECT '-- -- -- keys exist in the second granule';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 1, filter = 'K1 K2');

SELECT '-- -- -- keys do not exist in granules';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 1, filter = 'K2 K3');

SELECT '-- -- -- keys do not exist in granules';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 1, filter = 'K3 K4');

DROP VIEW explain_index_has_any_tokens;
DROP VIEW explain_index_has_all_tokens;

SELECT 'Function mapValues';

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt32,
    map Map(String, String),
    map_fixed Map(String, FixedString(2)),
    INDEX map_values_idx mapValues(map) TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX map_fixed_values_idx mapValues(map_fixed) TYPE text(tokenizer = 'splitByNonAlpha'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES (0, {'K0':'V0', 'K1':'V1'}, {'K0':'V0', 'K1':'V1'});
INSERT INTO tab VALUES (1, {'K1':'V1', 'K2':'V2'}, {'K1':'V1', 'K2':'V2'});

SELECT '-- operator[] support';

SELECT '-- -- query with String';

SELECT count() FROM tab WHERE map['K0'] = 'V0';
SELECT count() FROM tab WHERE map['K1'] = 'V1';
SELECT count() FROM tab WHERE map['K2'] = 'V2';
SELECT count() FROM tab WHERE map['K3'] = 'V3';

SELECT '-- -- query with FixedString';

SELECT count() FROM tab WHERE map_fixed['K0'] = toFixedString('V0', 2);
SELECT count() FROM tab WHERE map_fixed['K1'] = toFixedString('V1', 2);
SELECT count() FROM tab WHERE map_fixed['K2'] = toFixedString('V2', 2);
SELECT count() FROM tab WHERE map_fixed['K3'] = toFixedString('V3', 2);

SELECT '-- -- Check that the text index actually gets used (String)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_equals(use_idx_fixed = 0, filter = 'K3', value = 'V0');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_equals(use_idx_fixed = 0, filter = 'K3', value = 'V2');

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_equals(use_idx_fixed = 0, filter = 'K3', value = 'V1');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_equals(use_idx_fixed = 0, filter = 'K3', value = 'V3');

SELECT '-- -- Check that the text index actually gets used (FixedString)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_equals(use_idx_fixed = 1, filter = 'K3', value = toFixedString('V0', 2));

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_equals(use_idx_fixed = 1, filter = 'K3', value = toFixedString('V2', 2));

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_equals(use_idx_fixed = 1, filter = 'K3', value = toFixedString('V1', 2));

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_equals(use_idx_fixed = 1, filter = 'K3', value = toFixedString('V3', 2));

DROP VIEW IF EXISTS explain_index_has;
CREATE VIEW explain_index_has AS (
    SELECT trimLeft(explain) AS explain FROM (
        EXPLAIN indexes=1
        SELECT count() FROM tab WHERE (
            CASE
                WHEN {use_idx_fixed:boolean} = 1 THEN has(mapValues(map_fixed), {filter:FixedString(2)})
                ELSE has(mapValues(map), {filter:String})
            END
        )
    )
    WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
    LIMIT 2, 3
);

SELECT '-- has support';

SELECT '-- -- Check that the text index actually gets used (String)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'V0');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'V2');

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'V1');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_has(use_idx_fixed = 0, filter = 'V3');

SELECT '-- -- Check that the text index actually gets used (FixedString)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = 'V0');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = 'V2');

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = 'V1');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_has(use_idx_fixed = 1, filter = 'V3');

SELECT '-- hasAnyTokens support';

SELECT '-- -- query with String';

SELECT count() FROM tab WHERE hasAnyTokens(mapValues(map), 'V0 V1');
SELECT count() FROM tab WHERE hasAnyTokens(mapValues(map), 'V1 V2');
SELECT count() FROM tab WHERE hasAnyTokens(mapValues(map), 'V2 V3');
SELECT count() FROM tab WHERE hasAnyTokens(mapValues(map), 'V3 V4');

SELECT '-- -- query with FixedString';

SELECT count() FROM tab WHERE hasAnyTokens(mapValues(map_fixed), 'V0 V1');
SELECT count() FROM tab WHERE hasAnyTokens(mapValues(map_fixed), 'V1 V2');
SELECT count() FROM tab WHERE hasAnyTokens(mapValues(map_fixed), 'V2 V3');
SELECT count() FROM tab WHERE hasAnyTokens(mapValues(map_fixed), 'V3 V4');

DROP VIEW IF EXISTS explain_index_has_any_tokens;
CREATE VIEW explain_index_has_any_tokens AS (
    SELECT trimLeft(explain) AS explain FROM (
        EXPLAIN indexes=1
        SELECT count() FROM tab WHERE (
            CASE 
                WHEN {use_idx_fixed:boolean} = 1 THEN hasAnyTokens(mapValues(map_fixed), {filter:String})
                ELSE hasAnyTokens(mapValues(map), {filter:String})
            END
        )
    )
    WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
    LIMIT 2, 3
);

SELECT '-- -- Check that the text index actually gets used (String)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 0, filter = 'V0');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 0, filter = 'V2');

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 0, filter = 'V0 V1');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 0, filter = 'V3');

SELECT '-- -- Check that the text index actually gets used (FixedString)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 1, filter = 'V0');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 1, filter = 'V2');

SELECT '-- -- -- key exists only in both granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 1, filter = 'V0 V1');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_has_any_tokens(use_idx_fixed = 1, filter = 'V3');

SELECT '-- hasAllTokens support';

SELECT '-- -- query with String';

SELECT count() FROM tab WHERE hasAllTokens(mapValues(map), 'V0 V1');
SELECT count() FROM tab WHERE hasAllTokens(mapValues(map), 'V1 V2');
SELECT count() FROM tab WHERE hasAllTokens(mapValues(map), 'V2 V3');
SELECT count() FROM tab WHERE hasAllTokens(mapValues(map), 'V3 V4');

SELECT '-- -- query with FixedString';

SELECT count() FROM tab WHERE hasAllTokens(mapValues(map_fixed), 'V0 V1');
SELECT count() FROM tab WHERE hasAllTokens(mapValues(map_fixed), 'V1 V2');
SELECT count() FROM tab WHERE hasAllTokens(mapValues(map_fixed), 'V2 V3');
SELECT count() FROM tab WHERE hasAllTokens(mapValues(map_fixed), 'V3 V4');

DROP VIEW IF EXISTS explain_index_has_all_tokens;
CREATE VIEW explain_index_has_all_tokens AS (
    SELECT trimLeft(explain) AS explain FROM (
        EXPLAIN indexes=1
        SELECT count() FROM tab WHERE (
            CASE 
                WHEN {use_idx_fixed:boolean} = 1 THEN hasAllTokens(mapValues(map_fixed), {filter:String})
                ELSE hasAllTokens(mapValues(map), {filter:String})
            END
        )
    )
    WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
    LIMIT 2, 3
);

SELECT '-- -- Check that the text index actually gets used (String)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 0, filter = 'V0');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 0, filter = 'V2');

SELECT '-- -- -- key exists only in the first granules';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 0, filter = 'V0 V1');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 0, filter = 'V3');

SELECT '-- -- Check that the text index actually gets used (FixedString)';

SELECT '-- -- -- key exists only in the first granule';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 1, filter = 'V0');

SELECT '-- -- -- key exists only in the second granule';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 1, filter = 'V2');

SELECT '-- -- -- key exists only in the first granules';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 1, filter = 'V0 V1');

SELECT '-- -- -- key does not exist in granules';
SELECT * FROM explain_index_has_all_tokens(use_idx_fixed = 1, filter = 'V3');

DROP VIEW explain_index_mapContains;
DROP VIEW explain_index_equals;
DROP VIEW explain_index_has;
DROP VIEW explain_index_has_any_tokens;
DROP VIEW explain_index_has_all_tokens;
DROP TABLE tab;
