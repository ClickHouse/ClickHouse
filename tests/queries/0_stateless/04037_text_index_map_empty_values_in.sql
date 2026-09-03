-- Tests that text indexes on mapKeys(m) and mapValues(m) handle empty string values
-- correctly with IN operator. When a map key does not exist, map['key'] returns ''
-- (the default value). The index must not incorrectly skip granules in this case.
--
-- Data layout (200000 rows, 25 granules with index_granularity=8192):
--   rows 0-49999:       {'key': 'alpha'}
--   rows 50000-99999:   {'other': 'beta'}               -- 'key' absent, map['key'] = ''
--   rows 100000-149999: {'key': 'gamma', 'other': 'delta'}
--   rows 150000-199999: {'another': 'epsilon'}           -- 'key' absent, map['key'] = ''

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    map Map(String, String),
    INDEX idx_map_keys mapKeys(map) TYPE text(tokenizer = 'array'),
    INDEX idx_map_values mapValues(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0;

INSERT INTO tab SELECT
    number,
    CAST(
        multiIf(
            number < 50000,  [('key', 'alpha')],
            number < 100000, [('other', 'beta')],
            number < 150000, [('key', 'gamma'), ('other', 'delta')],
                             [('another', 'epsilon')]
        ),
        'Map(String, String)'
    )
FROM numbers(200000);

-- Empty value only: should match rows where 'key' is absent (100000 rows).
-- The index must NOT skip granules where the key does not exist.
-- { echoOn }
SELECT count() FROM tab WHERE map['key'] IN ('');
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE map['key'] IN ('')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

-- Empty value together with a real value.
-- { echoOn }
SELECT count() FROM tab WHERE map['key'] IN ('', 'alpha');
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE map['key'] IN ('', 'alpha')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

-- Non-empty values only (baseline for comparison).
-- { echoOn }
SELECT count() FROM tab WHERE map['key'] IN ('alpha');
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE map['key'] IN ('alpha')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

-- { echoOn }
SELECT count() FROM tab WHERE map['key'] IN ('alpha', 'gamma');
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE map['key'] IN ('alpha', 'gamma')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

DROP TABLE tab;
