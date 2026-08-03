-- Tests that text indexes on mapKeys(m) and mapValues(m) cooperate to skip granules with IN on large data.
--
-- Data layout (200000 rows, 25 granules with index_granularity=8192):
--   rows 0-49999:       {'service': 'web-api'}
--   rows 50000-99999:   {'service': 'backend'}
--   rows 100000-149999: {'service': 'frontend', 'env': 'prod'}
--   rows 150000-199999: {'service': 'worker',   'env': 'staging'}

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
        arrayConcat(
            [('service', multiIf(number < 50000, 'web-api', number < 100000, 'backend', number < 150000, 'frontend', 'worker'))],
            if(number >= 100000, [('env', if(number < 150000, 'prod', 'staging'))], [])
        ),
        'Map(String, String)'
    )
FROM numbers(200000);

-- { echoOn }
SELECT count() FROM tab WHERE map['env'] IN ('prod');
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE map['env'] IN ('prod')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%' ;

-- { echoOn }
SELECT count() FROM tab WHERE map['service'] IN ('worker') AND map['env'] IN ('prod');
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE map['service'] IN ('worker') AND map['env'] IN ('prod')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%' ;

-- { echoOn }
SELECT count() FROM tab WHERE map['service'] IN ('web-api', 'frontend');
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE map['service'] IN ('web-api', 'frontend')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%'  ;

-- { echoOn }
SELECT count() FROM tab WHERE map['env'] IN ('prod', 'staging');
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE map['env'] IN ('prod', 'staging')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

DROP TABLE tab;
