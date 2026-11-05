-- Tags: no-random-settings

-- Tests that text indexes built on mapValues(m) work correctly when the analyzer
-- rewrites arrayElement(m, 'key') into the map.key_* subcolumn form.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    map Map(String, String),
    INDEX idx_mv mapValues(map) TYPE text(tokenizer = 'array'),
    INDEX idx_mk mapKeys(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES (0, {'service':'web-api'}), (1, {'service':'backend'}), (2, {'service':'frontend'});

-- mapValues index with IN operator (subcolumn form via analyzer)
SELECT id FROM tab WHERE map['service'] IN ('web-api', 'backend') ORDER BY id;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE map['service'] IN ('web-api', 'backend')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

-- mapValues index with equality (subcolumn form via analyzer)
SELECT id FROM tab WHERE map['service'] = 'web-api' ORDER BY id;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE map['service'] = 'web-api'
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

-- empty string IN should not skip granules
SELECT id FROM tab WHERE map['nonexistent'] IN ('') ORDER BY id;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE map['nonexistent'] IN ('')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

-- single value IN
SELECT id FROM tab WHERE map['service'] IN ('frontend') ORDER BY id;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE map['service'] IN ('frontend')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

DROP TABLE tab;
