-- Tests that text indexes built on mapValues(m) work with the IN operator.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    key String,
    map Map(String, String),
    INDEX idx_key key TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX map_values_idx mapValues(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES (0, 'a', {'service':'web-api'}), (1, 'b', {'service':'backend'}), (2, 'c', {'service':'frontend'});

-- { echoOn }
SELECT id FROM tab WHERE map['service'] IN ('web-api', 'backend', 'dashboard') ORDER BY id;
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE map['service'] IN ('web-api', 'backend', 'dashboard')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

-- { echoOn }
SELECT id FROM tab WHERE map['service'] IN ('frontend') ORDER BY id;
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE map['service'] IN ('frontend')
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

-- { echoOn }
SELECT id FROM tab WHERE (key, map['service']) IN (('a', 'frontend'), ('a', 'web-api')) ORDER BY id;
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE (key, map['service']) IN (('a', 'frontend'), ('a', 'web-api'))
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

-- { echoOn }
SELECT id FROM tab WHERE (map['service'], key) IN (('frontend', 'c'), ('web-api', 'c')) ORDER BY id;
-- { echoOff }

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE (map['service'], key) IN (('frontend', 'c'), ('web-api', 'c'))
)
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

DROP TABLE tab;
