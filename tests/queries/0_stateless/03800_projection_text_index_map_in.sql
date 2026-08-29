SET allow_experimental_projection_text_index = 1;
-- Tests that projection text indexes on mapValues/mapKeys work with the IN operator,
-- including the empty string edge case when a map key is absent.
-- Adapted from 04035_text_index_map_values_in and 04037_text_index_map_empty_values_in.

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab_map_in;

-- Basic IN with mapValues projection
CREATE TABLE tab_map_in
(
    id UInt32,
    map Map(String, String),
    PROJECTION idx INDEX mapValues(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO tab_map_in VALUES (0, {'service':'web-api'}), (1, {'service':'backend'}), (2, {'service':'frontend'});
INSERT INTO tab_map_in VALUES (3, {'service':'dashboard'}), (4, {'service':'web-api'}), (5, {'service':'backend'});

-- Multiple values IN
SELECT id FROM tab_map_in WHERE map['service'] IN ('web-api', 'backend', 'dashboard') ORDER BY id;

-- Single value IN
SELECT id FROM tab_map_in WHERE map['service'] IN ('frontend') ORDER BY id;

-- Non-matching IN
SELECT id FROM tab_map_in WHERE map['service'] IN ('nonexistent') ORDER BY id;

DROP TABLE tab_map_in;

-- Empty string edge case: map['absent_key'] returns '' by default.
-- The index must not incorrectly skip rows where the key is absent.

DROP TABLE IF EXISTS tab_map_empty;

CREATE TABLE tab_map_empty
(
    id UInt64,
    map Map(String, String),
    PROJECTION idx INDEX mapValues(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- rows 0-9: key present with value 'alpha'
-- rows 10-19: key absent → map['key'] = ''
-- rows 20-29: key present with value 'gamma'
-- rows 30-39: key absent → map['key'] = ''
INSERT INTO tab_map_empty SELECT number, if(number < 10 OR (number >= 20 AND number < 30), map('key', if(number < 10, 'alpha', 'gamma')), map('other', 'beta')) FROM numbers(40);

-- Empty value only: match rows where 'key' is absent
SELECT count() FROM tab_map_empty WHERE map['key'] IN ('');

-- Empty value together with a real value
SELECT count() FROM tab_map_empty WHERE map['key'] IN ('', 'alpha');

-- Non-empty values only (baseline)
SELECT count() FROM tab_map_empty WHERE map['key'] IN ('alpha');

-- Multiple non-empty values
SELECT count() FROM tab_map_empty WHERE map['key'] IN ('alpha', 'gamma');

DROP TABLE tab_map_empty;

-- Map values subcolumn with IN via analyzer rewrite

DROP TABLE IF EXISTS tab_map_sub;

CREATE TABLE tab_map_sub
(
    id UInt32,
    map Map(String, String),
    PROJECTION idx INDEX mapValues(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO tab_map_sub VALUES (0, {'service':'web-api'}), (1, {'service':'backend'}), (2, {'service':'frontend'});

-- IN with analyzer subcolumn rewrite
SELECT id FROM tab_map_sub WHERE map['service'] IN ('web-api', 'backend') ORDER BY id;

-- Equality with analyzer subcolumn rewrite
SELECT id FROM tab_map_sub WHERE map['service'] = 'web-api' ORDER BY id;

-- empty string IN should not skip rows
SELECT id FROM tab_map_sub WHERE map['nonexistent'] IN ('') ORDER BY id;

DROP TABLE tab_map_sub;
