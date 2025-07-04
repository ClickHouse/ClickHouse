-- Test for Bug 52019: Undefined behavior

SET allow_experimental_full_text_index=1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    k UInt64,
    s Map(String, String),
    INDEX idx mapKeys(s) TYPE full_text(2) GRANULARITY 1)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi',
-- Full text index works only with full parts.
min_bytes_for_full_part_storage=0;

INSERT INTO tab (k) VALUES (0);
SELECT * FROM tab PREWHERE (s[NULL]) = 'Click a03' SETTINGS enable_analyzer=1;
SELECT * FROM tab PREWHERE (s[1]) = 'Click a03' SETTINGS enable_analyzer=1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM tab PREWHERE (s['foo']) = 'Click a03' SETTINGS enable_analyzer=1;

DROP TABLE tab;
