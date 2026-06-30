-- Tests that the text-index ILIKE dictionary-scan optimization is disabled when a postprocessor is
-- configured, so results are the same with and without use_text_index_like_evaluation_by_dictionary_scan.

DROP TABLE IF EXISTS tab;

-- The postprocessor strips the 'ing' suffix, so the index dictionary stores transformed tokens
-- ('running' -> 'runn'). ILIKE '%running%' on the raw value is TRUE for row 1; if the optimization
-- wrongly used the index it would search '%running%' against 'runn', miss, and drop the row.
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = replaceRegexpAll(message, 'ing$', ''))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab(id, message) VALUES (1, 'running walking'), (2, 'cat dog');

SELECT '-- without optimization';
SET use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT groupArray(id) FROM tab WHERE message ILIKE '%running%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%RUNNING%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%running%';

SELECT '-- with optimization (must produce same results)';
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT groupArray(id) FROM tab WHERE message ILIKE '%running%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%RUNNING%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%running%';

DROP TABLE tab;
