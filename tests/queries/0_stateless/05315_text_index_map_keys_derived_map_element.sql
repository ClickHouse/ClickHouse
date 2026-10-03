-- Element access on a map derived from the indexed column must not use the `mapKeys` text index:
-- `xkey5` is a key of the derived map only, so the index would prune every granule.

DROP TABLE IF EXISTS tab;
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_optimize_count_from_text_index = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    m Map(String, String),
    INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8;

INSERT INTO tab SELECT map('key' || toString(number % 1000), 'value' || toString(number % 1000)) FROM numbers(1024);

SELECT count() FROM tab WHERE mapApply((k, v) -> (concat('x', k), v), m)['xkey5'] != '';
SELECT count() FROM tab WHERE m['xkey5'] != '';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE mapApply((k, v) -> (concat('x', k), v), m)['xkey5'] != '') WHERE explain ILIKE '%idx_keys%';

DROP TABLE tab;
