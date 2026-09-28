DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    foo Array(LowCardinality(String)),
    INDEX idx foo TYPE bloom_filter
)
ENGINE = MergeTree
PRIMARY KEY tuple();

INSERT INTO tab VALUES (['a', 'b']);

SELECT '---';

SELECT table, name, type
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'tab';

SELECT '---';

EXPLAIN indexes = 1, description = 0 SELECT * FROM tab WHERE has(foo, 'b');

DROP TABLE tab;
