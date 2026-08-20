-- Test for crash when LIMIT 0 is used with use_skip_indexes_for_top_k
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/95065

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    v1 UInt32,
    INDEX v1idx v1 TYPE minmax GRANULARITY 1
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64;

INSERT INTO tab SELECT number, number FROM numbers(1000);

SELECT id, v1 FROM tab ORDER BY v1 DESC NULLS LAST LIMIT 0 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT id, v1 FROM tab ORDER BY v1 ASC LIMIT 0 SETTINGS use_skip_indexes_for_top_k = 1;

DROP TABLE tab;
