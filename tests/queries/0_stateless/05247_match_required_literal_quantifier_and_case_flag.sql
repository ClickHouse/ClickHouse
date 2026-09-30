-- https://github.com/ClickHouse/ClickHouse/issues/110411
-- The literal `match` requires from a pattern also survives `{n}`, a negated case flag and a scoped one.
SET use_query_condition_cache = 0;
SET query_plan_direct_read_from_text_index = 0;
SET explain_query_plan_default = 'legacy';
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt8, s String, INDEX idx s TYPE text(tokenizer = ngrams(3)) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'foobar'), (4, 'nothing'), (5, 'prefix foo suffix'), (6, 'qux'), (7, 'fo'), (8, 'Foo');

SELECT groupArray(id) FROM (SELECT id FROM tab WHERE match(s, 'foo{1}') ORDER BY id);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE match(s, 'foo{1}')) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE match(s, 'fo{2,}')) WHERE explain LIKE '%Granules:%';
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE match(s, '(?-i)foo') ORDER BY id);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE match(s, '(?-i)foo')) WHERE explain LIKE '%Granules:%';
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE match(s, '(?i:foo)bar') ORDER BY id);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE match(s, '(?i:foo)bar')) WHERE explain LIKE '%Granules:%';
-- The case-insensitive part itself is not a literal.
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE match(s, '(?i)foo') ORDER BY id);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE match(s, '(?i)foo')) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;
