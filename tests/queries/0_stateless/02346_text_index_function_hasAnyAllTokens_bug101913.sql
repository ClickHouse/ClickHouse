-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101913
-- hasAllTokens/hasAnyTokens must return correct results when the query plan is
-- serialized (serialize_query_plan=1). Serialization causes the ColumnConst holding
-- the needles argument to be recreated with size 0 in initializeSearchTokens, which
-- must not be treated as an absent or empty needle list.

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_dist;

CREATE TABLE tab (id UInt64, str String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tab_dist AS tab ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'tab');

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab_dist WHERE hasAllTokens(str, ['foo'])
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

SELECT count() FROM tab_dist WHERE hasAllTokens(str, ['foo', 'BAR'])
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

SELECT count() FROM tab_dist WHERE hasAnyTokens(str, ['foo', 'BAR'])
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

SELECT count() FROM tab_dist WHERE hasAnyTokens(str, ['xyz'])
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

-- Same failure mode with a const String needle (goes through Field::Types::String branch).
-- 'foo' tokenizes to ['foo']; only row 1 matches.
SELECT count() FROM tab_dist WHERE hasAllTokens(str, 'foo')
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

-- 'foo BAR' tokenizes to ['foo', 'BAR']; rows 1 and 2 match.
SELECT count() FROM tab_dist WHERE hasAnyTokens(str, 'foo BAR')
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

DROP TABLE tab_dist;
DROP TABLE tab;
