-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Direct read from a text index under make_distributed_plan (issue #109329). The initiator rewrites
-- text-search functions into __text_index_* virtual columns before the plan is split into fragments;
-- the serialized ReadFromMergeTree ships the text search queries and the default expressions behind
-- the virtual columns, and every worker rebuilds the index read tasks against its own copy of the
-- table. Every query is repeated with make_distributed_plan = 0; the results must be identical.

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3, max_rows_to_group_by = 0,
    query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
    query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS t_text_mdp;

SELECT '-- exact mode';

CREATE TABLE t_text_mdp (id UInt64, s String, INDEX idx_text s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text_mdp SELECT number, 'word' || toString(number) FROM numbers(100000);
INSERT INTO t_text_mdp SELECT number, 'other' || toString(number % 100) FROM numbers(100000, 50000);

SELECT count() FROM t_text_mdp WHERE hasToken(s, 'word42');
SELECT count() FROM t_text_mdp WHERE hasToken(s, 'word42') SETTINGS make_distributed_plan = 0;

SELECT count() FROM t_text_mdp WHERE hasAnyTokens(s, ['word42', 'other42', 'nonexistent']);
SELECT count() FROM t_text_mdp WHERE hasAnyTokens(s, ['word42', 'other42', 'nonexistent']) SETTINGS make_distributed_plan = 0;

SELECT count() FROM t_text_mdp WHERE hasAllTokens(s, ['word42']);
SELECT count() FROM t_text_mdp WHERE hasAllTokens(s, ['word42']) SETTINGS make_distributed_plan = 0;

SELECT sum(id) FROM t_text_mdp WHERE hasToken(s, 'other42');
SELECT sum(id) FROM t_text_mdp WHERE hasToken(s, 'other42') SETTINGS make_distributed_plan = 0;

SELECT '-- the same predicate in PREWHERE and WHERE';

SELECT count() FROM t_text_mdp PREWHERE hasToken(s, 'word42') WHERE hasToken(s, 'word42');
SELECT count() FROM t_text_mdp PREWHERE hasToken(s, 'word42') WHERE hasToken(s, 'word42') SETTINGS make_distributed_plan = 0;

SELECT '-- the plan distributes';

SELECT 'distributes'
FROM (EXPLAIN PIPELINE SELECT count() FROM t_text_mdp WHERE hasToken(s, 'word42'))
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;

SELECT '-- LIKE by dictionary scan (pattern queries)';

SELECT count() FROM t_text_mdp WHERE s LIKE '%word42%'
    SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT count() FROM t_text_mdp WHERE s LIKE '%word42%'
    SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, make_distributed_plan = 0;

SELECT count() FROM t_text_mdp WHERE s ILIKE '%WORD42%'
    SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT count() FROM t_text_mdp WHERE s ILIKE '%WORD42%'
    SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, make_distributed_plan = 0;

-- The AST-fuzzer shape from #108818: the text-search function appears both in a mixed PREWHERE
-- conjunction and inside a xor in WHERE, so the rewrite reaches the step from two filter stages.
SELECT '-- the fuzzer shape: mixed PREWHERE and a xor in WHERE';

SELECT id FROM t_text_mdp
PREWHERE (materialize(65537) >= id) AND hasToken(s, 'word42')
WHERE xor(hasToken(s, 'word42'), (id >= 65537))
ORDER BY id LIMIT 3;
SELECT id FROM t_text_mdp
PREWHERE (materialize(65537) >= id) AND hasToken(s, 'word42')
WHERE xor(hasToken(s, 'word42'), (id >= 65537))
ORDER BY id LIMIT 3 SETTINGS make_distributed_plan = 0;

-- A text index read reached through a subquery under a join: the read ships in its own fragment
-- of a multi-stage (shuffle join) plan.
SELECT '-- text index read under a join';

DROP TABLE IF EXISTS t_mdp_outer;
CREATE TABLE t_mdp_outer (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mdp_outer SELECT number FROM numbers(200000);

SELECT count() FROM t_mdp_outer AS o
JOIN (SELECT id FROM t_text_mdp PREWHERE hasToken(s, 'word42') WHERE id < 65537) AS t ON o.id = t.id;
SELECT count() FROM t_mdp_outer AS o
JOIN (SELECT id FROM t_text_mdp PREWHERE hasToken(s, 'word42') WHERE id < 65537) AS t ON o.id = t.id
SETTINGS make_distributed_plan = 0;

DROP TABLE t_mdp_outer;

DROP TABLE t_text_mdp;

SELECT '-- a part without the materialized index uses the shipped default expression';

DROP TABLE IF EXISTS t_text_mdp_mat;
CREATE TABLE t_text_mdp_mat (id UInt64, s String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text_mdp_mat SELECT number, 'word' || toString(number) FROM numbers(1000);
ALTER TABLE t_text_mdp_mat ADD INDEX idx_text s TYPE text(tokenizer = 'splitByNonAlpha');
INSERT INTO t_text_mdp_mat SELECT number, 'word' || toString(number) FROM numbers(1000, 1000);

SELECT count() FROM t_text_mdp_mat WHERE hasToken(s, 'word42');
SELECT count() FROM t_text_mdp_mat WHERE hasToken(s, 'word42') SETTINGS make_distributed_plan = 0;
SELECT count() FROM t_text_mdp_mat WHERE hasToken(s, 'word1042');
SELECT count() FROM t_text_mdp_mat WHERE hasToken(s, 'word1042') SETTINGS make_distributed_plan = 0;
SELECT count() FROM t_text_mdp_mat WHERE hasAnyTokens(s, ['word42', 'word1042']);
SELECT count() FROM t_text_mdp_mat WHERE hasAnyTokens(s, ['word42', 'word1042']) SETTINGS make_distributed_plan = 0;

DROP TABLE t_text_mdp_mat;

SELECT '-- preprocessor index';

DROP TABLE IF EXISTS t_text_mdp_prep;
CREATE TABLE t_text_mdp_prep (id UInt64, s String, INDEX idx_text (s) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(s)))
    ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text_mdp_prep SELECT number, 'WORD' || toString(number) FROM numbers(10000);

SELECT count() FROM t_text_mdp_prep WHERE hasToken(s, 'WoRd42');
SELECT count() FROM t_text_mdp_prep WHERE hasToken(s, 'WoRd42') SETTINGS make_distributed_plan = 0;
SELECT count() FROM t_text_mdp_prep WHERE hasAnyTokens(s, ['word42', 'WORD43']);
SELECT count() FROM t_text_mdp_prep WHERE hasAnyTokens(s, ['word42', 'WORD43']) SETTINGS make_distributed_plan = 0;

DROP TABLE t_text_mdp_prep;

SELECT '-- postprocessor index (hint mode)';

DROP TABLE IF EXISTS t_text_mdp_post;
CREATE TABLE t_text_mdp_post (id UInt64, val Array(String), INDEX idx (val) TYPE text(tokenizer = 'array', postprocessor = lower(val)))
    ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text_mdp_post VALUES (1, ['Foo']), (2, ['BAR']), (3, ['baz']);

SELECT count() FROM t_text_mdp_post WHERE has(val, 'Foo');
SELECT count() FROM t_text_mdp_post WHERE has(val, 'Foo') SETTINGS make_distributed_plan = 0;
SELECT count() FROM t_text_mdp_post WHERE has(val, 'foo');
SELECT count() FROM t_text_mdp_post WHERE has(val, 'foo') SETTINGS make_distributed_plan = 0;

DROP TABLE t_text_mdp_post;

SELECT '-- phrase search';

DROP TABLE IF EXISTS t_text_mdp_phrase;
CREATE TABLE t_text_mdp_phrase (id UInt64, s String, INDEX idx_text (s) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1))
    ENGINE = MergeTree ORDER BY id SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO t_text_mdp_phrase SELECT number, if(number % 100 = 42, 'hello brave world', 'brave hello new world') FROM numbers(10000);

SELECT count() FROM t_text_mdp_phrase WHERE hasPhrase(s, 'hello brave');
SELECT count() FROM t_text_mdp_phrase WHERE hasPhrase(s, 'hello brave') SETTINGS make_distributed_plan = 0;
SELECT count() FROM t_text_mdp_phrase WHERE hasPhrase(s, 'brave world');
SELECT count() FROM t_text_mdp_phrase WHERE hasPhrase(s, 'brave world') SETTINGS make_distributed_plan = 0;

DROP TABLE t_text_mdp_phrase;
