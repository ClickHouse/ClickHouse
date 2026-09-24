-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops global named collections
-- no-replicated-database: named collections are server-global, not database-scoped

-- =============================================================================
-- aiRerank Test Suite
-- Tests argument validation, error handling, return types, settings behavior,
-- and named collection resolution for the aiRerank function.
-- All tests run without a real AI provider or API key.
--
-- Signature: aiRerank(query, documents[, params]). `model` resolves from the map's
-- `model` key, falling back to the named collection's `model`.
-- Credentials come from the map's `credentials` key or, when absent, from
-- `ai_function_rerank_default_credentials`.
-- =============================================================================

SELECT '-- aiRerank: disabled without allow_experimental_ai_rerank_function';
SELECT aiRerank('q', ['d']) SETTINGS allow_experimental_ai_rerank_function = 0; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_ai_rerank_function = 1;

-- Helper table: a String column with zero rows, used to test function behavior
-- without triggering actual HTTP calls. A non-constant column prevents the
-- optimizer from constant-folding the AI function during analysis.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (x String) ENGINE = Memory;

-- =============================================================================
-- 1. Registration
-- =============================================================================

SELECT '-- aiRerank: registered';
SELECT name FROM system.functions WHERE name = 'aiRerank';

-- =============================================================================
-- 2. Argument count validation
-- =============================================================================

SELECT '-- aiRerank: too few arguments';
SELECT aiRerank(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT aiRerank('q'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiRerank: too many arguments';
SELECT aiRerank('q', ['d'], map('top_n', '1'), 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- =============================================================================
-- 3. Argument type validation
-- =============================================================================

SELECT '-- aiRerank: wrong type for query argument (not a string)';
SELECT aiRerank(1, ['d']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- aiRerank: wrong type for documents argument (not an array)';
SELECT aiRerank(x, 'not-an-array') FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- aiRerank: wrong type for documents argument (Array of non-String)';
SELECT aiRerank(x, [1, 2, 3]) FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- aiRerank: non-constant parameter map';
SELECT aiRerank(x, [x], map('top_n', toString(number))) FROM (SELECT x, 0 AS number FROM tab); -- { serverError ILLEGAL_COLUMN }

SELECT '-- aiRerank: wrong type for parameter argument (not a map)';
SELECT aiRerank(x, [x], 256) FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- =============================================================================
-- 4. Missing credentials
-- =============================================================================

SELECT '-- aiRerank: missing credentials (no default, no map)';
SELECT aiRerank('q', ['d']); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 5. Named collection: missing required fields / nonexistent
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_rerank_no_provider;
CREATE NAMED COLLECTION ai_rerank_no_provider AS
    endpoint = 'http://localhost:1/v2/rerank',
    api_key = 'fake-key';

SELECT '-- aiRerank: named collection missing provider';
SELECT aiRerank('q', ['d'], map('credentials', 'ai_rerank_no_provider')); -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_rerank_no_provider;

DROP NAMED COLLECTION IF EXISTS ai_rerank_no_model;
CREATE NAMED COLLECTION ai_rerank_no_model AS
    provider = 'cohere',
    endpoint = 'http://localhost:1/v2/rerank',
    api_key = 'fake-key';

SELECT '-- aiRerank: named collection missing model (and none in map)';
SELECT aiRerank('q', ['d'], map('credentials', 'ai_rerank_no_model')); -- { serverError BAD_ARGUMENTS }

SELECT '-- aiRerank: model supplied via the parameter map resolves';
SELECT count() FROM (SELECT aiRerank(x, [x], map('credentials', 'ai_rerank_no_model', 'model', 'test-model')) AS result FROM tab);

DROP NAMED COLLECTION ai_rerank_no_model;

SELECT '-- aiRerank: nonexistent named collection';
SELECT aiRerank('q', ['d'], map('credentials', 'nonexistent_collection_xyz')); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- =============================================================================
-- 6. Test collection + default credentials for remaining tests
-- =============================================================================

-- The named collection's `model` is the default, overridable via the map.
DROP NAMED COLLECTION IF EXISTS ai_rerank_credentials;
CREATE NAMED COLLECTION ai_rerank_credentials AS
    provider = 'cohere',
    endpoint = 'http://localhost:1/v2/rerank',
    model = 'test-model',
    api_key = 'fake-key';

SET ai_function_rerank_default_credentials = 'ai_rerank_credentials';

SELECT '-- aiRerank: model resolved from the named collection';
SELECT count() FROM (SELECT aiRerank(x, [x]) AS result FROM tab);

SELECT '-- aiRerank: model in the parameter map overrides the named collection';
SELECT count() FROM (SELECT aiRerank(x, [x], map('model', 'other-model')) AS result FROM tab);

-- =============================================================================
-- 7. Unknown / unsupported provider
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_rerank_bad_provider;
CREATE NAMED COLLECTION ai_rerank_bad_provider AS
    provider = 'unknown_provider',
    endpoint = 'http://localhost:1/v2/rerank',
    model = 'test-model',
    api_key = 'fake-key';

SELECT '-- aiRerank: unknown provider name';
SELECT aiRerank('q', ['d'], map('credentials', 'ai_rerank_bad_provider')); -- { serverError BAD_ARGUMENTS }

SELECT '-- aiRerank: unknown provider name on empty input';
SELECT aiRerank(x, [x], map('credentials', 'ai_rerank_bad_provider')) FROM (SELECT '' AS x WHERE 0); -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_rerank_bad_provider;

-- A known provider without a reranking endpoint fails with NOT_IMPLEMENTED, before the zero-row
-- fast path.
SELECT '-- aiRerank: rejects openai provider (no reranking endpoint)';
DROP NAMED COLLECTION IF EXISTS ai_rerank_openai;
CREATE NAMED COLLECTION ai_rerank_openai AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v2/rerank',
    model = 'test-model',
    api_key = 'fake-key';
SELECT aiRerank('q', ['d'], map('credentials', 'ai_rerank_openai')); -- { serverError NOT_IMPLEMENTED }
SELECT aiRerank(x, [x], map('credentials', 'ai_rerank_openai')) FROM (SELECT '' AS x WHERE 0); -- { serverError NOT_IMPLEMENTED }
DROP NAMED COLLECTION ai_rerank_openai;

SELECT '-- aiRerank: rejects anthropic provider (no reranking endpoint)';
DROP NAMED COLLECTION IF EXISTS ai_rerank_anthropic;
CREATE NAMED COLLECTION ai_rerank_anthropic AS
    provider = 'anthropic',
    endpoint = 'http://localhost:1/v1/messages',
    model = 'test-model',
    api_key = 'fake-key';
SELECT aiRerank('q', ['d'], map('credentials', 'ai_rerank_anthropic')); -- { serverError NOT_IMPLEMENTED }
SELECT aiRerank(x, [x], map('credentials', 'ai_rerank_anthropic')) FROM (SELECT '' AS x WHERE 0); -- { serverError NOT_IMPLEMENTED }
DROP NAMED COLLECTION ai_rerank_anthropic;

-- The cohere provider only supports reranking, so the chat and embedding functions must reject it.
-- The collection omits `model`: `aiEmbed`/`aiSimilarity` reject any collection that defines one
-- (`BAD_ARGUMENTS`) before checking provider support, which would hide the error under test.
-- `aiGenerate` gets `model` from the parameter map instead.
DROP NAMED COLLECTION IF EXISTS ai_cohere_rerank_only;
CREATE NAMED COLLECTION ai_cohere_rerank_only AS
    provider = 'cohere',
    endpoint = 'http://localhost:1/v2/rerank',
    api_key = 'fake-key';

SELECT '-- aiGenerate: rejects cohere provider (no chat endpoint)';
SELECT aiGenerate('hi', map('credentials', 'ai_cohere_rerank_only', 'model', 'test-model')); -- { serverError NOT_IMPLEMENTED }
SELECT aiGenerate(x, map('credentials', 'ai_cohere_rerank_only', 'model', 'test-model')) FROM (SELECT '' AS x WHERE 0); -- { serverError NOT_IMPLEMENTED }

SELECT '-- aiEmbed: rejects cohere provider (no embedding endpoint)';
SELECT aiEmbed('hi', 'test-model', map('credentials', 'ai_cohere_rerank_only')); -- { serverError NOT_IMPLEMENTED }
SELECT aiEmbed(x, 'test-model', map('credentials', 'ai_cohere_rerank_only')) FROM (SELECT '' AS x WHERE 0); -- { serverError NOT_IMPLEMENTED }

SELECT '-- aiSimilarity: rejects cohere provider (no embedding endpoint)';
SELECT aiSimilarity('a', 'b', 'test-model', map('credentials', 'ai_cohere_rerank_only')); -- { serverError NOT_IMPLEMENTED }

DROP NAMED COLLECTION ai_cohere_rerank_only;

-- =============================================================================
-- 8. Parameter map: `top_n` validation
-- =============================================================================

SELECT '-- aiRerank: unknown parameter key rejected';
SELECT aiRerank('q', ['d'], map('bogus', '1')); -- { serverError BAD_ARGUMENTS }

SELECT '-- aiRerank: non-integer top_n rejected';
SELECT aiRerank('q', ['d'], map('top_n', 'many')); -- { serverError BAD_ARGUMENTS }

SELECT '-- aiRerank: negative top_n rejected';
SELECT aiRerank('q', ['d'], map('top_n', '-1')); -- { serverError BAD_ARGUMENTS }

SELECT '-- aiRerank: overflowing top_n rejected (exceeds UInt64, must not wrap)';
SELECT aiRerank('q', ['d'], map('top_n', '18446744073709551616')); -- { serverError BAD_ARGUMENTS }

SELECT '-- aiRerank: top_n via map resolves';
SELECT count() FROM (SELECT aiRerank(x, [x], map('top_n', '3')) AS result FROM tab);

-- =============================================================================
-- 9. Return type
-- =============================================================================

-- `print_pretty_type_names` defaults to on and would print the named Tuple across multiple lines.
SELECT '-- aiRerank: return type';
SELECT toTypeName(aiRerank('', ['a'])) SETTINGS print_pretty_type_names = 0;

-- `Nullable(Array(...))` is not a valid ClickHouse type, so `aiRerank` must keep its return type as
-- non-Nullable `Array(Tuple(...))` even when given `Nullable(String)`, and a NULL query must map to
-- `[]` at execute time.
SELECT '-- aiRerank: return type with Nullable(String) query';
DROP TABLE IF EXISTS _05237_ret_rerank_null;
DROP TABLE IF EXISTS _05237_rerank_null_in;
CREATE TABLE _05237_rerank_null_in (x Nullable(String)) ENGINE = Memory;
CREATE TABLE _05237_ret_rerank_null ENGINE = Memory AS
    SELECT aiRerank(x, ['a']) AS result FROM _05237_rerank_null_in;
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_05237_ret_rerank_null';
DROP TABLE IF EXISTS _05237_ret_rerank_null;
DROP TABLE IF EXISTS _05237_rerank_null_in;

-- =============================================================================
-- 10. NULL query / empty documents → []
-- =============================================================================

SELECT '-- aiRerank: NULL query → []';
DROP TABLE IF EXISTS _05237_rerank_null_query;
CREATE TABLE _05237_rerank_null_query (x Nullable(String)) ENGINE = Memory;
INSERT INTO _05237_rerank_null_query VALUES (NULL);
SELECT length(aiRerank(x, ['a', 'b'])) FROM _05237_rerank_null_query;
DROP TABLE _05237_rerank_null_query;

SELECT '-- aiRerank: empty documents array → []';
SELECT length(aiRerank('q', CAST([], 'Array(String)')));

SELECT '-- aiRerank: empty query string → []';
SELECT length(aiRerank('', ['a', 'b']));

SELECT '-- aiRerank: empty input executes';
SELECT count() FROM (SELECT aiRerank(x, [x]) AS result FROM tab);

-- =============================================================================
-- 11. AI functions in column DEFAULTs: CREATE + INSERT + SELECT must complete.
-- The HTTP call fails (no provider on localhost:1); `ai_function_throw_on_error = 0`
-- swallows the error so the INSERT still succeeds, with `[]` for the row.
-- =============================================================================

SET ai_function_throw_on_error = 0;
SET ai_function_request_timeout_sec = 3;

SELECT '-- aiRerank: DEFAULT survives INSERT (no exception)';
DROP TABLE IF EXISTS _05237_rerank_default;
CREATE TABLE _05237_rerank_default
(
    id UInt32,
    query String,
    documents Array(String),
    ranked Array(Tuple(index UInt32, relevance_score Float32)) DEFAULT aiRerank(query, documents)
) ENGINE = MergeTree ORDER BY id;
INSERT INTO _05237_rerank_default (id, query, documents) VALUES (1, 'hello', ['world', 'greeting']);
SELECT id, length(ranked) FROM _05237_rerank_default;
DROP TABLE _05237_rerank_default;

SET ai_function_throw_on_error = 1;
SET ai_function_request_timeout_sec = 60;

-- =============================================================================
-- 12. Setting defaults
-- =============================================================================

SELECT '-- aiRerank: default-credentials setting default';
SELECT name, default AS default_value FROM system.settings WHERE name = 'ai_function_rerank_default_credentials';

-- =============================================================================
-- Cleanup
-- =============================================================================

SET ai_function_rerank_default_credentials = '';
DROP TABLE IF EXISTS tab;
DROP NAMED COLLECTION ai_rerank_credentials;
