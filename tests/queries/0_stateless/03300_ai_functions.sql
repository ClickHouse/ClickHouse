-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops global named collections
-- no-replicated-database: named collections are server-global, not database-scoped

-- =============================================================================
-- AI Functions Test Suite
-- Tests argument validation, error handling, return types, settings behavior,
-- and named collection resolution for `aiGenerate`.
-- All tests run without a real AI provider or API key.
-- =============================================================================

-- Helper table: a String column with zero rows, used to test function behavior
-- without triggering actual HTTP calls. A non-constant column prevents the
-- optimizer from constant-folding the AI function during analysis.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (x String) ENGINE = Memory;

-- =============================================================================
-- 1. Experimental setting
-- =============================================================================

SELECT '-- Disabled by default';
SELECT aiGenerate('hello', 'world'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_ai_functions = 1;

SELECT '-- Enabled after setting';
SELECT name FROM system.functions WHERE name = 'aiGenerate';

-- =============================================================================
-- 2. Argument count validation
-- =============================================================================

SELECT '-- aiGenerate: too few arguments';
SELECT aiGenerate('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerate: too many arguments';
SELECT aiGenerate('a', 'b', 'c', 0.7, 'x'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- =============================================================================
-- 3. Named collection: missing required fields
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_no_provider;
CREATE NAMED COLLECTION ai_no_provider AS
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key';

SELECT '-- Named collection missing provider';
SELECT aiGenerate('ai_no_provider', x) FROM tab; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_no_provider;

DROP NAMED COLLECTION IF EXISTS ai_no_endpoint;
CREATE NAMED COLLECTION ai_no_endpoint AS
    provider = 'openai',
    model = 'test-model',
    api_key = 'fake-key';

SELECT '-- Named collection missing endpoint';
SELECT aiGenerate('ai_no_endpoint', x) FROM tab; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_no_endpoint;

DROP NAMED COLLECTION IF EXISTS ai_no_model;
CREATE NAMED COLLECTION ai_no_model AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    api_key = 'fake-key';

SELECT '-- Named collection missing model';
SELECT aiGenerate('ai_no_model', x) FROM tab; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_no_model;

DROP NAMED COLLECTION IF EXISTS ai_no_api_key;
CREATE NAMED COLLECTION ai_no_api_key AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model';

SELECT '-- Named collection missing api_key';
SELECT aiGenerate('ai_no_api_key', x) FROM tab; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_no_api_key;

-- =============================================================================
-- 4. Named collection: nonexistent collection
-- =============================================================================

SELECT '-- Nonexistent named collection';
SELECT aiGenerate('nonexistent_collection_xyz', 'hello'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- =============================================================================
-- 5. Test collection for remaining tests
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_credentials;
CREATE NAMED COLLECTION ai_credentials AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key';

-- =============================================================================
-- 6. Return type verification
-- =============================================================================

SELECT '-- aiGenerate return type';
DROP TABLE IF EXISTS _03300_ret_content;
CREATE TABLE _03300_ret_content ENGINE = Memory AS
    SELECT aiGenerate('ai_credentials', x) AS result FROM tab;
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_content';
DROP TABLE IF EXISTS _03300_ret_content;

-- =============================================================================
-- 7. NULL input propagation
-- =============================================================================

DROP TABLE IF EXISTS _03300_null_input;
CREATE TABLE _03300_null_input (x Nullable(String)) ENGINE = Memory;
INSERT INTO _03300_null_input VALUES (NULL);

SELECT '-- NULL input returns NULL';
DROP TABLE IF EXISTS _03300_null_result;
CREATE TABLE _03300_null_result ENGINE = Memory AS
    SELECT aiGenerate('ai_credentials', x) AS result FROM _03300_null_input;
SELECT result IS NULL FROM _03300_null_result;
DROP TABLE IF EXISTS _03300_null_result;
DROP TABLE IF EXISTS _03300_null_input;

-- =============================================================================
-- 8. Empty string input: zero rows, should not error
-- =============================================================================

SELECT '-- Empty string input accepted';
SELECT count() FROM (SELECT aiGenerate('ai_credentials', x) AS result FROM tab);

-- =============================================================================
-- 9. Unknown provider name
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_bad_provider;
CREATE NAMED COLLECTION ai_bad_provider AS
    provider = 'unknown_provider',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key';

SELECT '-- Unknown provider name';
SELECT aiGenerate('ai_bad_provider', x) FROM tab; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_bad_provider;

-- =============================================================================
-- 10. Provider name: anthropic
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_anthropic;
CREATE NAMED COLLECTION ai_anthropic AS
    provider = 'anthropic',
    endpoint = 'http://localhost:1/v1/messages',
    model = 'claude-test',
    api_key = 'fake-key';

SELECT '-- Anthropic provider resolves';
SELECT count() FROM (SELECT aiGenerate('ai_anthropic', x) AS result FROM tab);

DROP NAMED COLLECTION ai_anthropic;

-- =============================================================================
-- 11. Custom system prompt argument
-- =============================================================================

SELECT '-- Custom system prompt accepted';
SELECT count() FROM (SELECT aiGenerate('ai_credentials', x, 'You are a pirate') AS result FROM tab);

-- =============================================================================
-- 12. Temperature argument
-- =============================================================================

SELECT '-- Temperature: Float32';
SELECT count() FROM (SELECT aiGenerate('ai_credentials', x, 'system', toFloat32(0.5)) AS result FROM tab);

SELECT '-- Temperature: Float64';
SELECT count() FROM (SELECT aiGenerate('ai_credentials', x, 'system', 0.5) AS result FROM tab);

SELECT '-- Temperature: zero';
SELECT count() FROM (SELECT aiGenerate('ai_credentials', x, 'system', toFloat32(0.0)) AS result FROM tab);

SELECT '-- Temperature: integer literal';
SELECT count() FROM (SELECT aiGenerate('ai_credentials', x, 'system', 1) AS result FROM tab);

SELECT '-- Temperature without system prompt';
SELECT aiGenerate('ai_credentials', x, toFloat32(0.5)) FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Temperature without system prompt (integer)';
SELECT aiGenerate('ai_credentials', x, 1) FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Non-constant system prompt';
SELECT aiGenerate('ai_credentials', x, x) FROM tab; -- { serverError ILLEGAL_COLUMN }

SELECT '-- Non-constant temperature';
SELECT aiGenerate('ai_credentials', x, 'system', toFloat32(number)) FROM (SELECT x, 0 AS number FROM tab); -- { serverError ILLEGAL_COLUMN }

SELECT '-- Wrong type for system prompt (number instead of string)';
SELECT aiGenerate('ai_credentials', x, 42) FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Wrong type for temperature (string instead of number)';
SELECT aiGenerate('ai_credentials', x, 'system', 'hot') FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- =============================================================================
-- 13. Setting types and defaults
-- =============================================================================

SELECT '-- Setting defaults';
SELECT
    name,
    default AS default_value
FROM system.settings
WHERE name IN (
    'allow_experimental_ai_functions',
    'ai_function_request_timeout_sec',
    'ai_function_max_retries',
    'ai_function_retry_initial_delay_ms',
    'ai_function_throw_on_error',
    'ai_function_max_input_tokens_per_query',
    'ai_function_max_output_tokens_per_query',
    'ai_function_max_api_calls_per_query',
    'ai_function_throw_on_quota_exceeded'
)
ORDER BY name;

-- =============================================================================
-- 14. aiClassify
-- =============================================================================

SELECT '-- aiClassify: registered';
SELECT name FROM system.functions WHERE name = 'aiClassify';

SELECT '-- aiClassify: too few arguments';
SELECT aiClassify('ai_credentials'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT aiClassify('ai_credentials', 'hello'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiClassify: too many arguments';
SELECT aiClassify('ai_credentials', 'x', ['a', 'b'], 0.0, 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiClassify: non-constant categories';
SELECT aiClassify('ai_credentials', x, [x]) FROM tab; -- { serverError ILLEGAL_COLUMN }

SELECT '-- aiClassify: wrong type for categories (not array)';
SELECT aiClassify('ai_credentials', x, 'positive,negative') FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- aiClassify: wrong type for categories (Array of non-String)';
SELECT aiClassify('ai_credentials', x, [1, 2, 3]) FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- aiClassify: empty categories array';
SELECT aiClassify('ai_credentials', 'test', CAST([], 'Array(String)')); -- { serverError BAD_ARGUMENTS }

SELECT '-- aiClassify: return type';
DROP TABLE IF EXISTS _03300_ret_classify;
CREATE TABLE _03300_ret_classify ENGINE = Memory AS
    SELECT aiClassify('ai_credentials', x, ['a', 'b', 'c']) AS result FROM tab;
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_classify';
DROP TABLE IF EXISTS _03300_ret_classify;

SELECT '-- aiClassify: empty input executes';
SELECT count() FROM (SELECT aiClassify('ai_credentials', x, ['a', 'b']) AS result FROM tab);

SELECT '-- aiClassify: with temperature';
SELECT count() FROM (SELECT aiClassify('ai_credentials', x, ['a', 'b'], 0.0) AS result FROM tab);

-- =============================================================================
-- 15. aiExtract
-- =============================================================================

SELECT '-- aiExtract: registered';
SELECT name FROM system.functions WHERE name = 'aiExtract';

SELECT '-- aiExtract: too few arguments';
SELECT aiExtract('ai_credentials'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT aiExtract('ai_credentials', 'hello'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiExtract: too many arguments';
SELECT aiExtract('ai_credentials', 'x', 'instr', 0.0, 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiExtract: non-constant instruction';
SELECT aiExtract('ai_credentials', x, x) FROM tab; -- { serverError ILLEGAL_COLUMN }

SELECT '-- aiExtract: return type';
DROP TABLE IF EXISTS _03300_ret_extract;
CREATE TABLE _03300_ret_extract ENGINE = Memory AS
    SELECT aiExtract('ai_credentials', x, 'main topic') AS result FROM tab;
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_extract';
DROP TABLE IF EXISTS _03300_ret_extract;

SELECT '-- aiExtract: JSON schema mode accepted';
SELECT count() FROM (SELECT aiExtract('ai_credentials', x, '{"topic":"main topic","sentiment":"pos/neg"}') AS result FROM tab);

SELECT '-- aiExtract: malformed JSON schema';
SELECT aiExtract('ai_credentials', x, '{invalid') FROM tab; -- { serverError BAD_ARGUMENTS }

SELECT '-- aiExtract: JSON schema with non-string value';
SELECT aiExtract('ai_credentials', x, '{"a":null}') FROM tab; -- { serverError BAD_ARGUMENTS }

-- Leading whitespace before the `{` must still be routed to schema mode, otherwise a malformed
-- JSON would be silently accepted as a free-text instruction.
SELECT '-- aiExtract: schema mode detection ignores leading whitespace';
SELECT aiExtract('ai_credentials', x, '   {invalid') FROM tab; -- { serverError BAD_ARGUMENTS }
SELECT aiExtract('ai_credentials', x, '\n\t {invalid') FROM tab; -- { serverError BAD_ARGUMENTS }

SELECT '-- aiExtract: with temperature';
SELECT count() FROM (SELECT aiExtract('ai_credentials', x, 'main topic', 0.0) AS result FROM tab);

-- =============================================================================
-- 16. aiTranslate
-- =============================================================================

SELECT '-- aiTranslate: registered';
SELECT name FROM system.functions WHERE name = 'aiTranslate';

SELECT '-- aiTranslate: too few arguments';
SELECT aiTranslate('ai_credentials'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT aiTranslate('ai_credentials', 'hello'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiTranslate: too many arguments';
SELECT aiTranslate('ai_credentials', 'x', 'French', 'instr', 0.3, 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiTranslate: non-constant target language';
SELECT aiTranslate('ai_credentials', x, x) FROM tab; -- { serverError ILLEGAL_COLUMN }

SELECT '-- aiTranslate: empty target language';
SELECT aiTranslate('ai_credentials', 'test', ''); -- { serverError BAD_ARGUMENTS }
SELECT aiTranslate('ai_credentials', 'test', '   '); -- { serverError BAD_ARGUMENTS }

SELECT '-- aiTranslate: return type';
DROP TABLE IF EXISTS _03300_ret_translate;
CREATE TABLE _03300_ret_translate ENGINE = Memory AS
    SELECT aiTranslate('ai_credentials', x, 'French') AS result FROM tab;
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_translate';
DROP TABLE IF EXISTS _03300_ret_translate;

SELECT '-- aiTranslate: with instructions and temperature';
SELECT count() FROM (SELECT aiTranslate('ai_credentials', x, 'French', 'keep proper nouns', 0.3) AS result FROM tab);

-- =============================================================================
-- 17. Re-disable the setting mid-session
-- =============================================================================

SET allow_experimental_ai_functions = 0;
SELECT '-- Re-disabled blocks function';
SELECT aiGenerate('ai_credentials', 'hello'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_ai_functions = 1;

-- =============================================================================
-- Cleanup
-- =============================================================================

DROP TABLE IF EXISTS tab;
DROP NAMED COLLECTION ai_credentials;
