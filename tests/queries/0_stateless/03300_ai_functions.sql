-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops global named collections
-- no-replicated-database: named collections are server-global, not database-scoped

-- =============================================================================
-- AI Functions Test Suite
-- Tests argument validation, error handling, return types, settings behavior,
-- and named collection resolution for `aiGenerateContent`.
-- All tests run without a real AI provider or API key.
-- =============================================================================

-- Helper table: a String column with zero rows, used to test function behavior
-- without triggering actual HTTP calls. A non-constant column prevents the
-- optimizer from constant-folding the AI function during analysis.
DROP TABLE IF EXISTS _03300_input;
CREATE TABLE _03300_input (x String) ENGINE = Memory;

-- =============================================================================
-- 1. Experimental setting gate
-- =============================================================================

SELECT '-- Disabled by default';
SELECT aiGenerateContent('hello', 'world'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_ai_functions = 1;

SELECT '-- Enabled after setting';
SELECT name FROM system.functions WHERE name = 'aiGenerateContent';

-- =============================================================================
-- 2. Argument count validation
-- =============================================================================

SELECT '-- aiGenerateContent: too few arguments';
SELECT aiGenerateContent('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateContent: too many arguments';
SELECT aiGenerateContent('a', 'b', 'c', 0.7, 'x'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- =============================================================================
-- 3. Named collection: missing required fields
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_no_provider;
CREATE NAMED COLLECTION ai_no_provider AS
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key';

SELECT '-- Named collection missing provider';
SELECT aiGenerateContent('ai_no_provider', x) FROM _03300_input; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_no_provider;

DROP NAMED COLLECTION IF EXISTS ai_no_endpoint;
CREATE NAMED COLLECTION ai_no_endpoint AS
    provider = 'openai',
    model = 'test-model',
    api_key = 'fake-key';

SELECT '-- Named collection missing endpoint';
SELECT aiGenerateContent('ai_no_endpoint', x) FROM _03300_input; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_no_endpoint;

DROP NAMED COLLECTION IF EXISTS ai_no_model;
CREATE NAMED COLLECTION ai_no_model AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    api_key = 'fake-key';

SELECT '-- Named collection missing model';
SELECT aiGenerateContent('ai_no_model', x) FROM _03300_input; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_no_model;

DROP NAMED COLLECTION IF EXISTS ai_no_api_key;
CREATE NAMED COLLECTION ai_no_api_key AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model';

SELECT '-- Named collection missing api_key';
SELECT aiGenerateContent('ai_no_api_key', x) FROM _03300_input; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION ai_no_api_key;

-- =============================================================================
-- 4. Named collection: nonexistent collection
-- =============================================================================

SELECT '-- Nonexistent named collection';
SELECT aiGenerateContent('nonexistent_collection_xyz', 'hello'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- =============================================================================
-- 5. Test collection for remaining tests
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_test;
CREATE NAMED COLLECTION ai_test AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key';

-- =============================================================================
-- 6. Return type verification
-- =============================================================================

SELECT '-- aiGenerateContent return type';
DROP TABLE IF EXISTS _03300_ret_content;
CREATE TABLE _03300_ret_content ENGINE = Memory AS
    SELECT aiGenerateContent('ai_test', x) AS result FROM _03300_input;
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
    SELECT aiGenerateContent('ai_test', x) AS result FROM _03300_null_input;
SELECT result IS NULL FROM _03300_null_result;
DROP TABLE IF EXISTS _03300_null_result;
DROP TABLE IF EXISTS _03300_null_input;

-- =============================================================================
-- 8. Empty string input: zero rows, should not error
-- =============================================================================

SELECT '-- Empty string input accepted';
SELECT count() FROM (SELECT aiGenerateContent('ai_test', x) AS result FROM _03300_input);

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
SELECT aiGenerateContent('ai_bad_provider', x) FROM _03300_input; -- { serverError BAD_ARGUMENTS }

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
SELECT count() FROM (SELECT aiGenerateContent('ai_anthropic', x) AS result FROM _03300_input);

DROP NAMED COLLECTION ai_anthropic;

-- =============================================================================
-- 11. Custom system prompt argument
-- =============================================================================

SELECT '-- Custom system prompt accepted';
SELECT count() FROM (SELECT aiGenerateContent('ai_test', x, 'You are a pirate') AS result FROM _03300_input);

-- =============================================================================
-- 12. Temperature argument
-- =============================================================================

SELECT '-- Temperature: Float32';
SELECT count() FROM (SELECT aiGenerateContent('ai_test', x, 'system', toFloat32(0.5)) AS result FROM _03300_input);

SELECT '-- Temperature: Float64';
SELECT count() FROM (SELECT aiGenerateContent('ai_test', x, 'system', 0.5) AS result FROM _03300_input);

SELECT '-- Temperature: zero';
SELECT count() FROM (SELECT aiGenerateContent('ai_test', x, 'system', toFloat32(0.0)) AS result FROM _03300_input);

SELECT '-- Temperature: integer literal';
SELECT count() FROM (SELECT aiGenerateContent('ai_test', x, 'system', 1) AS result FROM _03300_input);

SELECT '-- Temperature without system prompt';
SELECT aiGenerateContent('ai_test', x, toFloat32(0.5)) FROM _03300_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Temperature without system prompt (integer)';
SELECT aiGenerateContent('ai_test', x, 1) FROM _03300_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Non-constant system prompt';
SELECT aiGenerateContent('ai_test', x, x) FROM _03300_input; -- { serverError ILLEGAL_COLUMN }

SELECT '-- Non-constant temperature';
SELECT aiGenerateContent('ai_test', x, 'system', toFloat32(number)) FROM (SELECT x, 0 AS number FROM _03300_input); -- { serverError ILLEGAL_COLUMN }

SELECT '-- Wrong type for system prompt (number instead of string)';
SELECT aiGenerateContent('ai_test', x, 42) FROM _03300_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Wrong type for temperature (string instead of number)';
SELECT aiGenerateContent('ai_test', x, 'system', 'hot') FROM _03300_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

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
-- 14. Re-disable the setting mid-session
-- =============================================================================

SET allow_experimental_ai_functions = 0;
SELECT '-- Re-disabled blocks function';
SELECT aiGenerateContent('ai_test', 'hello'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_ai_functions = 1;

-- =============================================================================
-- Cleanup
-- =============================================================================

DROP TABLE IF EXISTS _03300_input;
DROP NAMED COLLECTION ai_test;
