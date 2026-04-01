-- Tags: no-fasttest, no-parallel, no-replicated-database
-- no-parallel: drops and creates named collections

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
SELECT aiGenerateContent('hello'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_ai_functions = 1;

SELECT '-- Enabled after setting';
SELECT name FROM system.functions WHERE name = 'aiGenerateContent';

-- =============================================================================
-- 2. Argument count validation
-- =============================================================================

SELECT '-- aiGenerateContent: too few arguments';
SELECT aiGenerateContent(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateContent: too many arguments';
SELECT aiGenerateContent('a', 'b', 0.7, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- =============================================================================
-- 3. No provider configured
-- =============================================================================

SET default_ai_provider = '';

SELECT '-- No provider: bare call';
SELECT aiGenerateContent('hello'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 4. Named collection: missing required fields
-- =============================================================================

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
-- 5. Named collection: nonexistent collection
-- =============================================================================

SELECT '-- Nonexistent named collection';
SELECT aiGenerateContent('nonexistent_collection_xyz', 'hello'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 6. default_ai_provider setting
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_test;
CREATE NAMED COLLECTION ai_test AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key';

SET default_ai_provider = 'ai_test';

-- With default_ai_provider set, the function should resolve config without
-- an explicit collection arg. Zero rows means no HTTP calls.
SELECT '-- default_ai_provider resolves collection';
SELECT count() FROM (SELECT aiGenerateContent(x) AS result FROM _03300_input);

-- =============================================================================
-- 7. Explicit collection overrides default_ai_provider
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_other;
CREATE NAMED COLLECTION ai_other AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'other-model',
    api_key = 'other-key';

SELECT '-- Explicit collection overrides default';
SELECT count() FROM (SELECT aiGenerateContent('ai_other', x) AS result FROM _03300_input);

DROP NAMED COLLECTION ai_other;

-- =============================================================================
-- 8. Return type verification
-- =============================================================================

SELECT '-- aiGenerateContent return type';
DROP TABLE IF EXISTS _03300_ret_content;
CREATE TABLE _03300_ret_content ENGINE = Memory AS
    SELECT aiGenerateContent(x) AS result FROM _03300_input;
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_content';
DROP TABLE IF EXISTS _03300_ret_content;

-- =============================================================================
-- 9. NULL input propagation
-- =============================================================================

DROP TABLE IF EXISTS _03300_null_input;
CREATE TABLE _03300_null_input (x Nullable(String)) ENGINE = Memory;
INSERT INTO _03300_null_input VALUES (NULL);

SELECT '-- NULL input returns empty string';
DROP TABLE IF EXISTS _03300_null_result;
CREATE TABLE _03300_null_result ENGINE = Memory AS
    SELECT aiGenerateContent(x) AS result FROM _03300_null_input;
SELECT result = '' FROM _03300_null_result;
DROP TABLE IF EXISTS _03300_null_result;
DROP TABLE IF EXISTS _03300_null_input;

-- =============================================================================
-- 10. Empty string input: zero rows, should not error
-- =============================================================================

SELECT '-- Empty string input accepted';
SELECT count() FROM (SELECT aiGenerateContent(x) AS result FROM _03300_input);

-- =============================================================================
-- 11. Unknown provider name
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
-- 12. Provider name: anthropic
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
-- 13. Custom system prompt argument
-- =============================================================================

SELECT '-- Custom system prompt accepted';
SELECT count() FROM (SELECT aiGenerateContent(x, 'You are a pirate') AS result FROM _03300_input);

-- =============================================================================
-- 14. Temperature argument
-- =============================================================================

SELECT '-- Temperature argument accepted';
SELECT count() FROM (SELECT aiGenerateContent(x, 'system', toFloat32(0.5)) AS result FROM _03300_input);

-- =============================================================================
-- 15. Named collection arg with too few remaining args
-- =============================================================================

SELECT '-- Named collection with no prompt';
SELECT aiGenerateContent('ai_test'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- =============================================================================
-- 16. Setting types and defaults
-- =============================================================================

SELECT '-- Setting defaults';
SELECT
    name,
    default AS default_value
FROM system.settings
WHERE name IN (
    'allow_experimental_ai_functions',
    'default_ai_provider',
    'ai_request_timeout_sec',
    'ai_max_concurrent_requests',
    'ai_max_rps',
    'ai_max_retries',
    'ai_retry_initial_delay_ms',
    'ai_cache_ttl_sec',
    'ai_on_error',
    'ai_max_rows_per_query',
    'ai_max_input_tokens_per_query',
    'ai_max_output_tokens_per_query',
    'ai_max_api_calls_per_query',
    'ai_on_quota_exceeded'
)
ORDER BY name;

-- =============================================================================
-- 17. Re-disable the setting mid-session
-- =============================================================================

SET allow_experimental_ai_functions = 0;
SELECT '-- Re-disabled blocks function';
SELECT aiGenerateContent('hello'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_ai_functions = 1;

-- =============================================================================
-- Cleanup
-- =============================================================================

DROP TABLE IF EXISTS _03300_input;
DROP NAMED COLLECTION ai_test;
