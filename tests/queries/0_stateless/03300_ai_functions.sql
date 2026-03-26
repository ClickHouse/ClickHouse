-- Tags: no-fasttest, no-parallel, no-replicated-database
-- no-parallel: drops and creates named collections

-- =============================================================================
-- AI Functions Test Suite
-- Tests argument validation, error handling, and return types for
-- aiGenerateContent. All tests run without a real AI provider or API key.
-- =============================================================================

SET allow_experimental_ai_functions = 1;
SET default_ai_provider = '';

-- =============================================================================
-- 1. Verify AI functions are registered
-- =============================================================================

SELECT '-- Function registration';
SELECT name FROM system.functions WHERE name IN ('aiGenerateContent') ORDER BY name;

-- =============================================================================
-- 2. aiGenerateContent: argument validation (expects 1-4 args)
-- =============================================================================

SELECT '-- aiGenerateContent: too few arguments';
SELECT aiGenerateContent(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateContent: too many arguments';
SELECT aiGenerateContent('a', 'b', 0.7, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateContent: missing named collection';
SELECT aiGenerateContent('hello world'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 3. Return type verification
-- Uses column references (not constants) so the optimizer cannot fold the
-- function call at analysis time. WHERE 0 prevents actual execution and HTTP.
-- =============================================================================

DROP NAMED COLLECTION IF EXISTS ai_credentials;
CREATE NAMED COLLECTION ai_credentials AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key-for-testing';

SET default_ai_provider = 'ai_credentials';

-- aiGenerateContent returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_content;
CREATE TABLE _03300_ret_content ENGINE = Memory AS
    SELECT aiGenerateContent(x) AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- aiGenerateContent return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_content';
DROP TABLE IF EXISTS _03300_ret_content;

-- =============================================================================
-- Cleanup
-- =============================================================================

DROP NAMED COLLECTION ai_credentials;
