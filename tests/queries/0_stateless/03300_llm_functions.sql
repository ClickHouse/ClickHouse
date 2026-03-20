-- Tags: no-fasttest, no-parallel

-- =============================================================================
-- LLM Functions Test Suite
-- Tests argument validation, error handling, and return types for all 6 LLM
-- functions. All tests run without a real LLM provider or API key.
--
-- Covered functions:
--   LLMClassify, LLMExtract, LLMTranslate, LLMGenerateSQL,
--   LLMGenerateContent, LLMGenerateEmbedding
-- =============================================================================

SET default_llm_resource = '';

-- =============================================================================
-- 1. Verify all LLM functions are registered
-- =============================================================================

SELECT '-- Function registration';
SELECT name FROM system.functions WHERE name LIKE 'LLM%' ORDER BY name;

-- =============================================================================
-- 2. LLMClassify: argument validation (expects 2-4 args)
-- =============================================================================

SELECT '-- LLMClassify: too few arguments';
SELECT LLMClassify(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT LLMClassify('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMClassify: too many arguments';
SELECT LLMClassify('a', ['b'], 0.5, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMClassify: missing named collection';
SELECT LLMClassify('some text', ['positive', 'negative']); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 3. LLMExtract: argument validation (expects 2-4 args)
-- =============================================================================

SELECT '-- LLMExtract: too few arguments';
SELECT LLMExtract(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT LLMExtract('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMExtract: too many arguments';
SELECT LLMExtract('a', 'b', 0.5, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMExtract: missing named collection';
SELECT LLMExtract('text', 'extract the name'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 4. LLMTranslate: argument validation (expects 2-5 args)
-- =============================================================================

SELECT '-- LLMTranslate: too few arguments';
SELECT LLMTranslate(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT LLMTranslate('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMTranslate: too many arguments';
SELECT LLMTranslate('a', 'b', 'c', 'd', 'e', 'f'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMTranslate: missing named collection';
SELECT LLMTranslate('hello world', 'French'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 5. LLMGenerateSQL: argument validation (expects 1-5 args)
-- =============================================================================

SELECT '-- LLMGenerateSQL: too few arguments';
SELECT LLMGenerateSQL(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMGenerateSQL: too many arguments';
SELECT LLMGenerateSQL('a', 'b', 'c', 'd', 'e', 'f'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMGenerateSQL: missing named collection';
SELECT LLMGenerateSQL('top 10 users by revenue'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 6. LLMGenerateContent: argument validation (expects 1-4 args)
-- =============================================================================

SELECT '-- LLMGenerateContent: too few arguments';
SELECT LLMGenerateContent(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMGenerateContent: too many arguments';
SELECT LLMGenerateContent('a', 'b', 0.7, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMGenerateContent: missing named collection';
SELECT LLMGenerateContent('hello world'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 7. LLMGenerateEmbedding: argument validation (expects 2-3 args)
-- =============================================================================

SELECT '-- LLMGenerateEmbedding: too few arguments';
SELECT LLMGenerateEmbedding(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT LLMGenerateEmbedding('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMGenerateEmbedding: too many arguments';
SELECT LLMGenerateEmbedding('a', 256, 'c', 'd'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- LLMGenerateEmbedding: missing named collection';
SELECT LLMGenerateEmbedding('text', 256); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 8. Return type verification
-- Uses column references (not constants) so the optimizer cannot fold the
-- function call at analysis time. WHERE 0 prevents actual execution and HTTP.
-- =============================================================================

CREATE NAMED COLLECTION IF NOT EXISTS _03300_test_nc AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key-for-testing';
SET default_llm_resource = '_03300_test_nc';

-- LLMClassify returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_classify;
CREATE TABLE _03300_ret_classify ENGINE = Memory AS
    SELECT LLMClassify(x, ['a', 'b']) AS result FROM (SELECT 'text' AS x WHERE 0);
SELECT '-- LLMClassify return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_classify';
DROP TABLE IF EXISTS _03300_ret_classify;

-- LLMExtract returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_extract;
CREATE TABLE _03300_ret_extract ENGINE = Memory AS
    SELECT LLMExtract(x, 'instruction') AS result FROM (SELECT 'text' AS x WHERE 0);
SELECT '-- LLMExtract return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_extract';
DROP TABLE IF EXISTS _03300_ret_extract;

-- LLMTranslate returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_translate;
CREATE TABLE _03300_ret_translate ENGINE = Memory AS
    SELECT LLMTranslate(x, 'French') AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- LLMTranslate return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_translate';
DROP TABLE IF EXISTS _03300_ret_translate;

-- LLMGenerateSQL returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_sql;
CREATE TABLE _03300_ret_sql ENGINE = Memory AS
    SELECT LLMGenerateSQL(x) AS result FROM (SELECT 'show tables' AS x WHERE 0);
SELECT '-- LLMGenerateSQL return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_sql';
DROP TABLE IF EXISTS _03300_ret_sql;

-- LLMGenerateContent returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_content;
CREATE TABLE _03300_ret_content ENGINE = Memory AS
    SELECT LLMGenerateContent(x) AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- LLMGenerateContent return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_content';
DROP TABLE IF EXISTS _03300_ret_content;

-- LLMGenerateEmbedding returns Array(Float32)
DROP TABLE IF EXISTS _03300_ret_embedding;
CREATE TABLE _03300_ret_embedding ENGINE = Memory AS
    SELECT LLMGenerateEmbedding(x, 256) AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- LLMGenerateEmbedding return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_embedding';
DROP TABLE IF EXISTS _03300_ret_embedding;

-- =============================================================================
-- 9. LLMGenerateEmbedding: dimensions argument must be constant
-- The fake named collection lets us get past config resolution so the
-- non-constant dimensions check is reached before any HTTP calls.
-- =============================================================================

SELECT '-- LLMGenerateEmbedding: non-constant dimensions';
SELECT LLMGenerateEmbedding(x, number) FROM (SELECT 'text' AS x, number FROM numbers(2)); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- Cleanup
-- =============================================================================

SET default_llm_resource = '';
DROP NAMED COLLECTION IF EXISTS _03300_test_nc;
