-- Tags: no-fasttest, no-parallel, no-replicated-database
-- no-parallel: drops and creates named collections

-- =============================================================================
-- AI Functions Test Suite
-- Tests argument validation, error handling, and return types for all 6 AI
-- functions. All tests run without a real AI provider or API key.
--
-- Covered functions:
--   aiClassify, aiExtract, aiTranslate, aiGenerateSQL,
--   aiGenerateContent, aiGenerateEmbedding, aiGenerateEmbeddingOrNull
-- =============================================================================

SET allow_experimental_ai_functions = 1;
SET default_ai_provider = '';

-- =============================================================================
-- 1. Verify all AI functions are registered
-- =============================================================================

SELECT '-- Function registration';
SELECT name FROM system.functions WHERE name IN ('aiClassify', 'aiExtract', 'aiGenerateContent', 'aiGenerateSQL', 'aiTranslate', 'aiGenerateEmbedding', 'aiGenerateEmbeddingOrNull') ORDER BY name;

-- =============================================================================
-- 2. aiClassify: argument validation (expects 2-4 args)
-- =============================================================================

SELECT '-- aiClassify: too few arguments';
SELECT aiClassify(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT aiClassify('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiClassify: too many arguments';
SELECT aiClassify('a', ['b'], 0.5, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiClassify: missing named collection';
SELECT aiClassify('some text', ['positive', 'negative']); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 3. aiExtract: argument validation (expects 2-4 args)
-- =============================================================================

SELECT '-- aiExtract: too few arguments';
SELECT aiExtract(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT aiExtract('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiExtract: too many arguments';
SELECT aiExtract('a', 'b', 0.5, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiExtract: missing named collection';
SELECT aiExtract('text', 'extract the name'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 4. aiTranslate: argument validation (expects 2-5 args)
-- =============================================================================

SELECT '-- aiTranslate: too few arguments';
SELECT aiTranslate(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT aiTranslate('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiTranslate: too many arguments';
SELECT aiTranslate('a', 'b', 'c', 'd', 'e', 'f'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiTranslate: missing named collection';
SELECT aiTranslate('hello world', 'French'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 5. aiGenerateSQL: argument validation (expects 1-5 args)
-- =============================================================================

SELECT '-- aiGenerateSQL: too few arguments';
SELECT aiGenerateSQL(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateSQL: too many arguments';
SELECT aiGenerateSQL('a', 'b', 'c', 'd', 'e', 'f'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateSQL: missing named collection';
SELECT aiGenerateSQL('top 10 users by revenue'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 6. aiGenerateContent: argument validation (expects 1-4 args)
-- =============================================================================

SELECT '-- aiGenerateContent: too few arguments';
SELECT aiGenerateContent(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateContent: too many arguments';
SELECT aiGenerateContent('a', 'b', 0.7, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateContent: missing named collection';
SELECT aiGenerateContent('hello world'); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 7. aiGenerateEmbedding: argument validation (expects 2-3 args)
-- =============================================================================

SELECT '-- aiGenerateEmbedding: too few arguments';
SELECT aiGenerateEmbedding(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT aiGenerateEmbedding('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateEmbedding: too many arguments';
SELECT aiGenerateEmbedding('a', 256, 'c', 'd'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateEmbedding: missing named collection';
SELECT aiGenerateEmbedding('text', 256); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 7b. aiGenerateEmbeddingOrNull: argument validation (expects 2-3 args)
-- =============================================================================

SELECT '-- aiGenerateEmbeddingOrNull: too few arguments';
SELECT aiGenerateEmbeddingOrNull(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT aiGenerateEmbeddingOrNull('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateEmbeddingOrNull: too many arguments';
SELECT aiGenerateEmbeddingOrNull('a', 256, 'c', 'd'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- aiGenerateEmbeddingOrNull: missing named collection';
SELECT aiGenerateEmbeddingOrNull('text', 256); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- 8. Return type verification
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

-- aiClassify returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_classify;
CREATE TABLE _03300_ret_classify ENGINE = Memory AS
    SELECT aiClassify(x, ['a', 'b']) AS result FROM (SELECT 'text' AS x WHERE 0);
SELECT '-- aiClassify return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_classify';
DROP TABLE IF EXISTS _03300_ret_classify;

-- aiExtract returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_extract;
CREATE TABLE _03300_ret_extract ENGINE = Memory AS
    SELECT aiExtract(x, 'instruction') AS result FROM (SELECT 'text' AS x WHERE 0);
SELECT '-- aiExtract return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_extract';
DROP TABLE IF EXISTS _03300_ret_extract;

-- aiTranslate returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_translate;
CREATE TABLE _03300_ret_translate ENGINE = Memory AS
    SELECT aiTranslate(x, 'French') AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- aiTranslate return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_translate';
DROP TABLE IF EXISTS _03300_ret_translate;

-- aiGenerateSQL returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_sql;
CREATE TABLE _03300_ret_sql ENGINE = Memory AS
    SELECT aiGenerateSQL(x) AS result FROM (SELECT 'show tables' AS x WHERE 0);
SELECT '-- aiGenerateSQL return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_sql';
DROP TABLE IF EXISTS _03300_ret_sql;

-- aiGenerateContent returns Nullable(String)
DROP TABLE IF EXISTS _03300_ret_content;
CREATE TABLE _03300_ret_content ENGINE = Memory AS
    SELECT aiGenerateContent(x) AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- aiGenerateContent return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_content';
DROP TABLE IF EXISTS _03300_ret_content;

-- aiGenerateEmbedding returns Array(Float32)
DROP TABLE IF EXISTS _03300_ret_embedding;
CREATE TABLE _03300_ret_embedding ENGINE = Memory AS
    SELECT aiGenerateEmbedding(x, 256) AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- aiGenerateEmbedding return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_embedding';
DROP TABLE IF EXISTS _03300_ret_embedding;

-- aiGenerateEmbeddingOrNull returns Nullable(Array(Float32))
DROP TABLE IF EXISTS _03300_ret_embedding_or_null;
CREATE TABLE _03300_ret_embedding_or_null ENGINE = Memory AS
    SELECT aiGenerateEmbeddingOrNull(x, 256) AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- aiGenerateEmbeddingOrNull return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = '_03300_ret_embedding_or_null';
DROP TABLE IF EXISTS _03300_ret_embedding_or_null;

-- =============================================================================
-- 9. aiGenerateEmbedding: dimensions argument must be constant
-- The fake named collection lets us get past config resolution so the
-- non-constant dimensions check is reached before any HTTP calls.
-- =============================================================================

SELECT '-- aiGenerateEmbedding: non-constant dimensions';
SELECT aiGenerateEmbedding(x, number) FROM (SELECT 'text' AS x, number FROM numbers(2)); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- Cleanup
-- =============================================================================

DROP NAMED COLLECTION ai_credentials;
