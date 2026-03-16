-- Tags: no-parallel
-- no-parallel: creates and drops a named collection

-- Tests LLM functions.
-- The tests run without a real LLM provider or API key because
-- - LLM function calls are non-deterministic
-- - LLM function calls cost $$$
-- - LLM function calls are slow
-- - LLM function calls need provider credentials.

SET allow_experimental_ai_functions = 1;
SET default_ai_resource = '';

-- llmClassify expects 2-4 args

SELECT '-- llmClassify: too few arguments';
SELECT llmClassify(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT llmClassify('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmClassify: too many arguments';
SELECT llmClassify('a', ['b'], 0.5, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmClassify: missing named collection';
SELECT llmClassify('some text', ['positive', 'negative']); -- { serverError BAD_ARGUMENTS }

-- llmExtract expects 2-4 args

SELECT '-- llmExtract: too few arguments';
SELECT llmExtract(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT llmExtract('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmExtract: too many arguments';
SELECT llmExtract('a', 'b', 0.5, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmExtract: missing named collection';
SELECT llmExtract('text', 'extract the name'); -- { serverError BAD_ARGUMENTS }

-- llmExtract expects 2-5 args

SELECT '-- llmExtract: too few arguments';
SELECT llmExtract(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT llmExtract('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmExtract: too many arguments';
SELECT llmExtract('a', 'b', 'c', 'd', 'e', 'f'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmExtract: missing named collection';
SELECT llmExtract('hello world', 'French'); -- { serverError BAD_ARGUMENTS }

-- llmGenerateSQL expects 1-5 args

SELECT '-- llmGenerateSQL: too few arguments';
SELECT llmGenerateSQL(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmGenerateSQL: too many arguments';
SELECT llmGenerateSQL('a', 'b', 'c', 'd', 'e', 'f'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmGenerateSQL: missing named collection';
SELECT llmGenerateSQL('top 10 users by revenue'); -- { serverError BAD_ARGUMENTS }

-- llmGenerateContent expects 1-4 args

SELECT '-- llmGenerateContent: too few arguments';
SELECT llmGenerateContent(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmGenerateContent: too many arguments';
SELECT llmGenerateContent('a', 'b', 0.7, 'x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- llmGenerateContent: missing named collection';
SELECT llmGenerateContent('hello world'); -- { serverError BAD_ARGUMENTS }

-- generateEmbedding expects 2-3 args

SELECT '-- generateEmbedding: too few arguments';
SELECT generateEmbedding(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateEmbedding('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- generateEmbedding: too many arguments';
SELECT generateEmbedding('a', 256, 'c', 'd'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- generateEmbedding: missing named collection';
SELECT generateEmbedding('text', 256); -- { serverError BAD_ARGUMENTS }

-- generateEmbeddingOrNull expects 2-3 args

SELECT '-- generateEmbeddingOrNull: too few arguments';
SELECT generateEmbeddingOrNull(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateEmbeddingOrNull('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- generateEmbeddingOrNull: too many arguments';
SELECT generateEmbeddingOrNull('a', 256, 'c', 'd'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- generateEmbeddingOrNull: missing named collection';
SELECT generateEmbeddingOrNull('text', 256); -- { serverError BAD_ARGUMENTS }

-- Return type verification
-- Needs to avoid actual execution, therefore done in an awkward way via CREATE TABLE AS
-- Also, it uses column references (not constants) so the optimizer cannot fold the
-- function call at analysis time. WHERE 0 prevents actual execution and HTTP.

DROP NAMED COLLECTION IF EXISTS llm_credentials;

CREATE NAMED COLLECTION llm_credentials AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key-for-testing';

SET default_ai_resource = 'llm_credentials';

-- llmClassify returns Nullable(String)
DROP TABLE IF EXISTS tab;
CREATE TABLE tab ENGINE = Memory AS
    SELECT llmClassify(x, ['a', 'b']) AS result FROM (SELECT 'text' AS x WHERE 0);
SELECT '-- llmClassify return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = 'tab';
DROP TABLE IF EXISTS tab;

-- llmExtract returns Nullable(String)
DROP TABLE IF EXISTS tab;
CREATE TABLE tab ENGINE = Memory AS
    SELECT llmExtract(x, 'instruction') AS result FROM (SELECT 'text' AS x WHERE 0);
SELECT '-- llmExtract return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = 'tab';
DROP TABLE IF EXISTS tab;

-- llmExtract returns Nullable(String)
DROP TABLE IF EXISTS tab;
CREATE TABLE tab ENGINE = Memory AS
    SELECT llmExtract(x, 'French') AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- llmExtract return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = 'tab';
DROP TABLE IF EXISTS tab;

-- llmGenerateSQL returns Nullable(String)
DROP TABLE IF EXISTS tab;
CREATE TABLE tab ENGINE = Memory AS
    SELECT llmGenerateSQL(x) AS result FROM (SELECT 'show tables' AS x WHERE 0);
SELECT '-- llmGenerateSQL return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = 'tab';
DROP TABLE IF EXISTS tab;

-- llmGenerateContent returns Nullable(String)
DROP TABLE IF EXISTS tab;
CREATE TABLE tab ENGINE = Memory AS
    SELECT llmGenerateContent(x) AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- llmGenerateContent return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = 'tab';
DROP TABLE IF EXISTS tab;

-- generateEmbedding returns Array(Float32)
DROP TABLE IF EXISTS tab;
CREATE TABLE tab ENGINE = Memory AS
    SELECT generateEmbedding(x, 256) AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- generateEmbedding return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = 'tab';
DROP TABLE IF EXISTS tab;

-- generateEmbeddingOrNull returns Nullable(Array(Float32))
DROP TABLE IF EXISTS tab;
CREATE TABLE tab ENGINE = Memory AS
    SELECT generateEmbeddingOrNull(x, 256) AS result FROM (SELECT 'hello' AS x WHERE 0);
SELECT '-- generateEmbeddingOrNull return type';
SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = 'tab';
DROP TABLE IF EXISTS tab;

-- generateEmbedding: dimensions argument must be constant
-- The fake named collection lets us get past config resolution so the
-- non-constant dimensions check is reached before any HTTP calls.

SELECT '-- generateEmbedding: non-constant dimensions';
SELECT generateEmbedding(x, number) FROM (SELECT 'text' AS x, number FROM numbers(2)); -- { serverError BAD_ARGUMENTS }

-- Cleanup

DROP NAMED COLLECTION llm_credentials;
