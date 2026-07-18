-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops global named collections
-- no-replicated-database: named collections are server-global, not database-scoped

-- =============================================================================
-- Default-credentials resolution for AI functions.
-- The text functions (aiGenerate/aiClassify/aiExtract/aiTranslate) and aiEmbed
-- use separate default-credentials settings, because a chat-completions endpoint
-- and model differ from an embeddings one. A per-call `credentials` map key
-- overrides the default. All tests run without a real AI provider.
-- =============================================================================

SET allow_experimental_ai_functions = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (x String) ENGINE = Memory;

DROP NAMED COLLECTION IF EXISTS ai_text_nc;
DROP NAMED COLLECTION IF EXISTS ai_embed_nc;
CREATE NAMED COLLECTION ai_text_nc AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'chat-model',
    api_key = 'fake-key';
CREATE NAMED COLLECTION ai_embed_nc AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/embeddings',
    model = 'embed-model',
    api_key = 'fake-key';

-- Start with no defaults set: bare calls must fail with a clear error.
SET ai_function_text_default_credentials = '';
SET ai_function_embedding_default_credentials = '';

SELECT '-- No defaults: text function fails';
SELECT aiGenerate('hi'); -- { serverError BAD_ARGUMENTS }
SELECT '-- No defaults: aiEmbed fails';
SELECT aiEmbed('hi'); -- { serverError BAD_ARGUMENTS }

-- Set only the text default. aiGenerate resolves; aiEmbed still has no default.
SET ai_function_text_default_credentials = 'ai_text_nc';

SELECT '-- Text default set: aiGenerate resolves via default';
SELECT count() FROM (SELECT aiGenerate(x) AS r FROM tab);

SELECT '-- Text default does not leak into aiEmbed';
SELECT aiEmbed('hi'); -- { serverError BAD_ARGUMENTS }

-- Set only the embedding default (clear the text one). aiEmbed resolves; text fails.
SET ai_function_text_default_credentials = '';
SET ai_function_embedding_default_credentials = 'ai_embed_nc';

SELECT '-- Embedding default set: aiEmbed resolves via default';
SELECT count() FROM (SELECT aiEmbed(x) AS r FROM tab);

SELECT '-- Embedding default does not leak into text functions';
SELECT aiGenerate('hi'); -- { serverError BAD_ARGUMENTS }

-- The per-call `credentials` map key overrides the default (and works with no default set).
SELECT '-- Map credentials override with no text default';
SELECT count() FROM (SELECT aiGenerate(x, map('credentials', 'ai_text_nc')) AS r FROM tab);

-- Map credentials override wins over a set default.
SET ai_function_text_default_credentials = 'ai_text_nc';
SELECT '-- Map credentials override wins over default';
SELECT count() FROM (SELECT aiGenerate(x, map('credentials', 'ai_embed_nc')) AS r FROM tab);

-- =============================================================================
-- Cleanup
-- =============================================================================

SET ai_function_text_default_credentials = '';
SET ai_function_embedding_default_credentials = '';
DROP NAMED COLLECTION ai_text_nc;
DROP NAMED COLLECTION ai_embed_nc;
DROP TABLE tab;
