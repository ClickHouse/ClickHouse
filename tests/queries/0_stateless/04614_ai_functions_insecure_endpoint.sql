-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops global named collections
-- no-replicated-database: named collections are server-global, not database-scoped

-- =============================================================================
-- AI functions refuse a plaintext endpoint on a remote host by default, so prompts
-- and API keys are not silently sent over an unencrypted connection. Loopback
-- endpoints stay allowed, and the `ai_function_allow_insecure_endpoint` setting
-- overrides the check.
--
-- The endpoint check runs in `resolveAIParams`, before the zero-row early
-- return in `executeImpl`, so an empty source table exercises it without any
-- real HTTP call. All tests run without a real AI provider.
-- =============================================================================

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (x String) ENGINE = Memory;

DROP NAMED COLLECTION IF EXISTS ai_remote_http;
DROP NAMED COLLECTION IF EXISTS ai_remote_https;
DROP NAMED COLLECTION IF EXISTS ai_local_http;
DROP NAMED COLLECTION IF EXISTS ai_local_ip_http;
DROP NAMED COLLECTION IF EXISTS ai_embed_remote_http;

CREATE NAMED COLLECTION ai_remote_http AS
    provider = 'openai', endpoint = 'http://ai.example.com/v1/chat/completions', model = 'chat-model', api_key = 'fake-key';
CREATE NAMED COLLECTION ai_remote_https AS
    provider = 'openai', endpoint = 'https://ai.example.com/v1/chat/completions', model = 'chat-model', api_key = 'fake-key';
CREATE NAMED COLLECTION ai_local_http AS
    provider = 'openai', endpoint = 'http://localhost:1/v1/chat/completions', model = 'chat-model', api_key = 'fake-key';
CREATE NAMED COLLECTION ai_local_ip_http AS
    provider = 'openai', endpoint = 'http://127.0.0.1:1/v1/chat/completions', model = 'chat-model', api_key = 'fake-key';
CREATE NAMED COLLECTION ai_embed_remote_http AS
    provider = 'openai', endpoint = 'http://ai.example.com/v1/embeddings', api_key = 'fake-key';

SELECT '-- Remote http endpoint is rejected by default';
SELECT aiGenerate(x, map('credentials', 'ai_remote_http')) FROM tab; -- { serverError BAD_ARGUMENTS }
SELECT aiEmbed(x, 'embed-model', map('credentials', 'ai_embed_remote_http')) FROM tab; -- { serverError BAD_ARGUMENTS }

SELECT '-- Remote https endpoint is allowed';
SELECT count() FROM (SELECT aiGenerate(x, map('credentials', 'ai_remote_https')) AS r FROM tab);

SELECT '-- Loopback http endpoint is allowed (localhost and 127.0.0.1)';
SELECT count() FROM (SELECT aiGenerate(x, map('credentials', 'ai_local_http')) AS r FROM tab);
SELECT count() FROM (SELECT aiGenerate(x, map('credentials', 'ai_local_ip_http')) AS r FROM tab);

SELECT '-- Setting ai_function_allow_insecure_endpoint permits a remote http endpoint';
SET ai_function_allow_insecure_endpoint = 1;
SELECT count() FROM (SELECT aiGenerate(x, map('credentials', 'ai_remote_http')) AS r FROM tab);
SELECT count() FROM (SELECT aiEmbed(x, 'embed-model', map('credentials', 'ai_embed_remote_http')) AS r FROM tab);
SET ai_function_allow_insecure_endpoint = 0;

-- =============================================================================
-- Cleanup
-- =============================================================================

DROP NAMED COLLECTION ai_remote_http;
DROP NAMED COLLECTION ai_remote_https;
DROP NAMED COLLECTION ai_local_http;
DROP NAMED COLLECTION ai_local_ip_http;
DROP NAMED COLLECTION ai_embed_remote_http;
DROP TABLE tab;
