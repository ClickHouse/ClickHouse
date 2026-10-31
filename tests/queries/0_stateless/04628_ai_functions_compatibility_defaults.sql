-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops global named collections
-- no-replicated-database: named collections are server-global, not database-scoped

-- =============================================================================
-- Three AI function settings changed their defaults in 26.8: `ai_function_allow_insecure_endpoint`
-- from 1 to 0, `ai_function_max_retries` from 0 to 3, and the new
-- `ai_function_max_concurrent_requests_per_thread` is 8 where requests used to go out one at a time (1).
-- `ai_function_max_api_calls_per_query` stays 0 (unlimited). `compatibility = 26.6` predates the
-- changes and restores them, which pins the previous_value/new_value pairs in `SettingsChangesHistory`.
--
-- The endpoint check runs in `resolveAIParams`, before the zero-row early return
-- in `executeImpl`, so an empty source table exercises it without any real HTTP
-- call. All tests run without a real AI provider.
-- =============================================================================

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (x String) ENGINE = Memory;

DROP NAMED COLLECTION IF EXISTS ai_compat_remote_http;
CREATE NAMED COLLECTION ai_compat_remote_http AS
    provider = 'openai', endpoint = 'http://ai.example.com/v1/chat/completions', model = 'chat-model', api_key = 'fake-key';

SELECT '-- Current defaults';
SELECT getSetting('ai_function_allow_insecure_endpoint'), getSetting('ai_function_max_api_calls_per_query'), getSetting('ai_function_max_retries'), getSetting('ai_function_max_concurrent_requests_per_thread');
SELECT aiGenerate(x, map('credentials', 'ai_compat_remote_http')) FROM tab; -- { serverError BAD_ARGUMENTS }

SELECT '-- compatibility = 26.6 restores the legacy defaults';
SET compatibility = '26.6';
SELECT getSetting('ai_function_allow_insecure_endpoint'), getSetting('ai_function_max_api_calls_per_query'), getSetting('ai_function_max_retries'), getSetting('ai_function_max_concurrent_requests_per_thread');
SELECT count() FROM (SELECT aiGenerate(x, map('credentials', 'ai_compat_remote_http')) AS r FROM tab);

-- =============================================================================
-- Cleanup
-- =============================================================================

SET compatibility = '';

DROP NAMED COLLECTION ai_compat_remote_http;
DROP TABLE tab;
