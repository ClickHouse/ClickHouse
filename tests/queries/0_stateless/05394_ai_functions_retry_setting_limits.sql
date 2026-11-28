-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops global named collections
-- no-replicated-database: named collections are server-global, not database-scoped

-- `ai_function_max_retries` is at most 5 and `ai_function_retry_initial_delay_ms` at most 10000. The limits are
-- checked before any request is sent, so the unreachable endpoint below is never contacted for a rejected query.

DROP NAMED COLLECTION IF EXISTS ai_retry_limits;
DROP NAMED COLLECTION IF EXISTS ai_retry_limits_embed;
CREATE NAMED COLLECTION ai_retry_limits AS
    provider = 'openai', endpoint = 'http://localhost:1/v1/chat/completions', model = 'test-model', api_key = 'fake-key';
CREATE NAMED COLLECTION ai_retry_limits_embed AS
    provider = 'openai', endpoint = 'http://localhost:1/v1/embeddings', api_key = 'fake-key';

DROP TABLE IF EXISTS t_retry_limits;
CREATE TABLE t_retry_limits (x String) ENGINE = Memory;
INSERT INTO t_retry_limits VALUES ('a');

SELECT aiGenerate(x, map('credentials', 'ai_retry_limits')) FROM t_retry_limits SETTINGS ai_function_max_retries = 6; -- { serverError BAD_ARGUMENTS }
SELECT aiGenerate(x, map('credentials', 'ai_retry_limits')) FROM t_retry_limits SETTINGS ai_function_retry_initial_delay_ms = 10001; -- { serverError BAD_ARGUMENTS }
SELECT aiEmbed(x, 'test-model', map('credentials', 'ai_retry_limits_embed')) FROM t_retry_limits SETTINGS ai_function_max_retries = 6; -- { serverError BAD_ARGUMENTS }

-- The maximums themselves are accepted: the request fails against the unreachable endpoint and the row gets an empty result.
SELECT aiGenerate(x, map('credentials', 'ai_retry_limits')) FROM t_retry_limits
SETTINGS ai_function_max_retries = 5, ai_function_retry_initial_delay_ms = 1, ai_function_throw_on_error = 0, ai_function_request_timeout_sec = 3;
SELECT aiGenerate(x, map('credentials', 'ai_retry_limits')) FROM t_retry_limits
SETTINGS ai_function_max_retries = 0, ai_function_retry_initial_delay_ms = 10000, ai_function_throw_on_error = 0, ai_function_request_timeout_sec = 3;

DROP TABLE t_retry_limits;
DROP NAMED COLLECTION ai_retry_limits;
DROP NAMED COLLECTION ai_retry_limits_embed;
