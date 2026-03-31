---
description: 'Documentation for AI Functions'
sidebar_label: 'AI'
slug: /sql-reference/functions/ai-functions
title: 'AI Functions'
doc_type: 'reference'
---

# AI functions

AI Functions are built-in functions in ClickHouse that you can use to call AI or generate embeddings to work with your data, extract information, classify data, etc...

:::note
AI functions can return unpredictable inputs. The result will highly depend on the quality of the prompt and the model used.
:::

All functions are sharing a common infrastructure that provides:

- **Deduplication**: Identical inputs within the same query are sent to the provider only once.
- **Result caching**: Responses are cached with configurable TTL ([`ai_cache_ttl_sec`](/operations/settings/settings#ai_cache_ttl_sec)) to avoid redundant API calls.
- **Concurrency**: Multiple API requests are dispatched in parallel (configurable via [`ai_max_concurrent_requests`](/operations/settings/settings#ai_max_concurrent_requests)).
- **Rate limiting**: Requests per second can be limited using [`ai_max_rps`](/operations/settings/settings#ai_max_rps).
- **Quota enforcement**: Per-query limits on rows ([`ai_max_rows_per_query`](/operations/settings/settings#ai_max_rows_per_query)), tokens ([`ai_max_input_tokens_per_query`](/operations/settings/settings#ai_max_input_tokens_per_query), [`ai_max_output_tokens_per_query`](/operations/settings/settings#ai_max_output_tokens_per_query)), and API calls ([`ai_max_api_calls_per_query`](/operations/settings/settings#ai_max_api_calls_per_query)).
- **Retry with backoff**: Transient failures are retried ([`ai_max_retries`](/operations/settings/settings#ai_max_retries)) with exponential backoff ([`ai_retry_initial_delay_ms`](/operations/settings/settings#ai_retry_initial_delay_ms)).

## Configuration {#configuration}

AI functions reference a **named collection** that stores provider credentials and configuration. The first argument to each function is the name of this collection. If omitted, the [`default_ai_provider`](/operations/settings/settings#default_ai_provider) setting is used.

```sql
CREATE NAMED COLLECTION ai_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/chat/completions',
    model = 'gpt-4o-mini',
    api_key = 'sk-...';
```

### Named collection parameters {#named-collection-parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `provider` | String | `'openai'` | Model provider. Supported: `'openai'`, `'anthropic'`. |
| `endpoint` | String | — | API endpoint URL. |
| `model` | String | — | Model name (e.g. `'gpt-4o-mini'`, `'text-embedding-3-small'`). |
| `api_key` | String | — | Authentication key for the provider. |

:::note
Any OpenAI-compatible API (e.g. vLLM, Ollama, LiteLLM) can be used by setting `provider = 'openai'` and pointing the `endpoint` to your service.
:::

### Query-level settings {#query-level-settings}

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| [`default_ai_provider`](/operations/settings/settings#default_ai_provider) | String | `''` | Default named collection used when no collection argument is passed. |
| [`ai_request_timeout_sec`](/operations/settings/settings#ai_request_timeout_sec) | UInt64 | `60` | HTTP timeout per request in seconds. |
| [`ai_max_concurrent_requests`](/operations/settings/settings#ai_max_concurrent_requests) | UInt64 | `16` | Maximum parallel API requests. |
| [`ai_max_rps`](/operations/settings/settings#ai_max_rps) | UInt64 | `50` | Maximum requests per second. |
| [`ai_max_retries`](/operations/settings/settings#ai_max_retries) | UInt64 | `3` | Number of retry attempts on transient failure. |
| [`ai_retry_initial_delay_ms`](/operations/settings/settings#ai_retry_initial_delay_ms) | UInt64 | `1000` | Initial retry delay in milliseconds (doubles on each retry). |
| [`ai_cache_ttl_sec`](/operations/settings/settings#ai_cache_ttl_sec) | UInt64 | `86400` | Cache time-to-live in seconds. Set to `0` to disable caching. |
| [`ai_on_error`](/operations/settings/settings#ai_on_error) | String | `'throw'` | Behavior on API error: `'throw'` raises an exception, `'null'` returns NULL. |
| [`ai_max_rows_per_query`](/operations/settings/settings#ai_max_rows_per_query) | UInt64 | `100000` | Maximum rows processed by AI functions per query. |
| [`ai_max_input_tokens_per_query`](/operations/settings/settings#ai_max_input_tokens_per_query) | UInt64 | `1000000` | Maximum input tokens per query. |
| [`ai_max_output_tokens_per_query`](/operations/settings/settings#ai_max_output_tokens_per_query) | UInt64 | `500000` | Maximum output tokens per query. |
| [`ai_max_api_calls_per_query`](/operations/settings/settings#ai_max_api_calls_per_query) | UInt64 | `1000` | Maximum API calls per query. |
| [`ai_on_quota_exceeded`](/operations/settings/settings#ai_on_quota_exceeded) | String | `'throw'` | Behavior when quota is exceeded: `'throw'` raises an exception, `'null'` returns NULL for remaining rows. |

<!--
The inner content of the tags below are replaced at doc framework build time with
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->

## Supported providers {#supported-providers}

| Provider | `provider` value | Chat functions | Notes |
|----------|-----------------|----------------|-------|
| OpenAI | `'openai'` | Yes | Default provider. |
| Anthropic | `'anthropic'` | Yes | Uses `/v1/messages` endpoint. |
| HuggingFace TEI | `'huggingface'` or `'tei'` | Yes | Uses OpenAI-compatible API format. Useful for self-hosted models. |


## Observability {#observability}

AI function activity is tracked through ClickHouse [ProfileEvents](/operations/system-tables/query_log):

| Event | Description |
|-------|-------------|
| `AIAPICalls` | Number of HTTP requests made to the AI provider. |
| `AIInputTokens` | Total input tokens consumed. |
| `AICacheHits` | Number of results served from cache. |
| `AICacheMisses` | Number of results that required an API call. |
| `AIRowsProcessed` | Number of rows that received a result. |
| `AIRowsSkipped` | Number of rows skipped (NULL, empty, quota exceeded). |

Query these events:

```sql
SELECT
    ProfileEvents['AIAPICalls'] AS api_calls,
    ProfileEvents['AICacheHits'] AS cache_hits,
    ProfileEvents['AIInputTokens'] AS tokens
FROM system.query_log
WHERE query='query_id'
AND type = 'QueryFinish'
ORDER BY event_time DESC;
```
