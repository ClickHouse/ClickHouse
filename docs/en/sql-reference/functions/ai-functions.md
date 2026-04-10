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
| `max_tokens` | UInt64 | `1024` | Maximum number of output tokens per API call. |

:::note
Any OpenAI-compatible API (e.g. vLLM, Ollama, LiteLLM) can be used by setting `provider = 'openai'` and pointing the `endpoint` to your service.
:::

### Query-level settings {#query-level-settings}

All AI-related settings are listed in [Settings](/operations/settings/settings) under the `ai_` prefix.

## Supported providers {#supported-providers}

| Provider | `provider` value | Chat functions | Notes |
|----------|-----------------|----------------|-------|
| OpenAI | `'openai'` | Yes | Default provider. |
| Anthropic | `'anthropic'` | Yes | Uses `/v1/messages` endpoint. |
| HuggingFace TEI | `'huggingface'` or `'tei'` | Yes | Uses OpenAI-compatible API format. Useful for self-hosted models. |


## Observability {#observability}

AI function activity is tracked through ClickHouse [ProfileEvents](/operations/system-tables/query_log) and [CurrentMetrics](/operations/system-tables/metrics):

| ProfileEvent | Description |
|-------|-------------|
| `AIAPICalls` | Number of HTTP requests made to the AI provider. |
| `AIInputTokens` | Total input tokens consumed. |
| `AIOutputTokens` | Total output tokens consumed. |
| `AICacheHits` | Number of results served from cache. |
| `AICacheMisses` | Number of results that required an API call. |
| `AIRowsProcessed` | Number of rows that received a result. |
| `AIRowsSkipped` | Number of rows skipped (empty input, quota exceeded, error). |

| CurrentMetric | Description |
|-------|-------------|
| `AIThreads` | Number of threads in the AI function thread pool. |
| `AIThreadsActive` | Number of threads in the AI function thread pool running a task. |
| `AIThreadsScheduled` | Number of queued or active jobs in the AI function thread pool. |
| `AICacheSizeInBytes` | Total size of the AI result cache in bytes. |
| `AICacheEntries` | Total number of entries in the AI result cache. |

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

<!--
The inner content of the tags below are replaced at doc framework build time with
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
