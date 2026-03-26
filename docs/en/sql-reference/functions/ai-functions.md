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
| [`embedding_max_batch_size`](/operations/settings/settings#embedding_max_batch_size) | UInt64 | `100` | Maximum number of texts per HTTP request for embedding functions. Texts are grouped into batches of this size to reduce API call overhead. |
| [`ai_on_quota_exceeded`](/operations/settings/settings#ai_on_quota_exceeded) | String | `'throw'` | Behavior when quota is exceeded: `'throw'` raises an exception, `'null'` returns NULL for remaining rows. |

## aiGenerateContent {#aigeneratecontent}

Generates free-form text content from a prompt.

**Syntax**

```sql
aiGenerateContent([collection,] prompt[, system_prompt][, temperature])
```

**Arguments**

- `collection`: Name of the named collection. [String](../data-types/string.md). Optional if [`default_ai_provider`](/operations/settings/settings#default_ai_provider) is set.
- `prompt`: The prompt or question for AI. [String](../data-types/string.md).
- `system_prompt`: System-level instruction for the model. [String](../data-types/string.md). Optional.
- `temperature`: Sampling temperature. Default: `0.7`. [Float64](../data-types/float.md). Optional.

**Returned value**

- The generated text response. Type: [Nullable(String)](../data-types/nullable.md).

**Example**

```sql
SELECT aiGenerateContent('ai_credentials', 'What is 2 + 2? Reply with just the number.') AS answer;
```

```response
┌─answer─┐
│ 4      │
└────────┘
```

Summarize column values:

```sql
SELECT
    article_title,
    aiGenerateContent('ai_credentials', concat('Summarize in one sentence: ', article_body)) AS summary
FROM articles
LIMIT 5;
```

## aiGenerateEmbedding {#generateembedding}

Generates a vector embedding for input text using an embedding model.

This function supports **batch API calls**, when processing multiple rows, texts are grouped into batches (configurable via [`embedding_max_batch_size`](/operations/settings/settings#embedding_max_batch_size)) and sent in a single HTTP request, significantly reducing overhead.

Returns an empty array `[]` for NULL or empty inputs.

**Syntax**

```sql
aiGenerateEmbedding([collection,] text, dimensions)
```

**Arguments**

- `collection`: Name of the named collection). [String](../data-types/string.md). Optional if [`default_ai_provider`](/operations/settings/settings#default_ai_provider) is set.
- `text`: Input text to embed. [String](../data-types/string.md) or [Nullable(String)](../data-types/nullable.md).
- `dimensions`: Dimensionality of the output embedding vector. Must be a constant. [UInt64](../data-types/int-uint.md).

**Returned value**

- The embedding vector. Type: [Array(Float32)](../data-types/array.md). Empty array for NULL or empty inputs.

:::note
The `dimensions` argument must be a constant value. Embedding models typically support specific dimension sizes (e.g. 256, 512, 1536). Check your model's documentation for supported values.
:::

:::note
For embedding functions, the named collection should use an embedding-specific endpoint (e.g. `https://api.openai.com/v1/embeddings`) and an embedding model (e.g. `text-embedding-3-small`).
:::

**Example**

Create an embedding named collection:

```sql
CREATE NAMED COLLECTION my_embeddings AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/embeddings',
    model = 'text-embedding-3-small',
    api_key = 'sk-...';
```

Generate an embedding:

```sql
SELECT aiGenerateEmbedding('my_embeddings', 'ClickHouse is a fast analytics database', 256) AS embedding;
```

**Semantic similarity search**

Use with [cosineDistance](/sql-reference/functions/distance-functions) to find semantically similar texts:

```sql
SELECT
    text,
    cosineDistance(
        aiGenerateEmbedding('my_embeddings', text, 256),
        aiGenerateEmbedding('my_embeddings', 'analytics database', 256)
    ) AS distance
FROM documents
ORDER BY distance ASC
LIMIT 5;
```

**Batch processing**

When processing a table column, texts are automatically batched for efficiency. For example, 1000 rows with 500 unique texts using the default [`embedding_max_batch_size`](/operations/settings/settings#embedding_max_batch_size)`=100` results in only 5 HTTP requests instead of 500:

```sql
SELECT
    text,
    aiGenerateEmbedding('my_embeddings', text, 512) AS embedding
FROM articles;
```

**Null and empty string handling**

NULL and empty inputs return an empty array without making an API call:

```sql
SELECT text, aiGenerateEmbedding('my_embeddings', text, 64) AS emb
FROM (SELECT '' UNION ALL SELECT NULL);
```

```response
┌─text──┬─emb──────────────────┐
│       │ []                   │
│ ᴺᵁᴸᴸ │ []                   │
└───────┴──────────────────────┘
```

## aiGenerateEmbeddingOrNull {#aigenerateembeddingornull}

Same as [generateEmbedding](#generateembedding), but **never throws an exception**. Returns an empty array `[]` on API errors and when quotas are exceeded, instead of raising an exception. The [`ai_on_error`](#query-level-settings) and [`ai_on_quota_exceeded`](#query-level-settings) settings are ignored, both are forced to `'null'` internally. This makes it safe for use in pipelines where a failed row should not abort the entire query.

**Syntax**

```sql
aiGenerateEmbeddingOrNull([collection_or_url,] text, dimensions)
```

**Arguments**

- `collection_or_url`: Name of the named collection, or an inline URL. [String](../data-types/string.md). Optional if [`default_ai_provider`](/operations/settings/settings#default_ai_provider) is set.
- `text`: Input text to embed. [String](../data-types/string.md) or [Nullable(String)](../data-types/nullable.md).
- `dimensions`: Dimensionality of the output embedding vector. Must be a constant. [UInt64](../data-types/int-uint.md).

**Returned value**

- The embedding vector. Type: [Array(Float32)](../data-types/array.md). Empty array for NULL/empty inputs or on API errors.

**Example**

```sql
SELECT aiGenerateEmbeddingOrNull('my_embeddings', text, 256) AS embedding
FROM documents;
```

If the provider returns an API error (timeout, authentication failure, rate limit, etc.) for a batch, `aiGenerateEmbeddingOrNull` returns an empty array for those rows instead of aborting the query:

```sql
SELECT
    text,
    aiGenerateEmbeddingOrNull('my_embeddings', text, 256) AS embedding
FROM documents;
```

## Supported providers {#supported-providers}

| Provider | `provider` value | Chat functions | Embedding functions | Notes |
|----------|-----------------|----------------|---------------------|-------|
| OpenAI | `'openai'` | Yes | Yes | Default provider. |
| Anthropic | `'anthropic'` | Yes | No | Uses `/v1/messages` endpoint. |
| HuggingFace TEI | `'huggingface'` or `'tei'` | Yes | Yes | Uses OpenAI-compatible API format. Useful for self-hosted models. |


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
