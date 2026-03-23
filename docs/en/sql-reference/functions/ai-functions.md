---
description: 'Documentation for AI Functions'
sidebar_label: 'AI'
slug: /sql-reference/functions/ai-functions
title: 'AI Functions'
doc_type: 'reference'
---

# AI functions

AI Functions are built-in functions in ClickHouse that lets you call remote LLMs (Large Language Model) or generate embeddings to work with your data, extract information, classify data, etc...

:::note
AI functions are non-deterministic, meaning that the same input can return different results in different invocations.
Also, the result highly depends on the quality of the prompt and the used model.
:::

All functions are sharing a common infrastructure that provides:

- **Deduplication**: Identical inputs within the same query are sent to the provider only once.
- **Result caching**: Responses are cached with configurable TTL ([`llm_cache_ttl_sec`](/operations/settings/settings#llm_cache_ttl_sec)) to avoid redundant API calls.
- **Concurrency**: Multiple API requests are dispatched in parallel (configurable via [`llm_max_concurrent_requests`](/operations/settings/settings#llm_max_concurrent_requests)).
- **Rate limiting**: Requests per second can be limited using [`llm_max_rps`](/operations/settings/settings#llm_max_rps).
- **Quota enforcement**: Per-query limits on rows ([`llm_max_rows_per_query`](/operations/settings/settings#llm_max_rows_per_query)), tokens ([`llm_max_input_tokens_per_query`](/operations/settings/settings#llm_max_input_tokens_per_query), [`llm_max_output_tokens_per_query`](/operations/settings/settings#llm_max_output_tokens_per_query)), and API calls ([`llm_max_api_calls_per_query`](/operations/settings/settings#llm_max_api_calls_per_query)).
- **Retry with backoff**: Transient failures are retried ([`llm_max_retries`](/operations/settings/settings#llm_max_retries)) with exponential backoff ([`llm_retry_initial_delay_ms`](/operations/settings/settings#llm_retry_initial_delay_ms)).

## Configuration {#configuration}

LLM functions reference a **named collection** that stores the provider credentials and configuration.
The first argument to each function is the name of this collection.
If omitted, the [`default_llm_resource`](/operations/settings/settings#default_llm_resource) setting is used.

```sql
CREATE NAMED COLLECTION my_llm AS
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
| [`default_llm_resource`](/operations/settings/settings#default_llm_resource) | String | `''` | Default named collection used when no collection argument is passed. |
| [`llm_request_timeout_sec`](/operations/settings/settings#llm_request_timeout_sec) | UInt64 | `60` | HTTP timeout per request in seconds. |
| [`llm_max_concurrent_requests`](/operations/settings/settings#llm_max_concurrent_requests) | UInt64 | `16` | Maximum parallel API requests. |
| [`llm_max_rps`](/operations/settings/settings#llm_max_rps) | UInt64 | `50` | Maximum requests per second. |
| [`llm_max_retries`](/operations/settings/settings#llm_max_retries) | UInt64 | `3` | Number of retry attempts on transient failure. |
| [`llm_retry_initial_delay_ms`](/operations/settings/settings#llm_retry_initial_delay_ms) | UInt64 | `1000` | Initial retry delay in milliseconds (doubles on each retry). |
| [`llm_cache_ttl_sec`](/operations/settings/settings#llm_cache_ttl_sec) | UInt64 | `86400` | Cache time-to-live in seconds. Set to `0` to disable caching. |
| [`llm_on_error`](/operations/settings/settings#llm_on_error) | String | `'throw'` | Behavior on API error: `'throw'` raises an exception, `'null'` returns NULL. |
| [`llm_max_rows_per_query`](/operations/settings/settings#llm_max_rows_per_query) | UInt64 | `100000` | Maximum rows processed by AI functions per query. |
| [`llm_max_input_tokens_per_query`](/operations/settings/settings#llm_max_input_tokens_per_query) | UInt64 | `1000000` | Maximum input tokens per query. |
| [`llm_max_output_tokens_per_query`](/operations/settings/settings#llm_max_output_tokens_per_query) | UInt64 | `500000` | Maximum output tokens per query. |
| [`llm_max_api_calls_per_query`](/operations/settings/settings#llm_max_api_calls_per_query) | UInt64 | `1000` | Maximum API calls per query. |
| [`embedding_max_batch_size`](/operations/settings/settings#embedding_max_batch_size) | UInt64 | `100` | Maximum number of texts per HTTP request for embedding functions. Texts are grouped into batches of this size to reduce API call overhead. |
| [`llm_on_quota_exceeded`](/operations/settings/settings#llm_on_quota_exceeded) | String | `'throw'` | Behavior when quota is exceeded: `'throw'` raises an exception, `'null'` returns NULL for remaining rows. |

## LLMClassify {#llmclassify}

Classifies input text into one of the provided categories.

**Syntax**

```sql
LLMClassify([collection,] text, categories[, temperature])
```

**Arguments**

- `collection`: Name of the named collection. [String](../data-types/string.md). Optional if [`default_llm_resource`](/operations/settings/settings#default_llm_resource) is set.
- `text`: Text to classify. [String](../data-types/string.md).
- `categories`: Array of category labels. [Array(String)](../data-types/array.md).
- `temperature`: Sampling temperature. Default: `0.0`. [Float64](../data-types/float.md). Optional.

**Returned value**

- One of the category strings from the `categories` array. Type: [Nullable(String)](../data-types/nullable.md).

**Example**

```sql
SELECT LLMClassify('my_llm', 'I absolutely love ClickHouse!', ['positive', 'negative', 'neutral']) AS sentiment;
```

```response
┌─sentiment─┐
│ positive  │
└───────────┘
```

Classify multiple rows:

```sql
SELECT
    review,
    LLMClassify('my_llm', review, ['positive', 'negative', 'neutral']) AS sentiment
FROM product_reviews
LIMIT 10;
```

## LLMExtract {#llmextract}

Extracts information from text.

**Syntax**

```sql
LLMExtract([collection,] text, what_to_extract[, temperature])
```

**Arguments**

- `collection`: Name of the named collection. [String](../data-types/string.md). Optional if [`default_llm_resource`](/operations/settings/settings#default_llm_resource) is set.
- `text`: Input text to extract from. [String](../data-types/string.md).
- `what_to_extract`: Description of what to extract, or a JSON template defining the output schema (e.g. `'{"company": "company name", "location": "city"}'`). [String](../data-types/string.md).
- `temperature`: Sampling temperature. Default: `0.0`. [Float64](../data-types/float.md). Optional.

**Returned value**

- The extracted text. Type: [Nullable(String)](../data-types/nullable.md).

**Example**

```sql
SELECT LLMExtract('my_llm', 'John Doe works at Acme Corp since 2020.', 'company name') AS company;
```

```response
┌─company───┐
│ Acme Corp │
└───────────┘
```

You can use a JSON as an extraction prompt to define the schema you want back.

```sql
SELECT
    JSONExtractString(info, 'company') AS company,
    JSONExtractString(info, 'location') AS location,
    JSONExtractString(info, 'stack') AS stack,
    JSONExtractString(info, 'contact') AS contact,
    JSONExtractString(info, 'remote') AS remote
FROM
(
    SELECT LLMExtract(
        'my_llm',
        text,
        '{"company": "company name", "location": "city and state or country", "stack": "main technologies, comma-separated", "contact": "email address or application URL if mentioned, or null", "remote": "yes, no, or hybrid"}'
    ) AS info
    FROM default.hackernews
    WHERE parent IN (22665398, 16735011, 15601729)
      AND type = 'comment'
      AND text != ''
      AND length(text) > 100
    ORDER BY cityHash64(id) ASC
    LIMIT 30
    SETTINGS llm_max_rows_per_query = 35, llm_max_rps = 10
)
ORDER BY company ASC;
```

```response
┌─company────────┬─location───────────┬─stack──────────────────────┬─contact─────────────────────────┬─remote─┐
│ Airbnb         │ San Francisco, CA  │ Ruby, React, Java          │ https://careers.airbnb.com      │ hybrid │
│ Datadog        │ New York, NY       │ Go, Python, Kafka          │ jobs@datadoghq.com              │ no     │
│ Fly.io         │ Remote             │ Rust, Go, Elixir           │ https://fly.io/jobs             │ yes    │
│ PlanetScale    │ Remote             │ Go, MySQL, Vitess          │ null                            │ yes    │
│ Stripe         │ San Francisco, CA  │ Ruby, Scala, TypeScript    │ https://stripe.com/jobs         │ hybrid │
└────────────────┴────────────────────┴────────────────────────────┴─────────────────────────────────┴────────┘
```

This works with any JSON schema, you can add or remove keys to control exactly what will be extracted.

## LLMTranslate {#llmtranslate}

Translates text into the specified target language.

**Syntax**

```sql
LLMTranslate([collection,] text, target_language[, instructions][, temperature])
```

**Arguments**

- `collection`: Name of the named collection. [String](../data-types/string.md). Optional if [`default_llm_resource`](/operations/settings/settings#default_llm_resource) is set.
- `text`: Text to translate. [String](../data-types/string.md).
- `target_language`: Target language name (e.g. `'French'`, `'Japanese'`, `'Spanish'`). [String](../data-types/string.md).
- `instructions`: Additional translation instructions (e.g. `'use formal tone'`). [String](../data-types/string.md). Optional.
- `temperature`: Sampling temperature. Default: `0.3`. [Float64](../data-types/float.md). Optional.

**Returned value**

- The translated text. Type: [Nullable(String)](../data-types/nullable.md).

**Example**

```sql
SELECT LLMTranslate('my_llm', 'Hello, how are you?', 'French') AS translated;
```

```response
┌─translated──────────────────┐
│ Bonjour, comment allez-vous? │
└─────────────────────────────┘
```

Translate a whole column:

```sql
SELECT
    original_text,
    LLMTranslate('my_llm', original_text, 'German') AS german_text
FROM articles
LIMIT 5;
```

## LLMGenerateSQL {#llmgeneratesql}

Generates a SQL query from a natural language description using an LLM. The function automatically discovers the database schema from the ClickHouse catalog, introspecting all accessible databases and tables to build context for the LLM.

**Syntax**

```sql
LLMGenerateSQL([collection,] prompt[, temperature])
```

**Arguments**

- `collection`: Name of the named collection. [String](../data-types/string.md). Optional if [`default_llm_resource`](/operations/settings/settings#default_llm_resource) is set.
- `prompt`: Natural language description of the desired query (e.g. `'Count users by country'`). [String](../data-types/string.md).
- `temperature`: Sampling temperature. Default: `0.1`. [Float64](../data-types/float.md). Optional.

**Returned value**

- A SQL query string. Type: [Nullable(String)](../data-types/nullable.md).

**Example**

```sql
SELECT LLMGenerateSQL(
    'my_llm',
    'Find the top 5 customers by total order amount'
) AS generated_sql;
```

```response
┌─generated_sql──────────────────────────────────────────────────────────────────────────┐
│ SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id ORDER BY total DESC LIMIT 5 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## LLMGenerateContent {#llmgeneratecontent}

Generates free-form text content from a prompt.

**Syntax**

```sql
LLMGenerateContent([collection,] prompt[, system_prompt][, temperature])
```

**Arguments**

- `collection`: Name of the named collection. [String](../data-types/string.md). Optional if [`default_llm_resource`](/operations/settings/settings#default_llm_resource) is set.
- `prompt`: The prompt or question for the LLM. [String](../data-types/string.md).
- `system_prompt`: System-level instruction for the model. [String](../data-types/string.md). Optional.
- `temperature`: Sampling temperature. Default: `0.7`. [Float64](../data-types/float.md). Optional.

**Returned value**

- The generated text response. Type: [Nullable(String)](../data-types/nullable.md).

**Example**

```sql
SELECT LLMGenerateContent('my_llm', 'What is 2 + 2? Reply with just the number.') AS answer;
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
    LLMGenerateContent('my_llm', concat('Summarize in one sentence: ', article_body)) AS summary
FROM articles
LIMIT 5;
```

## generateEmbedding {#generateembedding}

Generates a vector embedding for input text using an embedding model.

This function supports **batch API calls**, when processing multiple rows, texts are grouped into batches (configurable via [`embedding_max_batch_size`](/operations/settings/settings#embedding_max_batch_size)) and sent in a single HTTP request, significantly reducing overhead.

Returns an empty array `[]` for NULL or empty inputs.

**Syntax**

```sql
generateEmbedding([collection,] text, dimensions)
```

**Arguments**

- `collection`: Name of the named collection). [String](../data-types/string.md). Optional if [`default_llm_resource`](/operations/settings/settings#default_llm_resource) is set.
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
SELECT generateEmbedding('my_embeddings', 'ClickHouse is a fast analytics database', 256) AS embedding;
```

**Semantic similarity search**

Use with [cosineDistance](/sql-reference/functions/distance-functions) to find semantically similar texts:

```sql
SELECT
    text,
    cosineDistance(
        generateEmbedding('my_embeddings', text, 256),
        generateEmbedding('my_embeddings', 'analytics database', 256)
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
    generateEmbedding('my_embeddings', text, 512) AS embedding
FROM articles;
```

**Null and empty string handling**

NULL and empty inputs return an empty array without making an API call:

```sql
SELECT text, generateEmbedding('my_embeddings', text, 64) AS emb
FROM (SELECT '' UNION ALL SELECT NULL);
```

```response
┌─text──┬─emb──────────────────┐
│       │ []                   │
│ ᴺᵁᴸᴸ │ []                   │
└───────┴──────────────────────┘
```

## generateEmbeddingOrNull {#generateembeddingornull}

Same as [generateEmbedding](#generateembedding), but **never throws an exception**. Returns an empty array `[]` on API errors and when quotas are exceeded, instead of raising an exception. The [`llm_on_error`](#query-level-settings) and [`llm_on_quota_exceeded`](#query-level-settings) settings are ignored, both are forced to `'null'` internally. This makes it safe for use in pipelines where a failed row should not abort the entire query.

**Syntax**

```sql
generateEmbeddingOrNull([collection_or_url,] text, dimensions)
```

**Arguments**

- `collection_or_url`: Name of the named collection, or an inline URL. [String](../data-types/string.md). Optional if [`default_llm_resource`](/operations/settings/settings#default_llm_resource) is set.
- `text`: Input text to embed. [String](../data-types/string.md) or [Nullable(String)](../data-types/nullable.md).
- `dimensions`: Dimensionality of the output embedding vector. Must be a constant. [UInt64](../data-types/int-uint.md).

**Returned value**

- The embedding vector. Type: [Array(Float32)](../data-types/array.md). Empty array for NULL/empty inputs or on API errors.

**Example**

```sql
SELECT generateEmbeddingOrNull('my_embeddings', text, 256) AS embedding
FROM documents;
```

If the provider returns an API error (timeout, authentication failure, rate limit, etc.) for a batch, `generateEmbeddingOrNull` returns an empty array for those rows instead of aborting the query:

```sql
SELECT
    text,
    generateEmbeddingOrNull('my_embeddings', text, 256) AS embedding
FROM documents;
```

## Supported providers {#supported-providers}

| Provider        | `provider` name            | Chat functions | Embedding functions | Notes                                                             |
|-----------------|----------------------------|----------------|---------------------|-------------------------------------------------------------------|
| OpenAI          | `'openai'`                 | Yes            | Yes                 | Default provider.                                                 |
| Anthropic       | `'anthropic'`              | Yes            | No                  | Uses `/v1/messages` endpoint.                                     |
| HuggingFace TEI | `'huggingface'` or `'tei'` | Yes            | Yes                 | Uses OpenAI-compatible API format. Useful for self-hosted models. |


## Debugging {#debugging}

AI functions can be debugged using ClickHouse [ProfileEvents](/operations/system-tables/query_log):

| Event             | Description |
|-------------------|--------------------------------------------------------|
| `AIAPICalls`      | Number of HTTP requests made to the AI provider. |
| `AIInputTokens`   | Total input tokens consumed. |
| `AICacheHits`     | Number of results served from cache. |
| `AICacheMisses`   | Number of results that required an API call. |
| `AIRowsProcessed` | Number of rows that received a result. |
| `AIRowsSkipped`   | Number of rows skipped (NULL, empty, quota exceeded). |

Example:

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
