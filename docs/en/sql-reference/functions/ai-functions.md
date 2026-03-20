---
slug: /sql-reference/functions/ai-functions
sidebar_position: 6
sidebar_label: AI Functions
---

# AI Functions

## AI_EMBED

Generates vector embeddings from text using an external embedding API.

**Syntax**

```sql
AI_EMBED(connection_name, model, input_text)
```

**Arguments**

- `connection_name` — Name of a [named collection](/operations/named-collections) containing provider configuration, or an inline URL for HuggingFace TEI endpoints. [String](/sql-reference/data-types/string).
- `model` — Embedding model name (e.g. `'text-embedding-3-small'`). [String](/sql-reference/data-types/string).
- `input_text` — Text to embed. [String](/sql-reference/data-types/string).

The first two arguments must be constants.

**Returned value**

- The embedding vector. [Array(Float32)](/sql-reference/data-types/array).

The result works directly with [`cosineDistance`](/sql-reference/functions/distance-functions#cosineDistance), [`L2Distance`](/sql-reference/functions/distance-functions#L2Distance), and vector similarity indices.

**Named Collection Configuration**

Create a named collection with provider configuration:

```sql
CREATE NAMED COLLECTION my_openai AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/embeddings',
    api_key = 'sk-...',
    max_batch_size = 2048,
    timeout_ms = 30000;
```

Supported providers:
- `openai` — OpenAI, Azure OpenAI, vLLM, LiteLLM, or any OpenAI-compatible API
- `huggingface_tei` — HuggingFace Text Embeddings Inference
- `azure_openai` — Azure OpenAI (set `api_version` as needed)

Required keys: `provider`, `endpoint`.
Optional keys: `api_key`, `max_batch_size` (default 256), `timeout_ms` (default 30000), `api_version`.

For auth-free HuggingFace TEI endpoints, you can pass the URL directly:

```sql
SELECT AI_EMBED('http://localhost:8080', 'BAAI/bge-small-en-v1.5', text) FROM t;
```

**Example**

Semantic search:

```sql
SET allow_experimental_ai_functions = 1;

SELECT
    title,
    cosineDistance(embedding, AI_EMBED('my_conn', 'text-embedding-3-small', 'machine learning')) AS score
FROM articles
ORDER BY score ASC
LIMIT 10;
```

Batch backfill:

```sql
INSERT INTO articles_with_embeddings
SELECT *, AI_EMBED('my_conn', 'text-embedding-3-small', title || ' ' || body)
FROM articles;
```

**Settings**

- `allow_experimental_ai_functions` — Must be set to `1` to use AI functions. Default: `0`.
- `ai_embed_timeout_ms` — Per-request HTTP timeout in milliseconds. Default: `30000`.
- `ai_embed_max_retries` — Max retries per sub-batch for transient errors. Default: `3`.
- `ai_embed_max_rows_per_query` — Safety limit on rows per query. Default: `1000000`.
- `ai_embed_max_parallel_requests` — Max concurrent HTTP requests per block. Default: `4`.
- `ai_embed_cache_max_entries` — Max cached embeddings (0 to disable). Default: `100000`.
- `ai_embed_cache_max_bytes` — Max cache memory in bytes. Default: `536870912` (512 MB).

**Cache**

`AI_EMBED` maintains a cross-query LRU cache to avoid re-embedding identical texts. The cache is keyed by model name and input text. To clear the cache:

```sql
SYSTEM DROP AI EMBED CACHE;
```

## AI_EMBED_OR_NULL

Same as `AI_EMBED`, but returns `NULL` instead of throwing an exception on failure.

**Syntax**

```sql
AI_EMBED_OR_NULL(connection_name, model, input_text)
```

**Returned value**

- The embedding vector, or `NULL` if the embedding request failed. [Nullable(Array(Float32))](/sql-reference/data-types/nullable).

Use this variant for batch operations where partial failures are acceptable. Failed rows are logged at WARNING level and counted in the `AIEmbedFailedRows` profile event.
