---
description: 'Documentation for AI Functions'
sidebar_label: 'AI'
slug: /sql-reference/functions/ai-functions
title: 'AI Functions'
doc_type: 'reference'
---

AI Functions are built-in functions in ClickHouse that you can use to call AI or generate embeddings to work with your data, extract information, classify data, etc...

:::note
AI functions are experimental. Set [`allow_experimental_ai_functions`](/operations/settings/settings#allow_experimental_ai_functions) to enable them.
:::

:::note
AI functions can return unpredictable outputs. The result will highly depend on the quality of the prompt and the model used.
:::

All functions are sharing a common infrastructure that provides:

- **Quota enforcement**: Per-query limits on tokens ([`ai_function_max_input_tokens_per_query`](/operations/settings/settings#ai_function_max_input_tokens_per_query), [`ai_function_max_output_tokens_per_query`](/operations/settings/settings#ai_function_max_output_tokens_per_query)) and API calls ([`ai_function_max_api_calls_per_query`](/operations/settings/settings#ai_function_max_api_calls_per_query)).
- **Retry with backoff**: Transient failures are retried ([`ai_function_max_retries`](/operations/settings/settings#ai_function_max_retries)) with exponential backoff ([`ai_function_retry_initial_delay_ms`](/operations/settings/settings#ai_function_retry_initial_delay_ms)).

## Configuration {#configuration}

AI functions reference a [**named collection**](/operations/named-collections) that stores provider credentials and configuration. Different named collections can be created and used for different functions or functions calls. For example you may want to define a different named collection to use with the text functions (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`) vs the `aiEmbed` function, which require different endpoints and usually use different models.

Example statement to create a named collection with provider credentials, one with a chat endpoint and another with an embedding endpoint:
```sql
CREATE NAMED COLLECTION ai_text_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/chat/completions',
    model = 'gpt-4o-mini',
    api_key = 'sk-...';

-- `aiEmbed` does not read `model` from the named collection; pass it as a positional argument instead.
-- Defining `model` in an `aiEmbed` collection is an error, not silently ignored.
CREATE NAMED COLLECTION ai_embedding_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/embeddings',
    api_key = 'sk-...';
```

### Named collection parameters {#named-collection-parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `provider` | String | — | Model provider. Supported: `'openai'`, `'anthropic'`. See note below. |
| `endpoint` | String | — | API endpoint URL. |
| `model` | String | — | Model name (e.g. `'gpt-4o-mini'`). Used by the text functions; `aiEmbed` requires `model` as a positional argument and errors if `model` is specified in the named collection. |
| `api_key` | String | — | Authentication key for the provider. Optional: when omitted, the auth header is not sent, which allows targeting OpenAI-compatible servers that do not require authentication. |
| `max_tokens` | UInt64 | `1024` | Maximum number of output tokens per API call. |
| `api_version` | String | — | API version string. Used by Anthropic (`'2023-06-01'`). |

:::note
Any OpenAI-compatible API (e.g. vLLM, Ollama, LiteLLM) can be used by setting `provider = 'openai'` and pointing the `endpoint` to your service.
:::

### Selecting credentials {#selecting-credentials}

A function resolves the named collection to use from, in order:

1. the `credentials` key of its parameter map, when present;
2. otherwise the applicable default-credentials setting:
   - [`ai_function_text_default_credentials`](/operations/settings/settings#ai_function_text_default_credentials) for the text functions (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`);
   - [`ai_function_embedding_default_credentials`](/operations/settings/settings#ai_function_embedding_default_credentials) for `aiEmbed`.

If neither is set, the call fails. The text and embedding functions use separate default settings because a chat-completions endpoint differs from an embeddings one.

```sql
SET ai_function_text_default_credentials = 'ai_text_credentials';

-- Uses ai_text_credentials from the setting:
SELECT aiGenerate('What is 2 + 2? Reply with just the number.');

-- Overrides the default for this call:
SELECT aiGenerate('Bonjour', map('credentials', 'other_credentials'));
```

### Parameter map {#parameter-map}

Each function accepts an optional trailing `Map(String, String)` of parameters. All values are strings (quote numbers, e.g. `'0.2'`). Unknown keys are rejected. A key that is present overrides the corresponding named-collection value; a key that is absent falls back to the named collection (for `model`/`max_tokens`) or the built-in default. The exception is `aiEmbed`, which takes `model` as a required positional argument (`aiEmbed(text, model[, params])`) and errors if it is instead set in the parameter map or named collection. This is in order to enforce reproducible embeddings.

The following parameters are common to all the AI functions:

| Key | Description |
|-----|-------------|
| `credentials` | Named collection to use (see above). |
| `model` | Overrides the collection's `model` (text functions only; `aiEmbed` takes `model` as a required positional argument, not a map key). |

Individual functions accept additional, function-specific parameters (such as `max_tokens`, `temperature`, `system_prompt`, `instructions`, and `dimensions`). See each function's reference below for the parameters it accepts and their defaults.

```sql
SELECT aiGenerate(body, map('temperature', '0.2', 'system_prompt', 'You are terse.')) FROM articles;
```

### Query-level settings {#query-level-settings}

All AI-related settings are listed in [Settings](/operations/settings/settings) under the `ai_function_` prefix.

### Use in `DEFAULT` and `MATERIALIZED` columns {#default-and-materialized-columns}

A default-credentials setting is read when the default expression is evaluated, NOT when the column is defined. The collection name is not stored in the column definition unless the expression passes `credentials` in its parameter map:

```sql
CREATE TABLE t (id UInt32, doc String, vector Array(Float32) DEFAULT aiEmbed(doc, 'text-embedding-3-small')) ...;
-- The stored default is `aiEmbed(doc, 'text-embedding-3-small')`; no collection is captured.
```

Evaluating the expression requires three things: `allow_experimental_ai_functions` must be set, the credentials must resolve (from the expression's `credentials` parameter or the applicable default-credentials setting), and the evaluating user must hold `GRANT NAMED COLLECTION` on the collection (resolving the credentials runs a `NAMED COLLECTION` access check). Any of them missing raises an exception (`SUPPORT_IS_DISABLED`, an empty-credentials error, or `ACCESS_DENIED`).

A `DEFAULT` column is evaluated at `INSERT`, so both settings must be set in the inserting session or query:

```sql
GRANT NAMED COLLECTION ON ai_embedding_credentials TO user;
SET allow_experimental_ai_functions = 1;
SET ai_function_embedding_default_credentials = 'ai_embedding_credentials';
INSERT INTO t (id, doc) VALUES (1, 'hello');
```

To make such tables insertable without setting these per session, set both in a [settings profile](/operations/settings/settings-profiles):

```xml
<profiles>
    <default>
        <allow_experimental_ai_functions>1</allow_experimental_ai_functions>
        <ai_function_embedding_default_credentials>ai_embedding_credentials</ai_function_embedding_default_credentials>
    </default>
</profiles>
```

A `MATERIALIZED` column is computed at `INSERT` like a `DEFAULT` column, and is also recomputed by mutations such as `ALTER TABLE ... MATERIALIZE COLUMN`. Mutations run outside a user session and do not inherit a query's `SETTINGS` clause, but they do inherit settings from a settings profile. Set both settings in a settings profile, and grant `NAMED COLLECTION` to the table owner, for mutation-driven recomputation to succeed.

### Restricting endpoint hosts {#restricting-endpoint-hosts}

The `endpoint` URL in an AI named collection is an outbound destination the server connects to under its own identity, potentially carrying (if specified) the named collection's `api_key` in the request headers. By default, ClickHouse permits any host. To restrict functions to a specific set of providers, configure [`remote_url_allow_hosts`](/operations/server-configuration-parameters/settings#remote_url_allow_hosts) in the server config, e.g.:

```xml
<remote_url_allow_hosts>
    <host>api.openai.com</host>
    <host>api.anthropic.com</host>
</remote_url_allow_hosts>
```

Note that this setting is server-wide and applies to all HTTP-using features.

## Supported providers {#supported-providers}

| Provider | `provider` value | Chat functions | Notes |
|----------|-----------------|----------------|-------|
| OpenAI | `'openai'` | Yes | Default provider. |
| Anthropic | `'anthropic'` | Yes | Uses `/v1/messages` endpoint. |


## Observability {#observability}

AI function activity is tracked through ClickHouse [ProfileEvents](/operations/system-tables/query_log):

| ProfileEvent | Description |
|-------|-------------|
| `AIAPICalls` | Number of HTTP requests made to the AI provider. |
| `AIInputTokens` | Total input tokens consumed. |
| `AIOutputTokens` | Total output tokens consumed. |
| `AIRowsProcessed` | Number of rows that received a result. |
| `AIRowsSkipped` | Number of rows skipped (quota exceeded, or error with `ai_function_throw_on_error = 0`). |

Query these events:

```sql
SELECT
    ProfileEvents['AIAPICalls'] AS api_calls,
    ProfileEvents['AIInputTokens'] AS input_tokens,
    ProfileEvents['AIOutputTokens'] AS output_tokens
FROM system.query_log
WHERE query_id = 'query_id'
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
