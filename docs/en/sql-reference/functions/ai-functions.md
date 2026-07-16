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

AI functions resolve provider credentials and configuration from a [**named collection**](/operations/named-collections). To set a named collection to use for credentials, use the [`ai_function_credentials`](/operations/settings/settings#ai_function_credentials) setting.

Example statement to create a named collection with provider credentials:
```sql
CREATE NAMED COLLECTION my_ai_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/chat/completions',
    model = 'gpt-4o-mini',
    api_key = 'sk-...';
```

Select the collection with the `ai_function_credentials` setting, for the session or for a single query:
```sql
-- For the session:
SET allow_experimental_ai_functions = 1;
SET ai_function_credentials = 'my_ai_credentials';
SELECT aiClassify('I love this product!', ['positive', 'negative', 'neutral']);

-- Or for a single query:
SELECT aiClassify('I love this product!', ['positive', 'negative', 'neutral'])
SETTINGS allow_experimental_ai_functions = 1, ai_function_credentials = 'my_ai_credentials';
```

When `ai_function_credentials` is empty (the default), an exception is raised.

### Named collection parameters {#named-collection-parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `provider` | String | — | Model provider. Supported: `'openai'`, `'anthropic'`. See note below. |
| `endpoint` | String | — | API endpoint URL. |
| `model` | String | — | Model name (e.g. `'gpt-4o-mini'`, `'text-embedding-3-small'`). |
| `api_key` | String | — | Authentication key for the provider. Optional: when omitted, the auth header is not sent, which allows targeting OpenAI-compatible servers that do not require authentication. |
| `max_tokens` | UInt64 | `1024` | Maximum number of output tokens per API call. |
| `api_version` | String | — | API version string. Used by Anthropic (`'2023-06-01'`). |

:::note
Any OpenAI-compatible API (e.g. vLLM, Ollama, LiteLLM) can be used by setting `provider = 'openai'` and pointing the `endpoint` to your service.
:::

### Query-level settings {#query-level-settings}

Which named collection to use is controlled by the [`ai_function_credentials`](/operations/settings/settings#ai_function_credentials) setting. Other AI-related settings are listed in [Settings](/operations/settings/settings) under the `ai_function_` prefix.

### Use in `DEFAULT` and `MATERIALIZED` columns {#default-and-materialized-columns}

The `ai_function_credentials` setting is read when the default expression is evaluated, NOT when the column is defined. The collection name is not stored in the column definition:

```sql
CREATE TABLE t (id UInt32, doc String, vector Array(Float32) DEFAULT aiEmbed(doc)) ...;
-- The stored default is `aiEmbed(doc)`; no collection is captured.
```

Evaluating the expression requires three things: `allow_experimental_ai_functions` and `ai_function_credentials` must be set, and the evaluating user must hold `GRANT NAMED COLLECTION` on the collection (resolving the credentials runs a `NAMED COLLECTION` access check). Any of them missing raises an exception (`SUPPORT_IS_DISABLED`, an empty-credentials error, or `ACCESS_DENIED`).

A `DEFAULT` column is evaluated at `INSERT`, so both settings must be set in the inserting session or query:

```sql
GRANT NAMED COLLECTION ON my_ai_credentials TO user;
SET allow_experimental_ai_functions = 1;
SET ai_function_credentials = 'my_ai_credentials';
INSERT INTO t (id, doc) VALUES (1, 'hello');
```

To make such tables insertable without setting these per session, set both in a [settings profile](/operations/settings/settings-profiles):

```xml
<profiles>
    <default>
        <allow_experimental_ai_functions>1</allow_experimental_ai_functions>
        <ai_function_credentials>my_ai_credentials</ai_function_credentials>
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
