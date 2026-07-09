---
description: 'AI 函数文档'
sidebar_label: 'AI'
slug: /sql-reference/functions/ai-functions
title: 'AI 函数'
doc_type: 'reference'
---

AI 函数是 ClickHouse 中的内置函数，可用于调用 AI 或生成嵌入向量，以处理数据、提取信息、对数据进行分类等……

:::note
AI 函数目前处于 Experimental 状态。请设置 [`allow_experimental_ai_functions`](/zh/operations/settings/settings#allow_experimental_ai_functions) 以启用这些函数。
:::

:::note
AI 函数返回的输出可能不可预测。结果在很大程度上取决于 prompt 的质量以及所使用的模型。
:::

所有函数都共享一套通用基础设施，提供：

* **配额强制执行**：对每个查询的标记数 ([`ai_function_max_input_tokens_per_query`](/zh/operations/settings/settings#ai_function_max_input_tokens_per_query)、[`ai_function_max_output_tokens_per_query`](/zh/operations/settings/settings#ai_function_max_output_tokens_per_query)) 和 API 调用次数 ([`ai_function_max_api_calls_per_query`](/zh/operations/settings/settings#ai_function_max_api_calls_per_query)) 施加限制。
* **带退避的重试机制**：暂时性故障会自动重试 ([`ai_function_max_retries`](/zh/operations/settings/settings#ai_function_max_retries)) ，并采用指数退避 ([`ai_function_retry_initial_delay_ms`](/zh/operations/settings/settings#ai_function_retry_initial_delay_ms)) 。

<div id="configuration">
  ## 配置
</div>

AI 函数会引用一个**命名集合**，其中存储了提供商凭据和配置信息。可以针对不同的函数或函数调用创建并使用不同的命名集合。例如，你可能希望为文本函数 (`aiGenerate`、`aiClassify`、`aiExtract`、`aiTranslate`) 定义一个命名集合，而为 `aiEmbed` 函数定义另一个，因为它们需要不同的端点，而且通常使用不同的模型。

下面是创建命名集合的示例语句：其中一个包含聊天端点，另一个包含嵌入端点，二者都带有提供商凭据：

```sql
CREATE NAMED COLLECTION ai_text_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/chat/completions',
    model = 'gpt-4o-mini',
    api_key = 'sk-...';

CREATE NAMED COLLECTION ai_embedding_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/embeddings',
    model = 'text-embedding-3-small',
    api_key = 'sk-...';
```

<div id="named-collection-parameters">
  ### 命名集合参数
</div>

| 参数            | 类型     | 默认值    | 描述                                                                  |
| ------------- | ------ | ------ | ------------------------------------------------------------------- |
| `provider`    | String | —      | 模型提供商。支持：`'openai'`、`'anthropic'`。参见下方说明。                           |
| `endpoint`    | String | —      | API 端点 URL。                                                         |
| `model`       | String | —      | 模型名称 (例如 `'gpt-4o-mini'`、`'text-embedding-3-small'`) 。              |
| `api_key`     | String | —      | 用于提供商身份验证的密钥。可选：省略时不会发送身份验证请求头，因此可用于不需要身份验证的 OpenAI-compatible 服务器。 |
| `max_tokens`  | UInt64 | `1024` | 每次 API 调用的最大输出标记数。                                                  |
| `api_version` | String | —      | API 版本字符串。Anthropic 使用此参数 (`'2023-06-01'`) 。                        |

:::note
任何 OpenAI-compatible API (例如 vLLM、Ollama、LiteLLM) 都可以通过设置 `provider = 'openai'` 并将 `endpoint` 指向你的服务来使用。
:::

<div id="selecting-credentials">
  ### 选择凭据
</div>

函数会按以下顺序解析要使用的命名集合：

1. 如果存在，使用其参数映射中的 `credentials` 键；
2. 否则，使用适用的默认凭据设置：
   * 文本函数 (`aiGenerate`、`aiClassify`、`aiExtract`、`aiTranslate`) 使用 [`ai_function_text_default_credentials`](/zh/operations/settings/settings#ai_function_text_default_credentials)；
   * `aiEmbed` 使用 [`ai_function_embedding_default_credentials`](/zh/operations/settings/settings#ai_function_embedding_default_credentials)。

如果两者都未设置，调用将失败。文本函数和嵌入函数分别使用不同的默认设置，因为 chat-completions 的端点和模型与 embeddings 的端点和模型不同。

```sql
SET ai_function_text_default_credentials = 'ai_text_credentials';

-- Uses ai_text_credentials from the setting:
SELECT aiGenerate('What is 2 + 2? Reply with just the number.');

-- Overrides the default for this call:
SELECT aiGenerate('Bonjour', map('credentials', 'other_credentials'));
```

<div id="parameter-map">
  ### 参数映射
</div>

每个函数都接受一个可选的尾随 `Map(String, String)` 参数。所有值都必须是字符串 (数字也要加引号，例如 `'0.2'`) 。不支持未知键。已提供的键会覆盖命名集合中的对应值；未提供的键则会回退到命名集合中的值 (对于 `model`/`max_tokens`) ，或使用内置默认值。

以下参数是所有 AI 函数通用的：

| Key           | Description      |
| ------------- | ---------------- |
| `credentials` | 要使用的命名集合 (见上文) 。 |
| `model`       | 覆盖集合中的 `model`。  |

各个函数还接受额外的函数专用参数 (例如 `max_tokens`、`temperature`、`system_prompt`、`instructions` 和 `dimensions`) 。每个函数可接受的参数及其默认值，请参阅下方对应的参考说明。

```sql
SELECT aiGenerate(body, map('temperature', '0.2', 'system_prompt', 'You are terse.')) FROM articles;
```

<div id="query-level-settings">
  ### 查询级别设置
</div>

所有与 AI 相关的设置均列于 [设置](/zh/operations/settings/settings) 中，且都以 `ai_function_` 为前缀。

<div id="restricting-endpoint-hosts">
  ### 限制端点主机
</div>

AI 命名集合中的 `endpoint` URL 是服务器以自身身份连接的出站目标端，并且可能会在请求头中携带该命名集合的 `api_key` (如果已指定) 。默认情况下，ClickHouse 允许任何主机。要将函数限制为一组特定的提供商，请在服务器配置中设置 [`remote_url_allow_hosts`](/zh/operations/server-configuration-parameters/settings#remote_url_allow_hosts)，例如：

```xml
<remote_url_allow_hosts>
    <host>api.openai.com</host>
    <host>api.anthropic.com</host>
</remote_url_allow_hosts>
```

请注意，此设置在整个服务器范围内全局生效，并适用于所有使用 HTTP 的功能。

<div id="transport-security">
  ### 传输安全 (HTTP 与 HTTPS)
</div>

传输方式完全由 `endpoint` URL 的 scheme 决定。应用层不会对请求载荷进行加密；传输中数据的保护完全取决于所使用的 scheme：

* `https://` — 连接使用 TLS。请求体 (输入文本、提示词) 以及请求头中的 `api_key` 都会在传输过程中加密，并且会验证提供商的证书。任何远程提供商都应使用这种方式。
* `http://` — 连接**不加密**。请求体和 `api_key` 会以明文发送。仅应在私有网络中的受信任提供商上使用这种方式 (例如本地 `vLLM` 或 `Ollama` 实例) 。

AI 函数不会强制使用 HTTPS：`http://` 端点会被接受，并以未加密方式发送数据。目前还没有可拒绝明文 AI 端点的服务器端设置——[`remote_url_allow_hosts`](/zh/operations/server-configuration-parameters/settings#remote_url_allow_hosts) 只限制目标主机，不检查 URL scheme，因此，指向已允许主机的 `http://` 端点仍然会通过。要确保传输加密，请将命名集合配置为使用 `https://` 端点。

请注意，无论采用哪种方式，提供商在 TLS 终止后接收到的输入数据都是明文；TLS 仅保护服务器与提供商之间网络路径上的数据。

<div id="supported-providers">
  ## 支持的提供商
</div>

| 提供商       | `provider` 值  | 聊天功能 | 说明                    |
| --------- | ------------- | ---- | --------------------- |
| OpenAI    | `'openai'`    | 是    | 默认提供商。                |
| Anthropic | `'anthropic'` | 是    | 使用 `/v1/messages` 端点。 |

<div id="observability">
  ## 可观测性
</div>

AI 函数活动会通过 ClickHouse [ProfileEvents](/zh/operations/system-tables/query_log) 进行跟踪：

| ProfileEvent      | Description                                               |
| ----------------- | --------------------------------------------------------- |
| `AIAPICalls`      | 向 AI 提供商发出的 HTTP 请求数。                                     |
| `AIInputTokens`   | 消耗的输入标记总数。                                                |
| `AIOutputTokens`  | 消耗的输出标记总数。                                                |
| `AIRowsProcessed` | 获得结果的行数。                                                  |
| `AIRowsSkipped`   | 被跳过的行数 (超出配额，或在 `ai_function_throw_on_error = 0` 时发生错误) 。 |

查询这些事件：

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

{/*
  下面这些标签中的内容会在文档框架构建时
  替换为根据 system.functions 生成的文档。请不要修改或删除这些标签。
  参见：https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }