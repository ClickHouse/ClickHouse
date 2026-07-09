---
description: 'AI 関数のドキュメント'
sidebar_label: 'AI'
slug: /sql-reference/functions/ai-functions
title: 'AI 関数'
doc_type: 'reference'
---

AI 関数は ClickHouse に組み込まれている関数で、AI の呼び出しや埋め込みの生成に使用でき、データの処理、情報の抽出、データの分類などを行えます...

:::note
AI 関数は Experimental です。有効にするには [`allow_experimental_ai_functions`](/ja/operations/settings/settings#allow_experimental_ai_functions) を設定してください。
:::

:::note
AI 関数は予測不能な出力を返すことがあります。結果は、プロンプトの品質や使用するモデルに大きく左右されます。
:::

すべての関数は、以下を提供する共通のインフラストラクチャを利用しています。

* **Quota enforcement**: クエリごとのトークン数の上限 ([`ai_function_max_input_tokens_per_query`](/ja/operations/settings/settings#ai_function_max_input_tokens_per_query)、[`ai_function_max_output_tokens_per_query`](/ja/operations/settings/settings#ai_function_max_output_tokens_per_query)) および API 呼び出し回数の上限 ([`ai_function_max_api_calls_per_query`](/ja/operations/settings/settings#ai_function_max_api_calls_per_query)) 。
* **Retry with backoff**: 一時的な障害は、指数バックオフ ([`ai_function_retry_initial_delay_ms`](/ja/operations/settings/settings#ai_function_retry_initial_delay_ms)) を用いて再試行 ([`ai_function_max_retries`](/ja/operations/settings/settings#ai_function_max_retries)) されます。

<div id="configuration">
  ## 構成
</div>

AI 関数は、プロバイダーの認証情報や設定を保存した **named collection** を参照します。関数や関数呼び出しごとに、異なる named collection を作成して使い分けることができます。たとえば、テキスト関数 (`aiGenerate`、`aiClassify`、`aiExtract`、`aiTranslate`) で使う named collection と、異なるエンドポイントが必要で、通常は別のモデルを使用する `aiEmbed` 関数用の named collection は、分けて定義したい場合があります。

以下は、プロバイダーの認証情報を含む named collection を作成するステートメントの例です。1 つはチャットエンドポイント用、もう 1 つは埋め込みエンドポイント用です。

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
  ### Named collection のパラメータ
</div>

| パラメータ         | 型      | 既定値    | 説明                                                                     |
| ------------- | ------ | ------ | ---------------------------------------------------------------------- |
| `provider`    | String | —      | モデルプロバイダー。対応する値: `'openai'`、`'anthropic'`。以下の注記を参照してください。              |
| `endpoint`    | String | —      | API エンドポイント URL。                                                       |
| `model`       | String | —      | モデル名 (例: `'gpt-4o-mini'`、`'text-embedding-3-small'`) 。                 |
| `api_key`     | String | —      | プロバイダー用の認証キー。省略可能です。省略すると認証ヘッダーは送信されないため、認証が不要な OpenAI 互換サーバーを対象にできます。 |
| `max_tokens`  | UInt64 | `1024` | API 呼び出しごとの出力トークンの最大数。                                                 |
| `api_version` | String | —      | API バージョン文字列。Anthropic で使用されます (`'2023-06-01'`) 。                      |

:::note
任意の OpenAI 互換 API (例: vLLM、Ollama、LiteLLM) は、`provider = 'openai'` を設定し、`endpoint` を使用するサービスに向けることで利用できます。
:::

<div id="selecting-credentials">
  ### 認証情報の選択
</div>

関数は、使用する named collection を次の順序で決定します。

1. 存在する場合は、パラメータマップの `credentials` キー。
2. それ以外の場合は、該当するデフォルト認証情報設定。
   * テキスト関数 (`aiGenerate`、`aiClassify`、`aiExtract`、`aiTranslate`) には [`ai_function_text_default_credentials`](/ja/operations/settings/settings#ai_function_text_default_credentials)。
   * `aiEmbed` には [`ai_function_embedding_default_credentials`](/ja/operations/settings/settings#ai_function_embedding_default_credentials)。

どちらも設定されていない場合、呼び出しは失敗します。テキスト関数用と埋め込み関数用でデフォルト設定が分かれているのは、chat-completions のエンドポイントとモデルが embeddings 用のものとは異なるためです。

```sql
SET ai_function_text_default_credentials = 'ai_text_credentials';

-- Uses ai_text_credentials from the setting:
SELECT aiGenerate('What is 2 + 2? Reply with just the number.');

-- Overrides the default for this call:
SELECT aiGenerate('Bonjour', map('credentials', 'other_credentials'));
```

<div id="parameter-map">
  ### パラメータマップ
</div>

各関数は、省略可能な末尾の `Map(String, String)` パラメータを受け取ります。すべての値は文字列です (数値も `'0.2'` のように引用符で囲んでください) 。不明なキーは受け付けられません。キーが指定されている場合は、対応する named collection の値を上書きします。キーが指定されていない場合は、named collection (`model`/`max_tokens` の場合) または組み込みのデフォルト値が使用されます。

次のパラメータは、すべての AI 関数に共通です。

| Key           | Description                     |
| ------------- | ------------------------------- |
| `credentials` | 使用する named collection (上記を参照) 。 |
| `model`       | コレクションの `model` を上書きします。        |

各関数は、これに加えて関数固有の追加パラメータ (`max_tokens`、`temperature`、`system_prompt`、`instructions`、`dimensions` など) も受け付けます。受け付けるパラメータとそのデフォルトについては、以下の各関数のリファレンスを参照してください。

```sql
SELECT aiGenerate(body, map('temperature', '0.2', 'system_prompt', 'You are terse.')) FROM articles;
```

<div id="query-level-settings">
  ### クエリレベルの設定
</div>

AI 関連の設定はすべて、[Settings](/ja/operations/settings/settings) の `ai_function_` プレフィックスに一覧表示されています。

<div id="restricting-endpoint-hosts">
  ### エンドポイントのホストを制限する
</div>

AI named collection の `endpoint` URL は、サーバーが自身の認証情報で接続する外向きの宛先であり、指定されている場合は named collection の `api_key` がリクエストヘッダーに含まれることがあります。デフォルトでは、ClickHouse は任意のホストへの接続を許可します。関数を特定のプロバイダー群に制限するには、サーバーの config で [`remote_url_allow_hosts`](/ja/operations/server-configuration-parameters/settings#remote_url_allow_hosts) を設定します。例:

```xml
<remote_url_allow_hosts>
    <host>api.openai.com</host>
    <host>api.anthropic.com</host>
</remote_url_allow_hosts>
```

この設定はサーバー全体に適用され、HTTP を使用するすべての機能に影響する点に注意してください。

<div id="transport-security">
  ### 転送時のセキュリティ (HTTP と HTTPS)
</div>

転送方式は、`endpoint` URL のスキームのみによって決まります。リクエスト payload に対するアプリケーションレベルの暗号化はなく、転送中データの保護はスキームに完全に依存します。

* `https://` — 接続には TLS が使用されます。リクエストボディ (入力テキスト、プロンプト) と、リクエストヘッダー内の `api_key` は転送中に暗号化され、プロバイダーの証明書も検証されます。リモートプロバイダーを使用する場合は、必ずこちらを使ってください。
* `http://` — 接続は**暗号化されません**。リクエストボディと `api_key` は平文で送信されます。これは、プライベートネットワーク上の信頼できるプロバイダー (たとえばローカルの `vLLM` または `Ollama` インスタンス) に対してのみ使用してください。

AI 関数は HTTPS を強制しません。`http://` の `エンドポイント` も受け入れられ、データは暗号化されないまま送信されます。現時点では、平文の AI エンドポイント を拒否するサーバー側設定はありません。[`remote_url_allow_hosts`](/ja/operations/server-configuration-parameters/settings#remote_url_allow_hosts) は宛先 ホスト のみを制限し、URL スキームは確認しないため、許可された ホスト への `http://` の `エンドポイント` もそのまま通過します。転送を暗号化したい場合は、`https://` の endpoints を使って named collections を設定してください。

なお、いずれの場合も、TLS 終端後はプロバイダーが入力データを平文で受け取ります。TLS が保護するのは、サーバーとプロバイダーの間のネットワーク経路上のデータだけです。

<div id="supported-providers">
  ## 対応プロバイダー
</div>

| プロバイダー    | `provider` の値 | チャット関数 | 注記                            |
| --------- | ------------- | ------ | ----------------------------- |
| OpenAI    | `'openai'`    | はい     | デフォルトのプロバイダーです。               |
| Anthropic | `'anthropic'` | はい     | `/v1/messages` エンドポイントを使用します。 |

<div id="observability">
  ## オブザーバビリティ
</div>

AI 関数の動作は、ClickHouse の [ProfileEvents](/ja/operations/system-tables/query_log) で追跡できます。

| ProfileEvent      | 説明                                                                       |
| ----------------- | ------------------------------------------------------------------------ |
| `AIAPICalls`      | AI プロバイダーに送信された HTTP リクエスト数。                                             |
| `AIInputTokens`   | 消費された入力トークンの合計数。                                                         |
| `AIOutputTokens`  | 消費された出力トークンの合計数。                                                         |
| `AIRowsProcessed` | 結果が返された行数。                                                               |
| `AIRowsSkipped`   | スキップされた行数 (クォータ超過、または `ai_function_throw_on_error = 0` の場合に発生した error) 。 |

これらのイベントは次のようにクエリできます。

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
  以下のタグ内の内容は、ドキュメントフレームワークのビルド時に
  system.functions から生成されたドキュメントに差し替えられます。タグは変更または削除しないでください。
  参照: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }