---
description: 'AI 함수 문서'
sidebar_label: 'AI'
slug: /sql-reference/functions/ai-functions
title: 'AI 함수'
doc_type: 'reference'
---

AI 함수는 ClickHouse에 내장된 함수로, AI를 호출하거나 임베딩을 생성해 데이터를 처리하고, 정보를 추출하며, 데이터를 분류하는 등의 작업에 사용할 수 있습니다...

:::note
AI 함수는 실험적 기능입니다. 사용하려면 [`allow_experimental_ai_functions`](/ko/operations/settings/settings#allow_experimental_ai_functions)을 설정하십시오.
:::

:::note
AI 함수는 예측할 수 없는 출력을 반환할 수 있습니다. 결과는 프롬프트의 품질과 사용된 모델에 크게 좌우됩니다.
:::

모든 함수는 다음 기능을 제공하는 공통 인프라를 사용합니다:

* **쿼터 적용**: 토큰 수([`ai_function_max_input_tokens_per_query`](/ko/operations/settings/settings#ai_function_max_input_tokens_per_query), [`ai_function_max_output_tokens_per_query`](/ko/operations/settings/settings#ai_function_max_output_tokens_per_query)) 및 API 호출 수([`ai_function_max_api_calls_per_query`](/ko/operations/settings/settings#ai_function_max_api_calls_per_query))에 대한 쿼리별 제한
* **백오프를 사용한 재시도**: 일시적인 실패는 지수 백오프([`ai_function_retry_initial_delay_ms`](/ko/operations/settings/settings#ai_function_retry_initial_delay_ms))를 적용해 재시도됩니다([`ai_function_max_retries`](/ko/operations/settings/settings#ai_function_max_retries)).

<div id="configuration">
  ## 구성
</div>

AI 함수는 프로바이더 자격 증명과 구성을 저장하는 **명명된 컬렉션**을 참조합니다. 서로 다른 함수 또는 함수 호출에 맞게 서로 다른 명명된 컬렉션을 생성해 사용할 수 있습니다. 예를 들어 텍스트 함수(`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`)에 사용할 명명된 컬렉션과 `aiEmbed` 함수에 사용할 명명된 컬렉션을 별도로 정의할 수 있습니다. 이 함수들은 서로 다른 엔드포인트가 필요하고, 일반적으로 서로 다른 model을 사용합니다.

프로바이더 자격 증명이 포함된 명명된 컬렉션을 생성하는 예시 statement는 다음과 같습니다. 하나는 chat 엔드포인트용이고, 다른 하나는 embedding 엔드포인트용입니다:

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
  ### 명명된 컬렉션 매개변수
</div>

| 매개변수          | 유형     | 기본값    | 설명                                                                                                      |
| ------------- | ------ | ------ | ------------------------------------------------------------------------------------------------------- |
| `provider`    | String | —      | 모델 프로바이더입니다. 지원 값: `'openai'`, `'anthropic'`. 아래 참고 사항을 참조하세요.                                            |
| `endpoint`    | String | —      | API 엔드포인트 URL입니다.                                                                                    |
| `model`       | String | —      | 모델 이름입니다(예: `'gpt-4o-mini'`, `'text-embedding-3-small'`).                                               |
| `api_key`     | String | —      | 프로바이더의 인증 키입니다. 선택 사항입니다. 생략하면 인증 header가 전송되지 않으므로 인증이 필요 없는 OpenAI-compatible 서버를 대상으로 지정할 수 있습니다. |
| `max_tokens`  | UInt64 | `1024` | API 호출당 출력 token의 최대 개수입니다.                                                                             |
| `api_version` | String | —      | API 버전 문자열입니다. Anthropic에서 사용합니다(`'2023-06-01'`).                                                       |

:::note
`provider = 'openai'`로 설정하고 `endpoint`를 해당 서비스로 지정하면 모든 OpenAI-compatible API(예: vLLM, Ollama, LiteLLM)를 사용할 수 있습니다.
:::

<div id="selecting-credentials">
  ### 자격 증명 선택
</div>

함수는 다음 순서에 따라 사용할 명명된 컬렉션을 확인합니다.

1. 있는 경우, 매개변수 맵의 `credentials` 키
2. 그렇지 않으면 해당 기본 자격 증명 설정:
   * 텍스트 함수(`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`)에는 [`ai_function_text_default_credentials`](/ko/operations/settings/settings#ai_function_text_default_credentials)
   * `aiEmbed`에는 [`ai_function_embedding_default_credentials`](/ko/operations/settings/settings#ai_function_embedding_default_credentials)

둘 다 설정되어 있지 않으면 호출이 실패합니다. 텍스트 함수와 임베딩 함수는 chat-completions 엔드포인트와 모델이 embeddings용과 다르므로 각각 별도의 기본 설정을 사용합니다.

```sql
SET ai_function_text_default_credentials = 'ai_text_credentials';

-- Uses ai_text_credentials from the setting:
SELECT aiGenerate('What is 2 + 2? Reply with just the number.');

-- Overrides the default for this call:
SELECT aiGenerate('Bonjour', map('credentials', 'other_credentials'));
```

<div id="parameter-map">
  ### 매개변수 맵
</div>

각 함수는 선택적으로 마지막에 매개변수용 `Map(String, String)`을 받을 수 있습니다. 모든 값은 문자열이어야 합니다(숫자도 `'0.2'`처럼 따옴표로 감싸십시오). 알 수 없는 키는 허용되지 않습니다. 키가 있으면 해당 명명된 컬렉션의 값을 재정의하고, 키가 없으면 명명된 컬렉션(`model`/`max_tokens`의 경우) 또는 내장 기본값을 사용합니다.

다음 매개변수는 모든 AI 함수에 공통으로 적용됩니다.

| Key           | Description           |
| ------------- | --------------------- |
| `credentials` | 사용할 명명된 컬렉션입니다(위 참조). |
| `model`       | 컬렉션의 `model`을 재정의합니다. |

개별 함수는 이 외에도 함수별 추가 매개변수(`max_tokens`, `temperature`, `system_prompt`, `instructions`, `dimensions` 등)를 받을 수 있습니다. 허용되는 매개변수와 기본값은 아래 각 함수의 참고 문서를 확인하십시오.

```sql
SELECT aiGenerate(body, map('temperature', '0.2', 'system_prompt', 'You are terse.')) FROM articles;
```

<div id="query-level-settings">
  ### 쿼리 수준 설정
</div>

모든 AI 관련 설정은 [설정](/ko/operations/settings/settings) 문서에서 `ai_function_` 접두사로 나열되어 있습니다.

<div id="restricting-endpoint-hosts">
  ### 엔드포인트 호스트 제한
</div>

AI 명명된 컬렉션의 `endpoint` URL은 서버가 자체 아이덴티티로 연결하는 아웃바운드 대상이며, 요청 헤더에 명명된 컬렉션의 `api_key`가 포함될 수 있습니다(지정된 경우). 기본적으로 ClickHouse는 모든 호스트를 허용합니다. 함수를 특정 프로바이더 집합으로 제한하려면 서버 구성에서 [`remote_url_allow_hosts`](/ko/operations/server-configuration-parameters/settings#remote_url_allow_hosts)를 설정하십시오. 예를 들면 다음과 같습니다.

```xml
<remote_url_allow_hosts>
    <host>api.openai.com</host>
    <host>api.anthropic.com</host>
</remote_url_allow_hosts>
```

이 설정은 서버 전체에 적용되며 HTTP를 사용하는 모든 기능에 적용된다는 점에 유의하십시오.

<div id="transport-security">
  ### 전송 보안(HTTP vs HTTPS)
</div>

전송 방식은 오직 `endpoint` URL의 스킴으로만 결정됩니다. 요청 payload에 대한 애플리케이션 수준의 암호화는 없으며, 전송 중 데이터 보호는 전적으로 스킴에 달려 있습니다.

* `https://` — 연결에 TLS를 사용합니다. 요청 본문(입력 텍스트, 프롬프트)과 요청 헤더의 `api_key`는 전송 중 암호화되며, 프로바이더의 certificate도 검증됩니다. 원격 프로바이더에는 이 방식을 사용하십시오.
* `http://` — 연결이 **암호화되지 않습니다**. 요청 본문과 `api_key`가 평문으로 전송됩니다. 신뢰할 수 있는 프라이빗 네트워크의 프로바이더(예: 로컬 `vLLM` 또는 `Ollama` instance)에만 사용하십시오.

AI 함수는 HTTPS를 강제하지 않습니다. `http://` 엔드포인트도 허용되며, 데이터는 암호화되지 않은 상태로 전송됩니다. 현재는 평문 AI 엔드포인트를 거부하는 서버 측 설정이 없습니다. [`remote_url_allow_hosts`](/ko/operations/server-configuration-parameters/settings#remote_url_allow_hosts)는 대상 host만 제한할 뿐 URL 스킴은 검사하지 않으므로, 허용된 host를 가리키는 `http://` 엔드포인트도 그대로 허용됩니다. 암호화된 전송을 보장하려면 `https://` 엔드포인트를 사용하도록 명명된 컬렉션을 구성하십시오.

어느 경우든 TLS 종료 이후에는 프로바이더가 입력 데이터를 평문으로 받는다는 점에 유의하십시오. TLS는 server와 프로바이더 사이의 네트워크 경로에서만 데이터를 보호합니다.

<div id="supported-providers">
  ## 지원되는 프로바이더
</div>

| 프로바이더     | `provider` 값  | 채팅 기능 | 비고                              |
| --------- | ------------- | ----- | ------------------------------- |
| OpenAI    | `'openai'`    | 예     | 기본 프로바이더입니다.                    |
| Anthropic | `'anthropic'` | 예     | `/v1/messages` 엔드포인트를 사용합니다. |

<div id="observability">
  ## 관측성
</div>

AI 함수 활동은 ClickHouse [ProfileEvents](/ko/operations/system-tables/query_log)를 통해 추적됩니다:

| ProfileEvent      | Description                                                           |
| ----------------- | --------------------------------------------------------------------- |
| `AIAPICalls`      | AI 프로바이더로 전송된 HTTP 요청 수입니다.                                           |
| `AIInputTokens`   | 소비된 총 입력 토큰 수입니다.                                                     |
| `AIOutputTokens`  | 소비된 총 출력 토큰 수입니다.                                                     |
| `AIRowsProcessed` | 결과가 반환된 행 수입니다.                                                       |
| `AIRowsSkipped`   | 건너뛴 행 수입니다(할당량을 초과했거나 `ai_function_throw_on_error = 0`에서 오류가 발생한 경우). |

다음 이벤트를 쿼리합니다:

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
  아래 태그 내부의 내용은 문서 프레임워크 build 시점에
  system.functions에서 생성된 문서로 대체됩니다. 태그를 수정하거나 제거하지 마십시오.
  참고: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }