---
description: 'HTTP 관련 문서'
slug: /operations/external-authenticators/http
title: 'HTTP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

HTTP 서버를 사용해 ClickHouse 사용자를 인증할 수 있습니다. HTTP 인증은 `users.xml` 또는 로컬 access control 경로에 정의된 기존 사용자에 대해서만 외부 인증기(external authenticator)로 사용할 수 있습니다. 현재는 GET 메서드를 사용하는 [Basic](https://datatracker.ietf.org/doc/html/rfc7617) 인증 방식만 지원합니다.

<div id="http-auth-server-definition">
  ## HTTP 인증 서버 정의
</div>

HTTP 인증 서버를 정의하려면 `config.xml`에 `http_authentication_servers` 섹션을 추가하십시오.

**예시**

```xml
<clickhouse>
    <!- ... -->
    <http_authentication_servers>
        <basic_auth_server>
          <uri>http://localhost:8000/auth</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <forward_headers>
            <name>Custom-Auth-Header-1</name>
            <name>Custom-Auth-Header-2</name>
          </forward_headers>

        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

참고: `http_authentication_servers` 섹션 내에서 서로 다른 이름을 사용해 여러 HTTP 서버를 정의할 수 있습니다.

**매개변수**

* `uri` - 인증 요청을 보내기 위한 URI

서버와 통신하는 데 사용하는 소켓의 밀리초 단위 타임아웃:

* `connection_timeout_ms` - 기본값: 1000 ms.
* `receive_timeout_ms` - 기본값: 1000 ms.
* `send_timeout_ms` - 기본값: 1000 ms.

재시도 매개변수:

* `max_tries` - 인증 요청을 보낼 때의 최대 시도 횟수입니다. 기본값: 3
* `retry_initial_backoff_ms` - 재시도 시 백오프의 초기 간격입니다. 기본값: 50 ms
* `retry_max_backoff_ms` - 백오프의 최대 간격입니다. 기본값: 1000 ms

전달할 헤더:

이 부분에서는 클라이언트 요청 헤더에서 외부 HTTP authenticator로 어떤 헤더를 전달할지 정의합니다. 헤더는 구성에 지정된 헤더와 대소문자를 구분하지 않고 비교되지만, 전달될 때는 수정되지 않은 원래 형태 그대로 전달됩니다.

<div id="enabling-http-auth-in-users-xml">
  ### `users.xml`에서 HTTP 인증 활성화하기
</div>

사용자에 대해 HTTP 인증을 활성화하려면 사용자 정의에서 `password` 또는 이와 유사한 섹션 대신 `http_authentication` 섹션을 지정하십시오.

매개변수:

* `server` - 앞서 설명한 대로 기본 `config.xml` 파일에 구성된 HTTP 인증 서버의 이름입니다.
* `scheme` - HTTP 인증 방식입니다. 현재는 `Basic`만 지원됩니다. 기본값: Basic

예시 (`users.xml`에 추가):

```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <http_authentication>
            <server>basic_server</server>
            <scheme>basic</scheme>
        </http_authentication>
    </test_user_2>
</clickhouse>
```

:::note
HTTP 인증은 다른 authentication 메커니즘과 함께 사용할 수 없습니다. `http_authentication`과 함께 `password` 같은 다른 섹션이 있으면 ClickHouse가 종료됩니다.
:::

<div id="enabling-http-auth-using-sql">
  ### SQL을 사용하여 HTTP 인증 활성화
</div>

ClickHouse에서 [SQL 기반 액세스 제어 및 계정 관리](/ko/operations/access-rights#access-control-usage)가 활성화되어 있으면 HTTP 인증으로 식별되는 사용자도 SQL 문으로 생성할 수 있습니다.

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...또는 scheme를 명시적으로 정의하지 않으면 `Basic`이 기본값입니다

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

<div id="passing-session-settings">
  ### 세션 설정 전달
</div>

HTTP 인증 서버의 응답 본문이 JSON 포맷이며 `settings` 하위 객체를 포함하는 경우, ClickHouse는 그 key: value 쌍을 문자열 값으로 파싱해 인증된 사용자의 현재 세션에 대한 세션 설정으로 적용합니다. 파싱에 실패하면 서버의 응답 본문은 무시됩니다.