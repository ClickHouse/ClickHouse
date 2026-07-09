---
description: 'Composable protocols를 사용하면 ClickHouse 서버의 TCP 액세스를 더 유연하게 구성할 수 있습니다.'
sidebar_label: 'Composable protocols'
sidebar_position: 64
slug: /operations/settings/composable-protocols
title: 'Composable protocols'
doc_type: '참고'
---

<div id="overview">
  ## 개요
</div>

Composable protocols를 사용하면 ClickHouse 서버에 대한 TCP 액세스 구성을 더
유연하게 할 수 있습니다. 이 구성은 기존 구성과 함께 사용할 수도 있고, 이를
대체할 수도 있습니다.

<div id="composable-protocols-section-is-denoted-as-protocols-in-configuration-xml">
  ## Composable protocols 구성하기
</div>

Composable protocols는 XML 설정 파일에서 구성할 수 있습니다. XML 설정 파일에서는
`protocols` 태그로 protocols 섹션을 표시합니다:

```xml
<protocols>

</protocols>
```

<div id="basic-modules-define-protocol-layers">
  ### 프로토콜 계층 구성
</div>

기본 모듈로 프로토콜 계층을 정의할 수 있습니다. 예를 들어 HTTP 계층을 정의하려면
`protocols` 섹션에 새 기본 모듈을 추가할 수 있습니다:

```xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```

모듈은 다음 항목에 따라 구성할 수 있습니다:

* `plain_http` - 다른 계층에서 참조할 수 있는 이름
* `type` - 데이터 처리를 위해 인스턴스화할 프로토콜 핸들러를 나타냅니다.
  미리 정의된 프로토콜 핸들러는 다음과 같습니다:
  * `tcp` - 네이티브 ClickHouse 프로토콜 핸들러
  * `http` - HTTP ClickHouse 프로토콜 핸들러
  * `tls` - TLS 암호화 계층
  * `proxy1` - PROXYv1 계층
  * `mysql` - MySQL 호환 프로토콜 핸들러
  * `postgres` - PostgreSQL 호환 프로토콜 핸들러
  * `prometheus` - Prometheus 프로토콜 핸들러
  * `interserver` - ClickHouse interserver 핸들러

:::note
`gRPC` 프로토콜 핸들러는 `Composable protocols`에 구현되어 있지 않습니다
:::

<div id="endpoint-ie-listening-port-is-denoted-by-port-and-optional-host-tags">
  ### 엔드포인트 구성
</div>

엔드포인트(수신 포트)는 `<port>` 및 선택적 `<host>` 태그로 나타냅니다.
예를 들어, 앞서 추가한 HTTP 계층에서 엔드포인트를 구성하려면
구성을 다음과 같이 수정할 수 있습니다.

```xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- endpoint -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```

`<host>` 태그를 생략하면 최상위 구성의 `<listen_host>`가 사용됩니다.

<div id="layers-sequence-is-defined-by-impl-tag-referencing-another-module">
  ### 계층 시퀀스 구성하기
</div>

계층 시퀀스는 `<impl>` 태그를 사용해 정의하고, 다른 모듈을 참조합니다.
예를 들어, plain&#95;http 모듈 위에 TLS 계층을 구성하려면 구성을 다음과 같이 추가로 수정할 수 있습니다.

```xml
<protocols>

  <!-- http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

  <!-- https module configured as a tls layer on top of plain_http module -->
  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="endpoint-can-be-attached-to-any-layer">
  ### 계층에 엔드포인트 연결하기
</div>

모든 계층에 엔드포인트를 연결할 수 있습니다. 예를 들어,
HTTP(포트 8123)와 HTTPS(포트 8443)용 엔드포인트를 정의할 수 있습니다:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="additional-endpoints-can-be-defined-by-referencing-any-module-and-omitting-type-tag">
  ### 추가 엔드포인트 정의
</div>

추가 엔드포인트는 임의의 모듈을 참조하고
`<type>` 태그를 생략하여 정의할 수 있습니다. 예를 들어,
`plain_http` 모듈에 대해 `another_http` 엔드포인트를 다음과 같이 정의할 수 있습니다.

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

  <another_http>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8223</port>
  </another_http>

</protocols>
```

<div id="custom-http-handlers-per-endpoint">
  ### 엔드포인트별 사용자 지정 HTTP 핸들러
</div>

기본적으로 모든 `type=http` 프로토콜 항목은 동일한 `<http_handlers>`
구성을 공유합니다. `<handlers>` 태그를 추가해 다른 구성 섹션을 가리키도록
설정하면 이를 재정의할 수 있습니다. 이렇게 하면 각 HTTP 포트가 서로 다른
HTTP 라우팅 규칙 집합을 제공할 수 있습니다.

예를 들어, 포트 8124에서 자체 핸들러를 사용하는 대체 HTTP API를 실행하려면:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <alt_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8124</port>
    <handlers>http_handlers_alt</handlers>
  </alt_http>

</protocols>

<!-- Default handlers used by plain_http (port 8123) -->
<http_handlers>
    <defaults/>
</http_handlers>

<!-- Alternative handlers used by alt_http (port 8124) -->
<http_handlers_alt>
    <rule>
        <url>/custom</url>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT 'custom_endpoint'</query>
        </handler>
    </rule>
    <defaults/>
</http_handlers_alt>
```

이 예시에서는 포트 8123으로 들어오는 요청에 표준 `<http_handlers>` 규칙이 적용되고,
포트 8124로 들어오는 요청에는 `<http_handlers_alt>` 규칙이 적용됩니다. `<handlers>`를
생략하면 엔드포인트는 기본 `<http_handlers>`로 폴백합니다.

사용자 지정 핸들러 섹션은
[`<http_handlers>`](/ko/docs/operations/server-configuration-parameters/settings#http_handlers)와 동일한 형식을 따릅니다.
사용자 지정 핸들러 섹션의 변경 사항은 구성 다시 로드 중에 감지되며,
해당 엔드포인트는 자동으로 다시 시작됩니다.

<div id="some-modules-can-contain-specific-for-its-layer-parameters">
  ### 추가 계층 매개변수 지정
</div>

일부 모듈은 추가 계층 매개변수를 가질 수 있습니다. 예를 들어, TLS 계층에서는
private key(`privateKeyFile`)와 certificate 파일(`certificateFile`)을
다음과 같이 지정할 수 있습니다:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
    <privateKeyFile>another_server.key</privateKeyFile>
    <certificateFile>another_server.crt</certificateFile>
  </https>

</protocols>
```