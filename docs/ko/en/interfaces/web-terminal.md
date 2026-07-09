---
description: 'WebSocket을 통해 브라우저에서 `clickhouse-client` 세션을 대화형으로 사용할 수 있는 웹 터미널 문서'
sidebar_label: '웹 터미널'
sidebar_position: 22
slug: /interfaces/web-terminal
title: '웹 터미널'
doc_type: 'reference'
---

웹 터미널은 WebSocket을 통해 대화형 `clickhouse-client` 세션을 제공하는 브라우저 기반 인터페이스입니다. 모든 ClickHouse HTTP 포트의 `/webterminal` 경로에서 제공됩니다.

터미널을 열려면 ClickHouse HTTP 포트에서 `/webterminal`로 이동하십시오(예: `http://localhost:8123/webterminal`).

<div id="enabling-the-feature">
  ## 기능 활성화 및 비활성화
</div>

`/webterminal` 엔드포인트는 기본적으로 활성화되어 있으며, `enable_webterminal` server setting으로 제어됩니다. 비활성화하려면 해당 설정을 `false`로 지정하세요. 그러면 `/webterminal`에 대한 요청은 HTTP status `403 Forbidden`을 반환합니다.

```xml
<clickhouse>
    <enable_webterminal>false</enable_webterminal>
</clickhouse>
```

:::note
`enable_webterminal`은 이전 `allow_experimental_webterminal` 설정을 대체합니다. `enable_webterminal`이 설정되지 않은 경우에는 이전 버전과의 호환성을 위해 기존 이름도 계속 사용할 수 있습니다.
:::

<div id="authentication">
  ## 인증
</div>

웹 터미널은 HTTP 프로토콜과 동일한 `Session` 및 액세스 제어 검사를 사용해 사용자를 인증하지만, 자격 증명은 HTTP 업그레이드 요청이 아니라 이미 수립된 WebSocket 연결을 통해 인밴드 방식으로 교환됩니다. WebSocket 핸드셰이크가 완료되면 브라우저는 첫 번째 메시지를 JSON으로 전송합니다:

```json
{"type": "auth", "user": "<user>", "password": "<password>"}
```

이렇게 하면 자격 증명을 URL 쿼리 매개변수나 업그레이드 요청에 포함된 `Authorization` 헤더에 넣지 않아도 되므로, 해당 정보가 브라우저 기록, 서버 액세스 로그, 리버스 프록시 로그에 남을 수 있는 위험을 피할 수 있습니다. `/webterminal`은 업그레이드 요청의 URL 매개변수, HTTP Basic, `X-ClickHouse-User`/`X-ClickHouse-Key` 헤더를 의도적으로 **사용하지 않습니다**.

잘못된 자격 증명이 제공되면 서버는 코드 `1008`로 WebSocket 연결을 종료하며, 브라우저 UI는 자격 증명을 다시 입력하라는 메시지를 표시합니다.

<div id="session">
  ## 세션의 모습
</div>

인증이 완료되면 서버는 pseudoterminal에 연결된 `clickhouse-client`를 실행하고, 해당 입출력을 WebSocket을 통해 중계합니다. 이 세션에서는 다음을 포함한 `clickhouse-client`의 전체 기능을 사용할 수 있습니다.

* 구문 강조.
* 자동 완성.
* 여러 줄 쿼리.
* 명령어 이력(세션이 유지되는 동안 서버 측에 저장됨).

터미널 렌더링에는 [xterm.js](https://xtermjs.org/)를 사용합니다. 모든 자산은 ClickHouse 바이너리 자체에서 제공되며, 타사 CDN은 로드되지 않습니다.

<div id="play-integration">
  ## `/play`와의 통합
</div>

[`/play`](/ko/interfaces/http) Web SQL UI에는 웹 터미널이 도킹 가능한 패널로 내장되어 있습니다. 사이드바의 터미널 아이콘으로 표시를 전환하거나, 쿼리 편집기가 비어 있을 때 `~` 키를 눌러 전환할 수 있습니다. `/play` 페이지는 로드 시점에 `/webterminal`의 사용 가능 여부를 감지하며, 엔드포인트를 사용할 수 없으면 터미널 컨트롤을 숨깁니다(예: `enable_webterminal`이 `false`로 설정된 경우).

<div id="security">
  ## 보안 고려 사항
</div>

웹 터미널은 ClickHouse HTTP 엔드포인트에 인증할 수 있는 모든 사용자에게 대화형 셸과 같은 세션을 노출하므로, 여기에도 HTTP 프로토콜에 적용되는 것과 동일한 주의 사항이 적용됩니다:

* 신뢰할 수 없는 환경에서는 자격 증명과 세션 트래픽을 보호하기 위해 항상 HTTPS를 통해 `/webterminal`을 제공하세요.
* HTTP 프로토콜에 대한 접근을 제한하는 것과 같은 방식으로 네트워크 수준에서 접근을 제한하세요(firewall, 리버스 프록시 또는 `listen_host` 구성).
* 이 엔드포인트는 교차 출처 WebSocket 하이재킹을 완화하기 위해 `Origin` 헤더를 `Host`와 대조해 검증하므로, TLS를 외부에서 종료하는 경우 이에 맞게 리버스 프록시를 구성하세요.
* TLS 종료 리버스 프록시 뒤에서는 브라우저가 `https`를 사용하더라도 ClickHouse로의 업스트림 연결은 일반 `http`이므로 엄격한 동일 출처 검사가 정상적인 연결도 거부합니다. 이러한 배포에서는 WebSocket 세션을 열 수 있도록 허용할 전체 origin 목록을 쉼표로 구분해 `webterminal_allowed_origins`에 설정하세요. 이 설정이 비어 있지 않으면 기본 동일 출처 검사를 대체합니다. 예시: `<webterminal_allowed_origins>https://example.com,https://app.example.com:8443</webterminal_allowed_origins>`.

이 핸들러는 또한 RFC 6455에 따라 WebSocket 프로토콜 준수 여부를 강제합니다. 마스킹되지 않은 클라이언트 프레임, 예약된 opcode, 지나치게 크거나 조각난 제어 프레임, 그리고 예약된 RSV 비트는 프로토콜 오류 종료 코드와 함께 거부됩니다.

<div id="platform">
  ## 플랫폼 가용성
</div>

이 핸들러는 ClickHouse가 지원하는 모든 플랫폼에서 컴파일됩니다. 내장 `clickhouse-client` 실행기에 사용되는 pseudoterminal 계층은 이식 가능한 POSIX 기본 요소(`posix_openpt`/`grantpt`/`unlockpt`)를 기반으로 구현되어 있으며, Linux 전용 경로에서는 스레드 안전한 `ptsname_r`를 사용합니다. 엔드포인트를 사용할 수 없으면(예: `enable_webterminal`이 `false`로 설정된 경우) ClickHouse 시작 페이지와 `/play`의 `/webterminal` 링크는 자동으로 숨겨집니다.