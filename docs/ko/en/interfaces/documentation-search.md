---
description: 'HTTP 인터페이스의 `/docs` 경로에서 제공되며 `system.documentation` 테이블을 기반으로 하는 내장 문서 검색 웹 UI에 대한 문서'
sidebar_label: '문서 검색'
sidebar_position: 23
slug: /interfaces/documentation-search
title: '문서 검색'
doc_type: '참고'
---

문서 검색 페이지는 내장된 참고 문서를 즉시 검색할 수 있는 작고 독립적으로 동작하는 웹 UI입니다. 이 페이지는 모든 ClickHouse HTTP 포트에서 `/docs` 경로로 제공됩니다.

열려면 ClickHouse HTTP 포트의 `/docs`로 이동하세요(예: `http://localhost:8123/docs`).

<div id="what-it-does">
  ## 수행 방식
</div>

이 페이지는 입력하는 동안 HTTP를 통해 [`system.documentation`](/ko/operations/system-tables/documentation) 테이블에 쿼리를 보내고, 선택한 엔터티의 Markdown을 렌더링합니다. `system.documentation`을 읽기 때문에 이 테이블이 노출하는 모든 엔터티(함수, 집계 함수, 테이블 함수, 테이블 엔진, 데이터베이스 엔진, 데이터 타입, 설정, 포맷, 압축 코덱, 프로필 이벤트, 메트릭, 시스템 테이블 자체 등)를 다루며, 항상 실행 중인 서버에 내장된 문서와 일치합니다.

검색 상자에 입력하면 유형별 색상으로 구분된 목록에 일치 항목이 표시되며, 일치 항목을 선택하면 해당 문서가 렌더링됩니다. 렌더링에는 다음이 포함됩니다.

* 엔터티 제목 옆의 연필 링크를 통해 `system.documentation`의 `source` 컬럼에서 가져온 GitHub 원본 파일을 엽니다.
* [`/play`](/ko/interfaces/http) UI와 동일한 내장 렉서(`Lexer.wasm`)를 사용한 코드 블록의 ClickHouse SQL 구문 강조;
* [KaTeX](https://katex.org/)를 통한 TeX 수식(예: `corr` 페이지의 수식);
* `:::note`/`:::tip`/… 안내문, 공유 가능한 링크가 있는 제목 앵커, 코드 블록에 마우스를 올리면 표시되는 &quot;Copy&quot; 버튼;
* 상대 링크는 문서화된 다른 엔터티가 앱 내에 있으면 해당 엔터티로 연결되고, 없으면 `https://clickhouse.com/docs`로 연결됩니다. &quot;Related&quot; 및 &quot;Alias of&quot; 참조도 앱 내 링크로 바뀝니다.

현재 검색어, 열려 있는 엔터티, 섹션은 URL 프래그먼트에 반영되므로 특정 페이지나 섹션에 직접 링크할 수 있으며, 브라우저의 뒤로/앞으로 탐색으로 복원됩니다. 자동 감지를 지원하는 라이트/다크 테마 전환기는 `/play`와 동일하게 동작합니다.

<div id="connecting">
  ## 연결
</div>

헤더에는 `/play`와 정확히 동일하게 `URL`, `user`, `password` 입력란이 있습니다. 페이지가 ClickHouse에서 제공되면 `URL`의 기본값은 현재 오리진이 됩니다. 페이지를 로컬 파일로 열면 기본값은 `http://localhost:8123/`가 되므로, 원격 서버를 대상으로 페이지를 로컬에서 열 수도 있습니다. 연결이 변경되면 교차 링크 이름 캐시가 자동으로 다시 빌드됩니다.

<div id="assets">
  ## 에셋
</div>

Markdown 렌더러([Marked](https://marked.js.org/)), 수식 렌더러(KaTeX 및 해당 글꼴), SQL 렉서를 포함한 모든 에셋은 페이지가 HTTP로 제공될 때 ClickHouse 바이너리 자체에서 제공됩니다. ClickHouse HTTP 오리진에서는 타사 CDN을 로드하지 않으므로, 이 페이지는 필요한 요소를 자체적으로 모두 포함하고 있어 오프라인에서도 작동하며, 처리하는 자격 증명과 함께 타사 네트워크 코드를 실행하지 않습니다.

<div id="security">
  ## 보안 고려 사항
</div>

이 페이지는 헤더에 입력한 자격 증명을 사용해 ClickHouse HTTP endpoint로 쿼리를 전송하므로, HTTP protocol에 적용되는 것과 동일한 주의 사항이 여기에도 적용됩니다:

* 신뢰할 수 없는 환경에서는 자격 증명을 보호할 수 있도록 항상 HTTPS를 통해 `/docs`를 제공하십시오.
* HTTP protocol에 대한 접근을 제한하는 것과 같은 방식으로 네트워크 수준(firewall, 리버스 프록시 또는 `listen_host` 구성)에서 접근을 제한하십시오.

`system.documentation`에는 server에 내장된 정적 참고 문서만 포함되므로, 이 페이지를 통해 테이블의 데이터가 노출되지는 않습니다.