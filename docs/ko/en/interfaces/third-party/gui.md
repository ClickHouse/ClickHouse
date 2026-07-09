---
description: 'ClickHouse용 타사 GUI 도구 및 애플리케이션 목록'
sidebar_label: '시각적 인터페이스'
sidebar_position: 28
slug: /interfaces/third-party/gui
title: '타사 개발자의 시각적 인터페이스'
doc_type: '참고'
---

<div id="open-source">
  ## 오픈소스
</div>

<div id="agx">
  ### agx
</div>

[agx](https://github.com/agnosticeng/agx)는 Tauri와 SvelteKit으로 구축된 데스크톱 애플리케이션으로, ClickHouse의 내장 데이터베이스 엔진(chdb)을 사용해 데이터를 탐색하고 쿼리할 수 있는 현대적인 인터페이스를 제공합니다.

* 네이티브 애플리케이션 실행 시 ch-db를 활용합니다.
* 웹 인스턴스 실행 시 ClickHouse 인스턴스에 연결할 수 있습니다.
* Monaco 에디터를 사용해 익숙한 환경에서 작업할 수 있습니다.
* 다양한 데이터 시각화를 제공하며 계속 발전하고 있습니다.

<div id="ch-ui">
  ### ch-ui
</div>

[ch-ui](https://github.com/caioricciuti/ch-ui)는 쿼리 실행과 데이터 시각화를 위해 설계된 간단한 React.js 기반 ClickHouse 데이터베이스 인터페이스입니다. React와 웹용 ClickHouse 클라이언트로 구축되었으며, 세련되고 사용하기 쉬운 UI를 제공해 데이터베이스를 손쉽게 다룰 수 있습니다.

기능:

* ClickHouse Integration: 연결을 쉽게 관리하고 쿼리를 실행할 수 있습니다.
* 반응형 탭 관리: 쿼리 탭, 테이블 탭 등 여러 탭을 동적으로 처리합니다.
* 성능 최적화: 효율적인 캐싱과 상태 관리를 위해 IndexedDB를 활용합니다.
* 로컬 데이터 저장: 모든 데이터는 브라우저에 로컬로 저장되므로 다른 곳으로 전송되지 않습니다.

<div id="chartdb">
  ### ChartDB
</div>

[ChartDB](https://chartdb.io)는 쿼리 한 번으로 ClickHouse를 비롯한 데이터베이스 스키마를 시각화하고 설계할 수 있는 무료 오픈소스 도구입니다. React로 구축되었으며, 시작하는 데 데이터베이스 자격 증명이나 회원 가입이 필요 없는 매끄럽고 사용하기 쉬운 환경을 제공합니다.

기능:

* 스키마 시각화: 테이블 참조 관계를 보여 주는 materialized view와 일반 뷰를 포함한 ER 다이어그램과 함께 ClickHouse 스키마를 즉시 가져와 시각화합니다.
* AI 기반 DDL 내보내기: 더 효율적인 스키마 관리와 문서화를 위해 DDL 스크립트를 손쉽게 생성합니다.
* 다양한 SQL 방언 지원: 여러 SQL 방언과 호환되어 다양한 데이터베이스 환경에서 유연하게 사용할 수 있습니다.
* 회원 가입이나 자격 증명 불필요: 모든 기능을 브라우저에서 바로 사용할 수 있어 간편하고 안전합니다.

[ChartDB 소스 코드](https://github.com/chartdb/chartdb).

<div id="datastoria">
  ### DataStoria
</div>

[DataStoria](https://github.com/FrankChen021/datastoria)는 여러 ClickHouse 클러스터를 한곳에서 관리할 수 있는 AI 기반 웹 콘솔 애플리케이션입니다.

기능:

* **AI 기반 인텔리전스**: 자연어로 데이터를 탐색하고, SQL 쿼리를 최적화하거나 수정하고, 데이터를 시각화합니다.
* **공식 ClickHouse Agent Skills 통합**: [공식 Best Practices](https://github.com/ClickHouse/agent-skills)를 활용해 AI에 데이터베이스 최적화 및 개선 제안을 요청합니다.
* **스마트 오류 진단**: 정확한 줄 번호와 컬럼 강조 표시를 통해 구문 오류를 즉시 찾아내고, 한 번의 클릭으로 AI 기반 수정 제안을 받을 수 있습니다.
* **시스템 테이블 검사**: 강력한 시각화 대시보드와 필터를 사용해 `system.query_log`, `system.query_views_log`, `system.zookeeper`, `system.ddl_distributed_queue`, `system.part_log`, `system.processes`를 심층적으로 살펴보고 클러스터 상태를 빠르게 파악할 수 있습니다.
* **원클릭 Explain**: 시각적 AST 및 파이프라인 뷰로 쿼리 실행 계획을 즉시 이해할 수 있습니다.
* **의존성 그래프**: Materialized Views, 분산 테이블, 외부 시스템 전반에서 테이블 간 관계를 시각화하고 데이터 흐름을 추적합니다.
* **클러스터 모니터링**: 실시간 메트릭, 머지 작업, 복제 상태, 쿼리 성능 등을 통해 모든 노드를 모니터링합니다.
* **Privacy &amp; Security**: 모든 SQL 쿼리는 브라우저에서 ClickHouse 서버로 직접 실행되므로 완전한 프라이버시가 보장됩니다.

[DataStoria 문서](https://docs.datastoria.app).

<div id="datapup">
  ### DataPup
</div>

[DataPup](https://github.com/DataPupOrg/DataPup)은 네이티브 ClickHouse 지원을 제공하는 최신 AI 지원 크로스 플랫폼 데이터베이스 클라이언트입니다.

기능:

* 지능형 제안과 함께 AI 기반 SQL 쿼리 작성을 지원
* 자격 증명을 안전하게 처리하는 네이티브 ClickHouse 연결 지원
* 여러 테마(라이트, 다크, 다채로운 변형)를 제공하는 세련되고 접근성이 뛰어난 인터페이스
* 고급 쿼리 결과 필터링 및 탐색
* 크로스 플랫폼 지원(macOS, Windows, Linux)
* 빠르고 반응성이 뛰어난 성능
* 오픈 소스이며 MIT 라이선스를 따릅니다.

<div id="dory">
  ### Dory
</div>

[Dory](https://github.com/dorylab/dory)는 ClickHouse를 기본으로 지원하며 AI가 내장된 AI 네이티브 SQL 워크스페이스입니다.

기능:

* SQL 생성, 설명, 디버깅을 위한 AI Copilot
* 통합 워크스페이스에서 여러 ClickHouse 클러스터를 관리하고 쿼리 실행
* 스키마 기반 SQL 자동 완성과 다중 탭 쿼리 워크스페이스
* 필터링 및 시각화를 통한 대화형 쿼리 결과 탐색
* 데이터셋 이해를 돕는 AI 기반 테이블 요약
* SSH 터널을 지원하는 ClickHouse 직접 연결
* 라이트, 다크 등 테마를 지원하는 현대적이고 개발자 친화적인 인터페이스
* 크로스 플랫폼 데스크톱 앱(macOS, Windows, Linux) 및 Docker 지원
* 오픈소스이며 MIT 라이선스로 제공됨

<div id="clickhouse-schemaflow-visualizer">
  ### ClickHouse Schema Flow Visualizer
</div>

[ClickHouse Schema Flow Visualizer](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer)는 ClickHouse 테이블 간 관계를 시각화하는 오픈소스 웹 애플리케이션입니다.
이 도구는 ClickHouse 인스턴스에 연결해 `system.tables` 메타데이터(엔진 유형, 의존성, materialized view의 SELECT문)를 파싱하고, 각 엣지에 변환 표현식 레이블이 표시되는 컬럼 수준 관계와 함께 상호작용형 테이블 수준 데이터 흐름 다이어그램을 렌더링합니다. 다이어그램은 Dagre로 배치되며 일반 인라인 SVG로 렌더링되므로 클라이언트 측 다이어그램 런타임은 로드되지 않습니다.

기능:

* 직관적인 사이드바에서 ClickHouse 데이터베이스와 테이블 탐색
* Data Flow 보기: 테이블 수준 업스트림 소스와 다운스트림 materialized view
* Relationships 보기: 각 엣지에 파싱된 변환 표현식(예: `toStartOfHour(scheduled_departure)`, `avgState(delay_minutes)`)이 표시되는 컬럼 수준 매핑
* `MergeTree`, `Replicated*`, `Distributed`, `MaterializedView`, `Dictionary`용 엔진별 아이콘 및 색상 구분
* Relationships 보기에서 컬럼을 클릭해 파이프라인 전체의 데이터 경로 강조
* 실시간 사이드바 필터와 `Ctrl+K` / `⌘K` 명령 팔레트로 원하는 테이블, 컬럼 또는 엔진으로 이동
* 테이블별 행 수와 디스크 크기를 보여주는 선택적 메타데이터 오버레이
* 현재 다이어그램을 독립 실행형 HTML 파일로 내보내기
* ClickHouse에 대한 TLS 연결, 선택적 검증 건너뛰기, 사용자 지정 CA / 클라이언트 인증서 지원

[ClickHouse Schema Flow Visualizer - 소스 코드](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer)

<div id="tabix">
  ### Tabix
</div>

[Tabix](https://github.com/tabixio/tabix) 프로젝트의 ClickHouse용 웹 인터페이스입니다.

기능:

* 추가 소프트웨어를 설치하지 않아도 브라우저에서 ClickHouse를 직접 사용할 수 있습니다.
* 구문 강조(syntax highlighting)를 지원하는 쿼리 편집기.
* 명령 자동 완성.
* 쿼리 실행을 그래픽으로 분석하는 도구.
* 색상 구성표 옵션.

[Tabix 문서](https://tabix.io/doc/).

<div id="houseops">
  ### HouseOps
</div>

[HouseOps](https://github.com/HouseOps/HouseOps)는 OSX, Linux 및 Windows용 UI/IDE입니다.

기능:

* 구문 강조가 지원되는 쿼리 빌더. 응답은 테이블 또는 JSON 보기로 확인할 수 있습니다.
* 쿼리 결과를 CSV 또는 JSON으로 내보낼 수 있습니다.
* 설명이 포함된 프로세스 목록. 쓰기 모드. 프로세스를 중지(`KILL`)할 수 있습니다.
* 데이터베이스 그래프. 모든 테이블과 해당 컬럼을 추가 정보와 함께 표시합니다.
* 컬럼 크기를 빠르게 확인할 수 있습니다.
* 서버 구성.

다음 기능은 개발 예정입니다:

* 데이터베이스 관리.
* 사용자 관리.
* 실시간 데이터 분석.
* 클러스터 모니터링.
* 클러스터 관리.
* 복제된 테이블과 Kafka 테이블 모니터링.

<div id="lighthouse">
  ### LightHouse
</div>

[LightHouse](https://github.com/VKCOM/lighthouse)는 ClickHouse용 경량 웹 인터페이스입니다.

기능:

* 필터링과 메타데이터를 지원하는 테이블(table) 목록
* 필터링과 정렬을 지원하는 테이블 미리 보기
* 읽기 전용 쿼리 실행

<div id="redash">
  ### Redash
</div>

[Redash](https://github.com/getredash/redash)는 데이터 시각화 플랫폼입니다.

ClickHouse를 포함한 여러 데이터 소스를 지원하며, 서로 다른 데이터 소스의 쿼리 결과를 하나의 최종 데이터셋으로 결합할 수 있습니다.

기능:

* 강력한 쿼리 편집기
* 데이터베이스 탐색기
* 데이터를 다양한 형태로 표현할 수 있는 시각화 도구

<div id="grafana">
  ### Grafana
</div>

[Grafana](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/)는 모니터링 및 시각화를 위한 플랫폼입니다.

&quot;Grafana에서는 메트릭이 어디에 저장되어 있든 쿼리하고, 시각화하고, 알림을 설정하고, 이해할 수 있습니다. 팀과 함께 대시보드를 만들고, 탐색하고, 공유하여 데이터 기반 문화를 조성할 수 있습니다. 커뮤니티에서 신뢰받고 사랑받고 있습니다&quot; — grafana.com.

ClickHouse data source plugin은 ClickHouse를 백엔드 데이터베이스로 사용할 수 있도록 지원합니다.

<div id="qryn">
  ### qryn
</div>

[qryn](https://metrico.in)은 ClickHouse용 *(이전 명칭: cLoki)* 고성능 다중 프로토콜 관측성 스택으로, 네이티브 Grafana 통합을 통해 Loki/LogQL, Prometheus/PromQL, OTLP/Tempo, Elastic, InfluxDB 등을 지원하는 모든 에이전트에서 로그, 메트릭, 텔레메트리 트레이스를 수집하고 분석할 수 있습니다.

기능:

* 데이터를 쿼리하고 추출하며 시각화할 수 있는 내장 Explore UI 및 LogQL CLI
* 플러그인 없이 쿼리, 처리, 수집, 트레이싱, 알림을 수행할 수 있는 네이티브 Grafana API 지원
* 로그, 이벤트, 트레이스 등에서 데이터를 동적으로 검색, 필터링, 추출할 수 있는 강력한 파이프라인
* LogQL, PromQL, InfluxDB, Elastic 등과 원활하게 호환되는 수집 및 PUSH API
* Promtail, Grafana-Agent, Vector, Logstash, Telegraf 등 다양한 에이전트와 즉시 사용 가능

<div id="dbeaver">
  ### DBeaver
</div>

[DBeaver](https://dbeaver.io/) - ClickHouse를 지원하는 범용 데스크톱 데이터베이스 클라이언트입니다.

기능:

* 구문 강조와 자동 완성을 지원하는 쿼리 개발.
* 필터 및 메타데이터 검색 기능이 있는 테이블 목록.
* 테이블 데이터 미리보기.
* 전문 검색.

기본적으로 DBeaver는 세션을 사용해 연결하지 않습니다(CLI는 세션을 사용합니다). 세션 지원이 필요한 경우(예: 세션별 설정을 지정해야 하는 경우) 드라이버 연결 속성을 편집하고 `session_id`를 임의의 문자열로 설정하십시오(내부적으로 HTTP 연결을 사용합니다). 그러면 쿼리 창에서 모든 설정을 사용할 수 있습니다.

<div id="clickhouse-cli">
  ### clickhouse-cli
</div>

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli)는 Python 3로 작성된 ClickHouse용 대체 command-line client입니다.

기능:

* 자동 완성.
* 쿼리와 데이터 출력에 대한 구문 강조.
* 데이터 출력용 페이저 지원.
* 사용자 지정 PostgreSQL 스타일 명령.

<div id="clickhouse-flamegraph">
  ### clickhouse-flamegraph
</div>

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph)은 `system.trace_log`를 [플레임 그래프](http://www.brendangregg.com/flamegraphs.html)로 시각화하는 전용 도구입니다.

<div id="clickhouse-plantuml">
  ### clickhouse-plantuml
</div>

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/)은 테이블 스키마의 [PlantUML](https://plantuml.com/) 다이어그램을 생성하는 스크립트입니다.

<div id="clickhouse-table-graph">
  ### ClickHouse table graph
</div>

[ClickHouse table graph](https://github.com/mbaksheev/clickhouse-table-graph)는 ClickHouse 테이블 간 의존성을 시각화하는 간단한 CLI 도구입니다. 이 도구는 `system.tables` 테이블에서 테이블 간 연결 정보를 가져와 [mermaid](https://mermaid.js.org/syntax/flowchart.html) 포맷의 의존성 흐름도를 생성합니다. 이 도구를 사용하면 테이블 의존성을 쉽게 시각화하고 ClickHouse 데이터베이스 내 데이터 흐름을 파악할 수 있습니다. mermaid를 사용하므로 생성된 흐름도는 보기 좋고 Markdown 문서에도 쉽게 추가할 수 있습니다.

<div id="xeus-clickhouse">
  ### xeus-clickhouse
</div>

[xeus-clickhouse](https://github.com/wangfenjin/xeus-clickhouse)는 Jupyter에서 SQL로 ClickHouse 데이터를 쿼리할 수 있도록 지원하는 ClickHouse용 Jupyter 커널입니다.

<div id="mindsdb">
  ### MindsDB Studio
</div>

[MindsDB](https://mindsdb.com/)는 ClickHouse를 포함한 데이터베이스를 위한 오픈소스 AI 레이어로, 최첨단 머신 러닝 모델을 손쉽게 개발, 학습, 배포할 수 있게 해줍니다. MindsDB Studio(GUI)를 사용하면 데이터베이스의 데이터로 새 모델을 학습시키고, 모델이 생성한 예측을 해석하고, 잠재적인 데이터 바이어스를 식별하며, Explainable AI 기능을 사용해 모델 정확도를 평가하고 시각화하여 머신 러닝 모델을 더 빠르게 조정하고 최적화할 수 있습니다.

<div id="dbm">
  ### DBM
</div>

[DBM](https://github.com/devlive-community/dbm) DBM은 ClickHouse를 위한 시각적 관리 도구입니다!

기능:

* 쿼리 이력 지원(페이지네이션, 전체 삭제 등)
* 선택한 SQL 절 기반 쿼리 지원
* 쿼리 강제 종료 지원
* 테이블 관리 지원(메타데이터, 삭제, 미리보기)
* 데이터베이스 관리 지원(삭제, 생성)
* 사용자 지정 쿼리 지원
* 여러 데이터 소스 관리 지원(연결 테스트, 모니터링)
* 모니터링 지원(processor, connection, query)
* 데이터 마이그레이션 지원

<div id="bytebase">
  ### Bytebase
</div>

[Bytebase](https://bytebase.com)는 팀을 위한 웹 기반 오픈 소스 스키마 변경 및 버전 관리 도구입니다. ClickHouse를 포함한 다양한 데이터베이스를 지원합니다.

기능:

* 개발자와 DBA 간 스키마 검토.
* Database-as-Code 방식으로 GitLab과 같은 VCS에서 스키마를 버전 관리하고, 코드 커밋 시 배포를 실행합니다.
* 환경별 정책을 통한 간소화된 배포.
* 전체 마이그레이션 이력.
* 스키마 드리프트 감지.
* 백업 및 복원.
* RBAC.

<div id="zeppelin-interpreter-for-clickhouse">
  ### Zeppelin-Interpreter-for-ClickHouse
</div>

[Zeppelin-Interpreter-for-ClickHouse](https://github.com/SiderZhang/Zeppelin-Interpreter-for-ClickHouse)는 ClickHouse용 [Zeppelin](https://zeppelin.apache.org) 인터프리터입니다. JDBC 인터프리터와 비교했을 때, 장시간 실행되는 쿼리의 timeout을 더 효과적으로 제어할 수 있습니다.

<div id="clickcat">
  ### ClickCat
</div>

[ClickCat](https://github.com/clickcat-project/ClickCat)는 ClickHouse 데이터를 검색하고, 탐색하고, 시각화할 수 있도록 해주는 사용자 친화적인 UI입니다.

기능:

* 설치 없이 SQL 코드를 실행할 수 있는 온라인 SQL Editor입니다.
* 모든 프로세스와 뮤테이션을 확인할 수 있습니다. 아직 완료되지 않은 프로세스는 UI에서 종료할 수 있습니다.
* 메트릭에는 클러스터 분석, 데이터 분석, 쿼리 분석이 포함됩니다.

<div id="clickvisual">
  ### ClickVisual
</div>

[ClickVisual](https://clickvisual.net/) ClickVisual은 가벼운 오픈 소스 로그 쿼리, 분석 및 알림 시각화 플랫폼입니다.

기능:

* 분석용 로그 라이브러리를 원클릭으로 생성할 수 있습니다
* 로그 수집 구성 관리를 지원합니다
* 사용자 정의 인덱스 구성을 지원합니다
* 알림 구성을 지원합니다
* 라이브러리 및 테이블 수준의 세분화된 권한 구성을 지원합니다

<div id="clickmate">
  ### ClickHouse-Mate
</div>

[ClickHouse-Mate](https://github.com/metrico/clickhouse-mate)는 ClickHouse에서 데이터를 검색하고 탐색할 수 있는 Angular 웹 클라이언트이자 사용자 인터페이스입니다.

기능:

* ClickHouse SQL 쿼리 자동 완성
* 빠른 데이터베이스(Database) 및 테이블(Table) 트리 탐색
* 고급 결과 필터링 및 정렬
* 인라인 ClickHouse SQL 문서
* 쿼리 프리셋 및 이력
* 100% 브라우저 기반, 서버/백엔드 없음

이 클라이언트는 GitHub Pages를 통해 즉시 사용할 수 있습니다: https://metrico.github.io/clickhouse-mate/

<div id="uptrace">
  ### Uptrace
</div>

[Uptrace](https://github.com/uptrace/uptrace)는 OpenTelemetry와 ClickHouse를 기반으로 분산 추적과 메트릭을 제공하는 APM 도구입니다.

기능:

* [OpenTelemetry 추적](https://uptrace.dev/opentelemetry/distributed-tracing.html), 메트릭, 로그를 지원합니다.
* AlertManager를 사용해 Email/Slack/PagerDuty 알림을 보낼 수 있습니다.
* 스팬을 집계하기 위한 SQL 유사 쿼리 언어를 제공합니다.
* 메트릭을 쿼리하기 위한 PromQL-like 언어를 제공합니다.
* 사전 구축된 메트릭 대시보드를 제공합니다.
* YAML 구성을 통해 여러 사용자/프로젝트를 지원합니다.

<div id="clickhouse-monitoring">
  ### clickhouse-monitoring
</div>

[clickhouse-monitoring](https://github.com/duyet/clickhouse-monitoring)은 `system.*` 테이블을 기반으로 하는 간단한 Next.js 대시보드로, ClickHouse 클러스터를 모니터링하고 전반적인 상태를 파악하는 데 도움을 줍니다.

기능:

* 쿼리 모니터: 현재 쿼리, 쿼리 이력, 쿼리 리소스(메모리, 읽은 파트, file&#95;open, ...), 비용이 많이 드는 쿼리, 가장 많이 사용되는 테이블 또는 컬럼 등
* 클러스터 모니터: 전체 메모리/CPU 사용량, 분산 큐, 전역 설정, MergeTree 설정, 메트릭 등
* 테이블 및 파트 정보: 컬럼 수준의 세부 정보로 크기, 행 수, 압축, 파트 크기 등을 확인 가능
* 유용한 도구: ZooKeeper 데이터 탐색, 쿼리 EXPLAIN, 쿼리 강제 종료 등
* 메트릭 시각화 차트: 쿼리 및 리소스 사용량, 머지/뮤테이션 수, 머지 성능, 쿼리 성능 등

<div id="ckibana">
  ### CKibana
</div>

[CKibana](https://github.com/TongchengOpenSource/ckibana)는 네이티브 Kibana UI를 사용해 ClickHouse 데이터를 손쉽게 검색하고, 살펴보고, 시각화할 수 있게 해주는 경량 서비스입니다.

기능:

* 네이티브 Kibana UI의 차트 요청을 ClickHouse 쿼리 구문으로 변환합니다.
* 쿼리 성능을 높이기 위해 샘플링 및 캐싱과 같은 고급 기능을 지원합니다.
* ElasticSearch에서 ClickHouse로 이전한 사용자의 학습 부담을 최소화합니다.

<div id="telescope">
  ### Telescope
</div>

[Telescope](https://iamtelescope.net/)는 ClickHouse에 저장된 로그를 탐색하기 위한 현대적인 웹 인터페이스입니다. 세분화된 접근 제어를 바탕으로 로그 데이터를 쿼리하고, 시각화하고, 관리할 수 있는 사용자 친화적인 UI를 제공합니다.

기능:

* 강력한 필터와 맞춤 설정 가능한 필드 선택 기능을 갖춘 깔끔한 반응형 UI.
* 직관적이고 표현력이 뛰어난 로그 필터링을 위한 FlyQL 구문.
* 중첩된 JSON, 맵, 배열 필드를 포함해 `group-by`를 지원하는 시간 기반 그래프.
* 고급 필터링을 위한 선택적 raw SQL `WHERE` 쿼리 지원(권한 검사 포함).
* Saved Views: 쿼리 및 레이아웃에 대한 사용자 지정 UI 구성을 저장하고 공유합니다.
* 역할 기반 접근 제어(RBAC) 및 GitHub 인증 통합.
* ClickHouse 측에는 추가 에이전트나 구성 요소가 필요하지 않습니다.

[Telescope 소스 코드](https://github.com/iamtelescope/telescope) · [라이브 데모](https://demo.iamtelescope.net)

<div id="clicklens">
  ### ClickLens
</div>

[ClickLens](https://ntk148v.github.io/clicklens/)는 ClickHouse 데이터베이스를 관리하고 모니터링할 수 있는 현대적이고 강력하며 사용하기 쉬운 웹 인터페이스입니다. 개발자, 분석가, 관리자(Administrator)가 ClickHouse 클러스터를 효율적으로 다룰 수 있도록 포괄적인 도구 모음을 제공합니다. ClickHouse는 뛰어난 분석용 데이터베이스이지만, CLI나 기본 도구만으로 관리하기는 어려울 수 있습니다. ClickLens는 다음과 같은 기능을 제공하여 이러한 어려움을 해소합니다.

* Discover - 모든 테이블에서 Kibana와 유사한 방식으로 유연하게 데이터를 탐색
* SQL 콘솔 - 구문 강조와 streaming result를 통해 쿼리를 작성, 실행, 분석
* Real-time Monitoring - 클러스터 상태, 쿼리 성능, 리소스 사용량을 실시간으로 확인
* Schema Explorer - 데이터베이스, 테이블, 컬럼, 파트 등을 탐색
* Access Control - UI에서 직접 사용자와 역할을 관리
* Native RBAC - UI 권한이 ClickHouse 권한 부여에서 직접 파생됨

[ClickLens 소스 코드](https://github.com/ntk148v/clicklens)

<div id="chouse-ui">
  ### CHouse UI
</div>

[CHouse UI](https://chouse-ui.com)는 **운영 환경에서 ClickHouse를 사용하는 팀**을 위해 만들어진 오픈소스, 자체 호스팅 ClickHouse 웹 인터페이스입니다. 대부분의 도구는 쿼리 워크스페이스, 대시보드, AI 어시스턴트, 클러스터 모니터처럼 한 가지 영역에 집중합니다. CHouse UI는 이러한 기능을 *결합*한 도구로, 팀 단위 접근 계층에 멀티 클러스터 플릿 모니터링과 자율형 읽기 전용 AI SRE를 함께 제공합니다. 직접 데이터베이스 자격 증명이 필요한 클라이언트와 달리, 자격 증명을 서버 측에 암호화해 저장하고 자체 **역할 기반 접근 제어(RBAC)** 계층으로 접근을 통제하므로 브라우저가 ClickHouse 비밀번호를 직접 보지 않습니다.

기능:

* **팀 접근 및 보안** - 애플리케이션 수준 RBAC(사전 정의된 역할 + 사용자 지정 역할, 데이터베이스/테이블별 세분화된 데이터 접근 규칙), 실제 세션 Context가 포함된 감사 로깅, AES-256-GCM으로 암호화된 서버 측 자격 증명을 제공합니다.
* **멀티 클러스터 플릿** - 구성된 모든 클러스터를 하나의 화면에서 확인할 수 있습니다(status, memory, active queries, exceptions, 추세 스파크라인). 각 카드는 독립적으로 폴링되며 backend snapshot poller가 이를 지원합니다.
* **Chouse AI — Fleet Doctor** - 자율형 읽기 전용 AI SRE입니다. 보호된 `system.*` 전용 `SELECT` 도구(ClickHouse `readonly=1`)로 플릿을 스캔하고, 근본 원인을 짚어내며, 고비용 쿼리 심층 분석과 권장 재작성을 포함한 구조화된 보고서를 작성합니다. 클러스터를 변경하지는 않습니다.
* **모니터링 탭의 AI** - Query Logs 행에서 &quot;Optimize with Chouse AI&quot;를 사용할 수 있으며(재작성 + 변경 전→후 `EXPLAIN` 추정 + SQL 워크스페이스에서 열기), `system.errors` 행이나 part-log entry에서는 한 번의 클릭으로 &quot;Diagnose&quot;를 실행할 수 있습니다.
* **임계값 알림** - 노드 memory %, 쿼리별 memory, 장기 실행 쿼리 규칙을 Slack과 이메일로 전송하며, 임계값 초과 시 자율형 근본 원인 분석이 함께 첨부됩니다.
* **전체 워크스페이스** - Monaco SQL editor, schema explorer, kill 지원이 포함된 live-query view, ClickHouse 네이티브 모니터링(memory breakdown, parts/merges, replica lag, latency percentiles), 데이터 가져오기/내보내기를 제공합니다.

오픈소스(Apache 2.0)이며 온프레미스 우선입니다. 모든 기능이 기본 포함되며 유료 등급은 없습니다.

[CHouse UI 소스 코드](https://github.com/daun-gatal/chouse-ui)

<div id="clickhouse-flow">
  ### clickhouse-flow
</div>

[clickhouse-flow](https://github.com/MikeAmputer/clickhouse-flow)은 ClickHouse 테이블, 뷰, 그리고 materialized view 간의 데이터 흐름과 의존성을 시각화하는 오픈소스 도구입니다.

기능:

* ClickHouse 메타데이터를 바탕으로 스키마 그래프를 자동으로 생성합니다.
* materialized view를 통한 데이터 흐름을 시각화합니다.
* 스키마 구조를 탐색할 수 있는 대화형 UI를 제공합니다.
* 문서화 및 공유를 위해 다이어그램을 PDF 또는 SVG로 내보낼 수 있습니다.
* 개발 환경에서 빠르게 설정할 수 있도록 Docker 기반 배포를 제공합니다.

<div id="commercial">
  ## 상용 제품
</div>

<div id="datagrip">
  ### DataGrip
</div>

[DataGrip](https://www.jetbrains.com/datagrip/)은 ClickHouse 전용 지원을 제공하는 JetBrains의 데이터베이스 IDE입니다. 또한 PyCharm, IntelliJ IDEA, GoLand, PhpStorm 등 다른 IntelliJ 기반 도구에도 내장되어 있습니다.

기능:

* 매우 빠른 코드 자동 완성
* ClickHouse 구문 강조
* 중첩 컬럼, 테이블 엔진 등 ClickHouse 고유 기능 지원
* 데이터 편집기
* 리팩터링
* 검색 및 탐색

<div id="yandex-datalens">
  ### Yandex DataLens
</div>

[Yandex DataLens](https://yandex.cloud/en/services/datalens)은 데이터 시각화 및 분석 서비스입니다.

기능:

* 간단한 막대 차트부터 복잡한 대시보드까지, 다양한 시각화를 제공합니다.
* 대시보드를 공개로 설정할 수 있습니다.
* ClickHouse를 포함한 여러 data source를 지원합니다.
* ClickHouse 기반의 구체화된 데이터 저장소를 제공합니다.

DataLens는 부하가 낮은 프로젝트의 경우 상업적 용도를 포함해 [무료로 사용할 수 있습니다](https://yandex.cloud/en/docs/datalens/pricing).

* [DataLens 문서](https://yandex.cloud/en/docs/datalens/).
* ClickHouse 데이터베이스의 데이터를 시각화하는 [튜토리얼](https://yandex.cloud/en/docs/solutions/datalens/data-from-ch-visualization).

<div id="holistics-software">
  ### Holistics Software
</div>

[Holistics](https://www.holistics.io/)는 풀스택 데이터 플랫폼이자 비즈니스 인텔리전스 도구입니다.

기능:

* 보고서의 이메일, Slack, Google Sheet 예약 발송 자동화.
* 시각화, 버전 관리, 자동 완성, 재사용 가능한 쿼리 구성 요소, 동적 필터를 갖춘 SQL Editor.
* iframe을 통한 보고서 및 대시보드용 임베디드 애널리틱스.
* 데이터 준비 및 ETL 기능.
* 데이터를 관계형으로 매핑하기 위한 SQL 데이터 모델링 지원.

<div id="looker">
  ### Looker
</div>

[Looker](https://looker.com)는 ClickHouse를 포함해 50개 이상의 데이터베이스 방언을 지원하는 데이터 플랫폼이자 비즈니스 인텔리전스 도구입니다. Looker는 SaaS 플랫폼과 자체 호스팅 방식으로 제공됩니다. 사용자는 브라우저에서 Looker를 통해 데이터를 탐색하고, 시각화와 대시보드를 만들고, 보고서를 예약 실행하고, 동료와 인사이트를 공유할 수 있습니다. 또한 Looker는 이러한 기능을 다른 애플리케이션에 내장할 수 있도록 다양한 도구를 제공하며, API를 통해 데이터를 다른 애플리케이션과 통합할 수도 있습니다.

기능:

* LookML을 사용해 쉽고 민첩하게 개발할 수 있습니다. LookML은 보고서 작성자와 최종 사용자를 지원하기 위해 체계적인
  [Data Modeling](https://looker.com/platform/data-modeling)을 지원하는 언어입니다.
* Looker의 [Data Actions](https://looker.com/platform/actions)를 통한 강력한 워크플로 통합.

[Looker에서 ClickHouse를 구성하는 방법](https://docs.looker.com/setup-and-management/database-config/clickhouse)

<div id="seektable">
  ### SeekTable
</div>

[SeekTable](https://www.seektable.com)은 데이터 탐색 및 운영 보고를 위한 셀프서비스 BI 도구입니다. 클라우드 서비스와 자체 호스팅 버전으로 모두 제공됩니다. SeekTable의 보고서는 모든 웹 앱에 내장할 수 있습니다.

기능:

* 비즈니스 사용자가 쉽게 사용할 수 있는 보고서 빌더.
* SQL 필터링과 보고서별 쿼리 사용자 지정을 위한 강력한 보고서 매개변수.
* 네이티브 TCP/IP 엔드포인트와 HTTP(S) 인터페이스(서로 다른 2개의 드라이버)를 통해 ClickHouse에 연결할 수 있습니다.
* 차원/측정값 정의에서 ClickHouse SQL 방언의 모든 기능을 활용할 수 있습니다.
* 자동화된 보고서 생성을 위한 [Web API](https://www.seektable.com/help/web-api-integration).
* 계정 데이터 [백업/복원](https://www.seektable.com/help/self-hosted-backup-restore)을 포함한 보고서 개발 워크플로를 지원합니다. 데이터 모델(큐브) / 보고서 구성은 사람이 읽을 수 있는 XML 형식이며 버전 관리 시스템에 저장할 수 있습니다.

SeekTable은 개인 사용자의 경우 [무료](https://www.seektable.com/help/cloud-pricing)입니다.

[SeekTable에서 ClickHouse 연결을 구성하는 방법](https://www.seektable.com/help/clickhouse-pivot-table)

<div id="chadmin">
  ### Chadmin
</div>

[Chadmin](https://github.com/bun4uk/chadmin)은 ClickHouse 클러스터에서 현재 실행 중인 쿼리와 관련 정보를 시각적으로 확인하고, 필요하면 종료할 수 있는 간단한 UI입니다.

<div id="tablum_io">
  ### TABLUM.IO
</div>

[TABLUM.IO](https://tablum.io/) — ETL 및 시각화를 위한 온라인 쿼리 및 analytics 도구입니다. ClickHouse에 연결하고, 유연한 SQL 콘솔을 통해 데이터를 쿼리할 수 있으며, 정적 파일과 타사 서비스에서 데이터를 로드할 수도 있습니다. TABLUM.IO는 데이터 결과를 차트와 테이블로 시각화할 수 있습니다.

기능:

* ETL: 널리 사용되는 데이터베이스, 로컬 및 원격 파일, API 호출을 통한 데이터 로드
* 구문 강조와 시각적 쿼리 빌더를 갖춘 유연한 SQL 콘솔
* 차트와 테이블을 통한 데이터 시각화
* 데이터 머티리얼라이제이션 및 서브쿼리
* Slack, Telegram 또는 이메일로 데이터 리포트 전송
* 독점 API를 통한 데이터 파이프라이닝
* JSON, CSV, SQL, HTML 포맷으로 데이터 내보내기
* 웹 기반 인터페이스

TABLUM.IO는 자체 호스팅 솔루션(Docker image)으로 실행하거나 클라우드에서 실행할 수 있습니다.
라이선스: 3개월 무료 기간이 제공되는 [상용](https://tablum.io/pricing) 제품입니다.

무료로 [클라우드에서](https://tablum.io/try) 사용해 보십시오.
제품에 대한 자세한 내용은 [TABLUM.IO](https://tablum.io/)에서 확인하십시오.

<div id="ckman">
  ### CKMAN
</div>

[CKMAN](https://www.github.com/housepower/ckman)은 ClickHouse 클러스터를 관리하고 모니터링하는 도구입니다!

기능:

* 브라우저 인터페이스를 통해 클러스터를 빠르고 편리하게 자동 배포
* 클러스터를 확장하거나 축소할 수 있습니다
* 클러스터 데이터의 로드 밸런싱 지원
* 클러스터를 온라인으로 업그레이드
* 페이지에서 클러스터 구성 수정
* 클러스터 노드 모니터링 및 ZooKeeper 모니터링 제공
* 테이블 및 파티션 상태를 모니터링하고 느린 SQL 문을 모니터링
* 사용하기 쉬운 SQL 실행 페이지 제공

<div id="1bench">
  ### 1bench
</div>

[1bench](https://1bench.dev)는 ClickHouse를 우선적으로 지원하는 여러 데이터베이스용 네이티브 데스크톱 GUI로, 서버 개요, 스키마 관리, 벡터 검색, 대규모 결과 집합 탐색까지 지원합니다.

기능:

* 연결 시 서버 개요를 제공합니다 — version, uptime, 실행 중인 쿼리, 활성 머지, 파트 및 스토리지 크기, 레플리카 상태, clusters와 nodes를 한눈에 확인할 수 있습니다.
* Monaco SQL Editor와 함께 시각적 쿼리 빌더(컬럼 선택기, 필터, 정렬, Limit)를 제공하며, 연결별 구문 강조와 쿼리 이력을 지원합니다.
* `MergeTree` 계열, `ORDER BY`, `PARTITION BY`, `SETTINGS`, `Nullable()` 자동 래핑을 지원하는 시각적 `CREATE TABLE` 마법사입니다.
* 네이티브 ClickHouse 유형 처리 — `Nullable`, `Array`, `LowCardinality`, 중첩 객체를 지원합니다.
* 벡터 검색 지원 — `Array(Float32)` embedding 컬럼을 간결한 벡터 cell로 렌더링하고, 2D embedding 시각화와 `cosineDistance`를 통한 유사 항목 찾기를 지원합니다.
* 결과 테이블에서 일괄 저장과 함께 인라인 데이터 편집을 지원하며, ClickHouse의 Native 형식을 사용해 CSV/JSON/SQL 내보내기 및 가져오기를 수행할 수 있습니다.
* 연결 옵션: HTTP/HTTPS, firewall 뒤의 Private clusters를 위한 SSH 터널, 안전한 production 탐색을 위한 선택적 읽기 전용 모드.
* ClickHouse Cloud 및 자체 호스팅 환경에서 동작합니다.