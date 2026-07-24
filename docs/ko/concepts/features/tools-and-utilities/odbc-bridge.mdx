---
description: 'Odbc Bridge 문서'
slug: /operations/utilities/odbc-bridge
title: 'clickhouse-odbc-bridge'
doc_type: 'reference'
---

ODBC 드라이버용 프록시처럼 동작하는 단순한 HTTP 서버입니다. 주된 목적은
ODBC 구현에서 발생할 수 있는 segfault나 기타 오류로 인해
clickhouse-server 프로세스 전체가 크래시하는 일을 방지하는 것입니다.

이 도구는 파이프, 공유 메모리, TCP가 아니라 HTTP를 통해 동작합니다. 이유는 다음과 같습니다.

* 구현이 더 간단합니다
* 디버그하기 더 쉽습니다
* jdbc-bridge도 같은 방식으로 구현할 수 있습니다

<div id="usage">
  ## 사용법
</div>

`clickhouse-server`는 ODBC 테이블 함수와 StorageODBC 내부에서 이 도구를 사용합니다.
하지만 다음
매개변수가 포함된 POST 요청 URL을 사용해 명령줄에서 독립 실행형 도구로 사용할 수도
있습니다:

* `connection_string` -- ODBC 연결 문자열입니다.
* `sample_block` -- ClickHouse NamesAndTypesList 포맷의 컬럼 설명입니다. 이름은 백틱으로 감싸고,
  유형은 문자열로 지정합니다. 이름과 유형은 공백으로 구분하며, 행은
  줄바꿈으로 구분합니다.
* `max_block_size` -- 선택적 매개변수로, 단일 블록의 최대 크기를 설정합니다.
  쿼리는 POST 본문으로 전송됩니다. 응답은 RowBinary 포맷으로 반환됩니다.

<div id="example">
  ## 예시:
</div>

```bash
$ clickhouse-odbc-bridge --http-port 9018 --daemon

$ curl -d "query=SELECT PageID, ImpID, AdType FROM Keys ORDER BY PageID, ImpID" --data-urlencode "connection_string=DSN=ClickHouse;DATABASE=stat" --data-urlencode "sample_block=columns format version: 1
3 columns:
\`PageID\` String
\`ImpID\` String
\`AdType\` String
"  "http://localhost:9018/" > result.txt

$ cat result.txt
12246623837185725195925621517
```