---
description: 'Apache Arrow Flight server를 통해 노출되는 데이터의 읽기 및 쓰기를 허용합니다.'
sidebar_label: 'arrowFlight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'arrowFlight'
doc_type: 'reference'
---

[Apache Arrow Flight](/ko/interfaces/arrowflight) server를 통해 노출되는 데이터의 읽기 및 쓰기를 허용합니다.

**구문**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**인수**

* `host:port` — Arrow Flight 서버의 주소입니다. 포트가 생략되면 기본 포트 `8815`가 사용됩니다. [String](../../sql-reference/data-types/string.md).
* `dataset_name` — Arrow Flight 서버에서 사용할 수 있는 데이터셋 또는 디스크립터의 이름입니다. [String](../../sql-reference/data-types/string.md).
* `username` — 기본 HTTP 인증(authentication)에 사용할 사용자 이름입니다. [String](../../sql-reference/data-types/string.md).
* `password` — 기본 HTTP 인증(authentication)에 사용할 비밀번호입니다. [String](../../sql-reference/data-types/string.md).

`username` 및 `password`를 지정하지 않으면 인증이 사용되지 않습니다(이는 Arrow Flight 서버가 인증 없는 접근을 허용하는 경우에만 동작합니다).

이 함수는 [이름이 지정된 컬렉션](/ko/operations/named-collections)도 지원합니다. 지원되는 매개변수 목록은 [ArrowFlight 테이블 엔진](/ko/engines/table-engines/integrations/arrowflight#named-collections)을 참조하십시오.

**반환 값**

원격 데이터셋을 나타내는 테이블 객체입니다. 스키마는 Arrow Flight 서버에서 자동으로 추론됩니다.

**설정**

* `arrow_flight_request_descriptor_type` — 데이터셋 이름을 Flight 서버로 전송하는 방식을 제어합니다. 값: `path`(기본값) 또는 `command`. 자세한 내용은 [ArrowFlight 테이블 엔진](/ko/engines/table-engines/integrations/arrowflight#settings)을 참조하십시오.

**예시**

원격 Arrow Flight 서버에서 읽기:

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

원격 Arrow Flight 서버에 데이터 삽입:

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

명명된 컬렉션 사용하기:

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**관련 항목**

* [ArrowFlight 테이블 엔진](/ko/engines/table-engines/integrations/arrowflight)
* [Arrow Flight 인터페이스](/ko/interfaces/arrowflight)
* [Apache Arrow Flight SQL 명세](https://arrow.apache.org/docs/format/FlightSql.html)