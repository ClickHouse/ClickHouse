---
description: '이 엔진을 사용하면 Apache Arrow Flight 프로토콜을 통해 원격 데이터셋을 쿼리하고 데이터를 삽입할 수 있습니다.'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'ArrowFlight 테이블 엔진'
doc_type: 'reference'
---

ArrowFlight 테이블 엔진을 사용하면 ClickHouse가 [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) 프로토콜을 통해 원격 데이터셋에서 데이터를 읽고 쓸 수 있습니다.
이 통합을 통해 ClickHouse는 외부 Flight 지원 서버와 열 지향 Arrow 형식으로 고성능 상호 작용을 수행할 수 있습니다.

<div id="creating-a-table">
  ## 테이블(Table) 만들기
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**엔진 매개변수**

* `host:port` — 원격 Arrow Flight 서버의 주소입니다. 포트를 생략하면 기본 포트 `8815`를 사용합니다. [String](../../../sql-reference/data-types/string.md).
* `dataset_name` — Flight 서버의 데이터셋 식별자입니다(`arrow_flight_request_descriptor_type` 설정에 따라 PATH 디스크립터로 사용되거나 `SELECT *` 쿼리에서 사용됨). [String](../../../sql-reference/data-types/string.md).
* `username` — 기본 HTTP authentication에 사용할 사용자 이름입니다. [String](../../../sql-reference/data-types/string.md).
* `password` — 기본 HTTP authentication에 사용할 비밀번호입니다. [String](../../../sql-reference/data-types/string.md).

`username` 및 `password`를 생략하면 authentication을 사용하지 않습니다(이 방식은 Arrow Flight 서버가 인증 없이 접근하는 것을 허용하는 경우에만 동작합니다).

컬럼 목록은 선택 사항입니다. 생략하면 `GetSchema`를 통해 원격 Arrow Flight 서버에서 스키마를 추론합니다.

<div id="named-collections">
  ## 이름이 지정된 컬렉션
</div>

엔진은 연결 매개변수를 저장하는 데 [이름이 지정된 컬렉션](/ko/operations/named-collections)을 지원합니다:

```sql
CREATE TABLE remote_flight_data
    ENGINE = ArrowFlight(named_collection_name);
```

명명된 컬렉션 매개변수:

| 매개변수                       | 필수       | 기본값     | 설명                            |
| -------------------------- | -------- | ------- | ----------------------------- |
| `host` or `hostname`       | 아니요      | `""`    | 서버 호스트명입니다.                   |
| `port`                     | 예        | —       | 서버 포트입니다.                     |
| `dataset`                  | 아니요      | `""`    | 데이터셋 이름 또는 디스크립터입니다.          |
| `use_basic_authentication` | 아니요      | `true`  | 기본 인증을 사용하도록 설정합니다.           |
| `user` or `username`       | 인증 활성화 시 | —       | 인증에 사용할 사용자 이름입니다.            |
| `password`                 | 아니요      | `""`    | 인증에 사용할 비밀번호입니다.              |
| `enable_ssl`               | 아니요      | `false` | TLS 암호화를 사용하도록 설정합니다.         |
| `ssl_ca`                   | 아니요      | `""`    | TLS 검증에 사용할 CA 인증서 파일의 경로입니다. |
| `ssl_override_hostname`    | 아니요      | `""`    | TLS 검증 중 확인할 호스트명을 재정의합니다.    |

<div id="settings">
  ## 설정
</div>

* `arrow_flight_request_descriptor_type` — 데이터셋 이름을 서버에 전송하는 방식을 제어합니다. 가능한 값은 `path`(기본값, PATH 디스크립터로 전송) 또는 `command`(`SELECT * FROM <dataset>``를 포함한 CMD 디스크립터로 전송)입니다. SQL 명령을 기대하는 서버(예: Dremio)에서는 `command&#96;를 사용하십시오.

<div id="usage-example">
  ## 사용 예시
</div>

원격 Arrow Flight 서버에서 데이터 읽기:

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');

SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

원격 서버로 데이터 삽입:

```sql
INSERT INTO remote_flight_data VALUES (4, 'qux', 99.9);
```

<div id="notes">
  ## 참고 사항
</div>

* `CREATE TABLE` 문에서 컬럼을 지정한 경우, Flight 서버가 반환하는 스키마와 일치해야 합니다.
* 컬럼을 생략하면 원격 서버의 스키마를 자동으로 추론합니다.
* 읽기(`SELECT`)와 쓰기(`INSERT`)를 모두 지원합니다.
* `arrow_flight_request_descriptor_type` 설정은 데이터셋 이름을 PATH 디스크립터로 보낼지, 또는 `SELECT *` 쿼리를 감싼 CMD 디스크립터로 보낼지를 제어합니다.

<div id="see-also">
  ## 관련 항목
</div>

* [arrowFlight 테이블 함수](/ko/sql-reference/table-functions/arrowflight)
* [Arrow Flight 인터페이스](/ko/interfaces/arrowflight)
* [Apache Arrow Flight SQL 사양](https://arrow.apache.org/docs/format/FlightSql.html)
* [ClickHouse의 Arrow 형식](/ko/interfaces/formats/Arrow)