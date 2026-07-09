---
alias: []
description: 'Avro 포맷 문서'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
doc_type: '참고'
---

import DataTypeMapping from './_snippets/data-types-matching.md'

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

[Apache Avro](https://avro.apache.org/)는 효율적인 데이터 처리를 위해 이진 인코딩을 사용하는 행 지향 직렬화 포맷입니다. `Avro` 포맷은 [Avro 데이터 파일](https://avro.apache.org/docs/current/specification/#object-container-files)의 읽기와 쓰기를 지원합니다. 이 포맷은 스키마가 내장된 자체 설명형 메시지를 사용합니다. 스키마 레지스트리와 함께 Avro를 사용하는 경우 [`AvroConfluent`](./AvroConfluent.md) 포맷을 참조하십시오.

<div id="data-type-mapping">
  ## 데이터 타입 매핑
</div>

<DataTypeMapping />

<div id="format-settings">
  ## 포맷 설정
</div>

| 설정                                         | 설명                                                                                                   | 기본값     |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------------- | ------- |
| `input_format_avro_allow_missing_fields`   | 스키마에서 필드를 찾을 수 없을 때 오류를 발생시키는 대신 기본값을 사용할지 여부입니다.                                                    | `0`     |
| `input_format_avro_null_as_default`        | 널을 허용하지 않는 컬럼에 `null` 값을 삽입할 때 오류를 발생시키는 대신 기본값을 사용할지 여부입니다.                                         | `0`     |
| `output_format_avro_codec`                 | Avro 출력 파일에 사용할 압축 알고리즘입니다. 가능한 값: `null`, `deflate`, `snappy`, `zstd`.                              |         |
| `output_format_avro_sync_interval`         | Avro 파일의 sync marker 간격(바이트 단위)입니다.                                                                  | `16384` |
| `output_format_avro_string_column_pattern` | Avro 문자열 유형 매핑에 사용할 `String` 컬럼을 식별하는 정규식입니다. 기본적으로 ClickHouse `String` 컬럼은 Avro `bytes` 유형으로 기록됩니다. |         |
| `output_format_avro_rows_in_file`          | Avro 출력 파일당 최대 행 수입니다. 이 한도에 도달하면 새 파일이 생성됩니다(스토리지 시스템이 파일 분할을 지원하는 경우).                             | `1`     |

<div id="examples">
  ## 예시
</div>

<div id="reading-avro-data">
  ### Avro 데이터 읽기
</div>

Avro 파일의 데이터를 ClickHouse 테이블로 가져오려면 다음과 같이 하십시오:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

입력된 Avro 파일의 루트 스키마는 `record` 유형이어야 합니다.

테이블 컬럼과 Avro 스키마 필드의 대응 관계를 찾기 위해 ClickHouse는 이름을 비교합니다.
이 비교는 대소문자를 구분하며, 사용되지 않는 필드는 무시됩니다.

ClickHouse 테이블 컬럼의 데이터 유형은 삽입되는 Avro 데이터의 해당 필드와 다를 수 있습니다. 데이터를 삽입할 때 ClickHouse는 먼저 위 표에 따라 데이터 유형을 해석한 다음, 데이터를 해당 컬럼 유형으로 [형 변환](/ko/sql-reference/functions/type-conversion-functions#CAST)합니다.

데이터를 가져오는 중 스키마에서 필드를 찾을 수 없고 설정 [`input_format_avro_allow_missing_fields`](/ko/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields)가 활성화되어 있으면, 오류를 반환하는 대신 기본값이 사용됩니다.

<div id="writing-avro-data">
  ### Avro 데이터 쓰기
</div>

ClickHouse 테이블의 데이터를 Avro 파일로 쓰려면:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

컬럼 이름은 다음 조건을 충족해야 합니다:

* `[A-Za-z_]`로 시작해야 합니다
* 그 뒤에는 `[A-Za-z0-9_]`만 올 수 있습니다

Avro 파일의 출력 압축과 동기화 인터벌은 각각 [`output_format_avro_codec`](/ko/operations/settings/settings-formats.md/#output_format_avro_codec) 및 [`output_format_avro_sync_interval`](/ko/operations/settings/settings-formats.md/#output_format_avro_sync_interval) 설정으로 구성할 수 있습니다.

<div id="inferring-the-avro-schema">
  ### Avro 스키마 추론
</div>

ClickHouse [`DESCRIBE`](/ko/sql-reference/statements/describe-table) 함수를 사용하면 다음 예시와 같이 Avro 파일에서 추론된 포맷을 빠르게 확인할 수 있습니다.
이 예시에는 ClickHouse S3 공개 버킷에서 공개적으로 액세스할 수 있는 Avro 파일의 URL이 포함되어 있습니다:

```sql
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro', 'Avro');

┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```