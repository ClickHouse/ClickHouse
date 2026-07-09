---
description: 'ClickHouse에서 입력 데이터로부터 자동 스키마 추론을 설명하는 페이지'
sidebar_label: '스키마 추론'
slug: /interfaces/schema-inference
title: '입력 데이터로부터 자동 스키마 추론'
doc_type: '참고'
---

ClickHouse는 지원되는 거의 모든 [입력 형식](formats.md)에서 입력 데이터의 구조를 자동으로 파악할 수 있습니다.
이 문서에서는 스키마 추론이 언제 사용되는지, 다양한 입력 형식에서 어떻게 작동하는지, 그리고 이를 제어할 수 있는 설정에는 어떤 것들이 있는지 설명합니다.

<div id="usage">
  ## 사용법
</div>

스키마 추론은 ClickHouse가 특정 데이터 포맷의 데이터를 읽어야 하는데 구조를 알 수 없을 때 사용됩니다.

<div id="table-functions-file-s3-url-hdfs-azureblobstorage">
  ## 테이블 함수 [file](../sql-reference/table-functions/file.md), [s3](../sql-reference/table-functions/s3.md), [url](../sql-reference/table-functions/url.md), [hdfs](../sql-reference/table-functions/hdfs.md), [azureBlobStorage](../sql-reference/table-functions/azureBlobStorage.md).
</div>

이러한 테이블 함수에는 입력 데이터의 구조를 나타내는 선택적 인수 `structure`가 있습니다. 이 인수를 지정하지 않거나 `auto`로 설정하면 구조가 데이터에서 자동으로 추론됩니다.

**예시:**

`user_files` 디렉터리에 다음 내용이 포함된 JSONEachRow 포맷의 `hobbies.jsonl` 파일이 있다고 가정해 보겠습니다:

```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fitness", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

구조를 지정하지 않아도 ClickHouse에서 이 데이터를 읽을 수 있습니다:

```sql
SELECT * FROM file('hobbies.jsonl')
```

```response
┌─id─┬─age─┬─name───┬─hobbies──────────────────────────┐
│  1 │  25 │ Josh   │ ['football','cooking','music']   │
│  2 │  19 │ Alan   │ ['tennis','art']                 │
│  3 │  32 │ Lana   │ ['fitness','reading','shopping'] │
│  4 │  47 │ Brayan │ ['movies','skydiving']           │
└────┴─────┴────────┴──────────────────────────────────┘
```

참고: 포맷 `JSONEachRow`는 파일 확장자 `.jsonl`을 기준으로 자동으로 결정되었습니다.

`DESCRIBE` 쿼리를 사용하면 자동으로 결정된 구조를 확인할 수 있습니다:

```sql
DESCRIBE file('hobbies.jsonl')
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="table-engines-file-s3-url-hdfs-azureblobstorage">
  ## 테이블 엔진 [File](../engines/table-engines/special/file.md), [S3](../engines/table-engines/integrations/s3.md), [URL](../engines/table-engines/special/url.md), [HDFS](../engines/table-engines/integrations/hdfs.md), [azureBlobStorage](../engines/table-engines/integrations/azureBlobStorage.md)
</div>

`CREATE TABLE` 쿼리에서 컬럼 목록을 지정하지 않으면 데이터로부터 테이블 구조가 자동으로 추론됩니다.

**예시:**

`hobbies.jsonl` 파일을 사용하겠습니다. 이 파일의 데이터를 사용해 `File` 엔진 테이블을 생성할 수 있습니다:

```sql
CREATE TABLE hobbies ENGINE=File(JSONEachRow, 'hobbies.jsonl')
```

```response
Ok.
```

```sql
SELECT * FROM hobbies
```

```response
┌─id─┬─age─┬─name───┬─hobbies──────────────────────────┐
│  1 │  25 │ Josh   │ ['football','cooking','music']   │
│  2 │  19 │ Alan   │ ['tennis','art']                 │
│  3 │  32 │ Lana   │ ['fitness','reading','shopping'] │
│  4 │  47 │ Brayan │ ['movies','skydiving']           │
└────┴─────┴────────┴──────────────────────────────────┘
```

```sql
DESCRIBE TABLE hobbies
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="clickhouse-local">
  ## clickhouse-local
</div>

`clickhouse-local`에는 입력 데이터의 구조를 지정하는 선택적 매개변수 `-S/--structure`가 있습니다. 이 매개변수를 지정하지 않거나 `auto`로 설정하면 데이터에서 구조를 자동으로 추론합니다.

**예시:**

파일 `hobbies.jsonl`을 사용하겠습니다. `clickhouse-local`을 사용해 이 파일의 데이터에 쿼리할 수 있습니다:

```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='DESCRIBE TABLE hobbies'
```

```response
id    Nullable(Int64)
age    Nullable(Int64)
name    Nullable(String)
hobbies    Array(Nullable(String))
```

```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='SELECT * FROM hobbies'
```

```response
1    25    Josh    ['football','cooking','music']
2    19    Alan    ['tennis','art']
3    32    Lana    ['fitness','reading','shopping']
4    47    Brayan    ['movies','skydiving']
```

<div id="using-structure-from-insertion-table">
  ## 삽입 테이블의 구조 사용
</div>

테이블 함수 `file/s3/url/hdfs`를 사용해 테이블에 데이터를 삽입할 때,
데이터에서 구조를 추출하는 대신 삽입 테이블의 구조를 사용하는 옵션이 있습니다.
스키마 추론에 시간이 다소 걸릴 수 있으므로 삽입 성능을 향상시킬 수 있습니다. 또한 테이블에 최적화된 스키마가 있는 경우,
타입 간 변환이 수행되지 않으므로 유용합니다.

이 동작을 제어하는 특별한 설정 [use&#95;structure&#95;from&#95;insertion&#95;table&#95;in&#95;table&#95;functions](/ko/operations/settings/settings.md/#use_structure_from_insertion_table_in_table_functions)이 있습니다.
이 설정에는 가능한 값이 3개 있습니다:

* 0 - 테이블 함수가 데이터에서 구조를 추출합니다.
* 1 - 테이블 함수가 삽입 테이블의 구조를 사용합니다.
* 2 - ClickHouse가 삽입 테이블의 구조를 사용할 수 있는지, 또는 스키마 추론을 사용할지를 자동으로 결정합니다. 기본값입니다.

**예시 1:**

다음 구조로 `hobbies1` 테이블을 생성해 보겠습니다:

```sql
CREATE TABLE hobbies1
(
    `id` UInt64,
    `age` LowCardinality(UInt8),
    `name` String,
    `hobbies` Array(String)
)
ENGINE = MergeTree
ORDER BY id;
```

그리고 `hobbies.jsonl` 파일의 데이터를 삽입합니다:

```sql
INSERT INTO hobbies1 SELECT * FROM file(hobbies.jsonl)
```

이 경우 파일의 모든 컬럼이 변경 없이 테이블에 삽입되므로, ClickHouse는 스키마 추론 대신 삽입 테이블의 구조를 사용합니다.

**예시 2:**

다음 구조로 테이블 `hobbies2`를 생성하겠습니다:

```sql
CREATE TABLE hobbies2
(
  `id` UInt64,
  `age` LowCardinality(UInt8),
  `hobbies` Array(String)
)
  ENGINE = MergeTree
ORDER BY id;
```

그리고 `hobbies.jsonl` 파일의 데이터를 삽입합니다:

```sql
INSERT INTO hobbies2 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

이 경우 `SELECT` 쿼리의 모든 컬럼이 테이블에 있으므로 ClickHouse는 삽입 테이블의 구조를 사용합니다.
다만 이는 JSONEachRow, TSKV, Parquet 등처럼 컬럼의 부분 집합만 읽을 수 있는 입력 형식에서만 작동합니다(따라서 예를 들어 TSV 포맷에서는 작동하지 않습니다).

**예시 3:**

다음 구조로 테이블 `hobbies3`를 생성해 보겠습니다:

```sql
CREATE TABLE hobbies3
(
  `identifier` UInt64,
  `age` LowCardinality(UInt8),
  `hobbies` Array(String)
)
  ENGINE = MergeTree
ORDER BY identifier;
```

그리고 `hobbies.jsonl` 파일의 데이터를 삽입합니다:

```sql
INSERT INTO hobbies3 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

이 경우 `SELECT` 쿼리에서는 컬럼 `id``를 사용하지만, 테이블에는 이 컬럼이 없고(`identifier&#96;라는 이름의 컬럼이 있음),
ClickHouse는 삽입 테이블의 구조를 사용할 수 없으므로 스키마 추론이 사용됩니다.

**예시 4:**

다음 구조로 테이블 `hobbies4`를 생성합니다:

```sql
CREATE TABLE hobbies4
(
  `id` UInt64,
  `any_hobby` Nullable(String)
)
  ENGINE = MergeTree
ORDER BY id;
```

그리고 `hobbies.jsonl` 파일의 데이터를 삽입합니다:

```sql
INSERT INTO hobbies4 SELECT id, empty(hobbies) ? NULL : hobbies[1] FROM file(hobbies.jsonl)
```

이 경우 `SELECT` 쿼리에서 컬럼 `hobbies`를 테이블에 삽입하기 전에 일부 연산이 수행되므로, ClickHouse는 삽입 테이블의 구조를 사용할 수 없고 스키마 추론이 사용됩니다.

<div id="schema-inference-cache">
  ## 스키마 추론 캐시
</div>

대부분의 입력 형식에서는 구조를 파악하기 위해 일부 데이터를 읽어 스키마를 추론하며, 이 과정에 다소 시간이 걸릴 수 있습니다.
ClickHouse가 동일한 파일의 데이터를 읽을 때마다 같은 스키마를 매번 추론하지 않도록, 추론된 스키마는 캐시에 저장됩니다. 이후 동일한 파일에 다시 접근하면 ClickHouse는 캐시에 저장된 스키마를 사용합니다.

이 캐시를 제어하는 특별한 설정이 있습니다:

* `schema_inference_cache_max_elements_for_{file/s3/hdfs/url/azure}` - 해당 테이블 함수에 대해 캐시할 수 있는 스키마의 최대 개수입니다. 기본값은 `4096`입니다. 이 설정은 server 구성에서 지정해야 합니다.
* `schema_inference_use_cache_for_{file,s3,hdfs,url,azure}` - 스키마 추론에 캐시를 사용할지 여부를 켜거나 끌 수 있습니다. 이 설정은 쿼리에서 사용할 수 있습니다.

파일의 스키마는 데이터를 수정하거나 포맷 설정을 변경하면 달라질 수 있습니다.
이 때문에 스키마 추론 캐시는 파일 소스, 포맷 이름, 사용된 포맷 설정, 그리고 파일의 마지막 수정 시각을 기준으로 스키마를 식별합니다.

참고: `url` 테이블 함수에서 URL로 접근하는 일부 파일에는 마지막 수정 시각 정보가 없을 수 있습니다. 이런 경우를 위해 특별한 설정인
`schema_inference_cache_require_modification_time_for_url`가 있습니다. 이 설정을 비활성화하면 이러한 파일에 대해서는 마지막 수정 시각이 없어도 캐시의 스키마를 사용할 수 있습니다.

또한 캐시에 있는 현재 모든 스키마를 보여주는 system table [schema&#95;inference&#95;cache](../operations/system-tables/schema_inference_cache.md)와 `SYSTEM CLEAR SCHEMA CACHE [FOR File/S3/URL/HDFS]` system query도 있으며,
이를 사용하면 모든 소스 또는 특정 소스의 스키마 캐시를 정리할 수 있습니다.

**예시:**

S3의 샘플 데이터셋 `github-2022.ndjson.gz`의 구조를 추론해 보고, 스키마 추론 캐시가 어떻게 작동하는지 살펴보겠습니다:

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
```

```response
┌─name───────┬─type─────────────────────────────────────────┐
│ type       │ Nullable(String)                             │
│ actor      │ Tuple(                                      ↴│
│            │↳    avatar_url Nullable(String),            ↴│
│            │↳    display_login Nullable(String),         ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    login Nullable(String),                 ↴│
│            │↳    url Nullable(String))                    │
│ repo       │ Tuple(                                      ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    name Nullable(String),                  ↴│
│            │↳    url Nullable(String))                    │
│ created_at │ Nullable(String)                             │
│ payload    │ Tuple(                                      ↴│
│            │↳    action Nullable(String),                ↴│
│            │↳    distinct_size Nullable(Int64),          ↴│
│            │↳    pull_request Tuple(                     ↴│
│            │↳        author_association Nullable(String),↴│
│            │↳        base Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        head Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        number Nullable(Int64),             ↴│
│            │↳        state Nullable(String),             ↴│
│            │↳        title Nullable(String),             ↴│
│            │↳        updated_at Nullable(String),        ↴│
│            │↳        user Tuple(                         ↴│
│            │↳            login Nullable(String))),       ↴│
│            │↳    ref Nullable(String),                   ↴│
│            │↳    ref_type Nullable(String),              ↴│
│            │↳    size Nullable(Int64))                    │
└────────────┴──────────────────────────────────────────────┘
5 rows in set. Elapsed: 0.601 sec.
```

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
```

```response
┌─name───────┬─type─────────────────────────────────────────┐
│ type       │ Nullable(String)                             │
│ actor      │ Tuple(                                      ↴│
│            │↳    avatar_url Nullable(String),            ↴│
│            │↳    display_login Nullable(String),         ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    login Nullable(String),                 ↴│
│            │↳    url Nullable(String))                    │
│ repo       │ Tuple(                                      ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    name Nullable(String),                  ↴│
│            │↳    url Nullable(String))                    │
│ created_at │ Nullable(String)                             │
│ payload    │ Tuple(                                      ↴│
│            │↳    action Nullable(String),                ↴│
│            │↳    distinct_size Nullable(Int64),          ↴│
│            │↳    pull_request Tuple(                     ↴│
│            │↳        author_association Nullable(String),↴│
│            │↳        base Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        head Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        number Nullable(Int64),             ↴│
│            │↳        state Nullable(String),             ↴│
│            │↳        title Nullable(String),             ↴│
│            │↳        updated_at Nullable(String),        ↴│
│            │↳        user Tuple(                         ↴│
│            │↳            login Nullable(String))),       ↴│
│            │↳    ref Nullable(String),                   ↴│
│            │↳    ref_type Nullable(String),              ↴│
│            │↳    size Nullable(Int64))                    │
└────────────┴──────────────────────────────────────────────┘

5 rows in set. Elapsed: 0.059 sec.
```

보시는 것처럼 두 번째 쿼리는 거의 즉시 성공했습니다.

이제 추론된 스키마에 영향을 줄 수 있는 몇 가지 설정을 변경해 보겠습니다.

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
SETTINGS input_format_json_try_infer_named_tuples_from_objects=0, input_format_json_read_objects_as_strings = 1

┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ type       │ Nullable(String) │              │                    │         │                  │                │
│ actor      │ Nullable(String) │              │                    │         │                  │                │
│ repo       │ Nullable(String) │              │                    │         │                  │                │
│ created_at │ Nullable(String) │              │                    │         │                  │                │
│ payload    │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

5 rows in set. Elapsed: 0.611 sec
```

보시는 것처럼 동일한 파일에 대해서는 캐시된 스키마가 사용되지 않았습니다. 추론된 스키마에 영향을 줄 수 있는 설정이 변경되었기 때문입니다.

`system.schema_inference_cache` 테이블의 내용을 확인해 보겠습니다:

```sql
SELECT schema, format, source FROM system.schema_inference_cache WHERE storage='S3'
```

```response
┌─schema──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─format─┬─source───────────────────────────────────────────────────────────────────────────────────────────────────┐
│ type Nullable(String), actor Tuple(avatar_url Nullable(String), display_login Nullable(String), id Nullable(Int64), login Nullable(String), url Nullable(String)), repo Tuple(id Nullable(Int64), name Nullable(String), url Nullable(String)), created_at Nullable(String), payload Tuple(action Nullable(String), distinct_size Nullable(Int64), pull_request Tuple(author_association Nullable(String), base Tuple(ref Nullable(String), sha Nullable(String)), head Tuple(ref Nullable(String), sha Nullable(String)), number Nullable(Int64), state Nullable(String), title Nullable(String), updated_at Nullable(String), user Tuple(login Nullable(String))), ref Nullable(String), ref_type Nullable(String), size Nullable(Int64)) │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
│ type Nullable(String), actor Nullable(String), repo Nullable(String), created_at Nullable(String), payload Nullable(String)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

보시다시피 동일한 파일에 대해 서로 다른 2개의 스키마가 있습니다.

system 쿼리를 사용하면 스키마 캐시를 지울 수 있습니다:

```sql
SYSTEM CLEAR SCHEMA CACHE FOR S3
```

```response
Ok.
```

```sql
SELECT count() FROM system.schema_inference_cache WHERE storage='S3'
```

```response
┌─count()─┐
│       0 │
└─────────┘
```

<div id="text-formats">
  ## 텍스트 형식
</div>

텍스트 형식의 경우, ClickHouse는 데이터를 행별로 읽고 포맷에 따라 컬럼 값을 추출한 다음, 재귀적 파서와 휴리스틱을 사용해 각 값의 타입을 결정합니다. 스키마 추론에서 데이터로부터 읽는 최대 행 수와 바이트 수는 설정 `input_format_max_rows_to_read_for_schema_inference`(기본값 25000) 및 `input_format_max_bytes_to_read_for_schema_inference`(기본값 32Mb)로 제어됩니다.
기본적으로 추론된 모든 타입은 [널 허용](../sql-reference/data-types/nullable.md)이지만, `schema_inference_make_columns_nullable` 설정을 통해 이를 변경할 수 있습니다([설정](#settings-for-text-formats) 섹션의 예시 참조).

<div id="json-formats">
  ### JSON 포맷
</div>

JSON 포맷에서 ClickHouse는 JSON 명세에 따라 값을 파싱한 후, 가장 적합한 데이터 타입을 찾습니다.

작동 방식, 추론 가능한 타입, JSON 포맷에서 사용할 수 있는 설정에 대해 살펴보겠습니다.

**예시**

이하 예시에서는 [format](../sql-reference/table-functions/format.md) 테이블 함수를 사용합니다.

정수(Integers), Floats, Bools, Strings:

```sql
DESC format(JSONEachRow, '{"int" : 42, "float" : 42.42, "string" : "Hello, World!"}');
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ int    │ Nullable(Int64)   │              │                    │         │                  │                │
│ float  │ Nullable(Float64) │              │                    │         │                  │                │
│ bool   │ Nullable(Bool)    │              │                    │         │                  │                │
│ string │ Nullable(String)  │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Dates, DateTimes:

```sql
DESC format(JSONEachRow, '{"date" : "2022-01-01", "datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}')
```

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date       │ Nullable(Date)          │              │                    │         │                  │                │
│ datetime   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

배열:

```sql
DESC format(JSONEachRow, '{"arr" : [1, 2, 3], "nested_arrays" : [[1, 2, 3], [4, 5, 6], []]}')
```

```response
┌─name──────────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr           │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ nested_arrays │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

배열에 `null`이 포함된 경우, ClickHouse는 나머지 배열 요소의 타입을 사용합니다:

```sql
DESC format(JSONEachRow, '{"arr" : [null, 42, null]}')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

배열에 서로 다른 타입의 값이 포함되어 있고 `input_format_json_infer_array_of_dynamic_from_array_of_different_types` 설정이 활성화되어 있으면(기본값은 활성화), 해당 배열의 타입은 `Array(Dynamic)`이 됩니다:

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types=1;
DESC format(JSONEachRow, '{"arr" : [42, "hello", [1, 2, 3]]}');
```

```response
┌─name─┬─type───────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Dynamic) │              │                    │         │                  │                │
└──────┴────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

이름이 지정된 Tuple:

설정 `input_format_json_try_infer_named_tuples_from_objects`이 활성화되면, ClickHouse는 스키마 추론 중 JSON 객체에서 named tuple을 추론합니다.
이렇게 생성된 named tuple에는 샘플 데이터에서 해당하는 모든 JSON 객체의 모든 요소가 포함됩니다.

```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

```response
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

이름 없는 튜플:

`input_format_json_infer_array_of_dynamic_from_array_of_different_types` 설정이 비활성화되어 있으면, JSON 포맷에서는 요소 타입이 서로 다른 배열을 이름 없는 튜플로 처리합니다.

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0;
DESC format(JSONEachRow, '{"tuple" : [1, "Hello, World!", [1, 2, 3]]}')
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ tuple │ Tuple(Nullable(Int64), Nullable(String), Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

일부 값이 `null`이거나 비어 있으면, 다른 행에 있는 해당 값의 타입을 사용합니다:

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;
DESC format(JSONEachRow, $$
                              {"tuple" : [1, null, null]}
                              {"tuple" : [null, "Hello, World!", []]}
                              {"tuple" : [null, null, [1, 2, 3]]}
                         $$)
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ tuple │ Tuple(Nullable(Int64), Nullable(String), Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

맵:

JSON에서는 값의 유형이 모두 동일한 객체를 맵(Map) 타입으로 읽을 수 있습니다.
참고: 이 기능은 `input_format_json_read_objects_as_strings` 및 `input_format_json_try_infer_named_tuples_from_objects` 설정이 비활성화되어 있을 때만 작동합니다.

```sql
SET input_format_json_read_objects_as_strings = 0, input_format_json_try_infer_named_tuples_from_objects = 0;
DESC format(JSONEachRow, '{"map" : {"key1" : 42, "key2" : 24, "key3" : 4}}')
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ map  │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Nested 복합 타입:

```sql
DESC format(JSONEachRow, '{"value" : [[[42, 24], []], {"key1" : 42, "key2" : 24}]}')
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Tuple(Array(Array(Nullable(String))), Tuple(key1 Nullable(Int64), key2 Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

데이터에 null 값/빈 객체/빈 배열만 있어 ClickHouse가 일부 키의 타입을 추론할 수 없는 경우, `input_format_json_infer_incomplete_types_as_strings` 설정이 활성화되어 있으면 `String` 타입을 사용하고, 그렇지 않으면 예외가 발생합니다:

```sql
DESC format(JSONEachRow, '{"arr" : [null, null]}') SETTINGS input_format_json_infer_incomplete_types_as_strings = 1;
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Nullable(String)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, '{"arr" : [null, null]}') SETTINGS input_format_json_infer_incomplete_types_as_strings = 0;
```

```response
Code: 652. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot determine type for column 'arr' by first 1 rows of data,
most likely this column contains only Nulls or empty Arrays/Maps.
...
```

<div id="json-settings">
  #### JSON 설정
</div>

<div id="input_format_json_try_infer_numbers_from_strings">
  ##### input_format_json_try_infer_numbers_from_strings
</div>

이 설정을 활성화하면 문자열 값에서 숫자를 추론합니다.

이 설정은 기본적으로 비활성화되어 있습니다.

**예시:**

```sql
SET input_format_json_try_infer_numbers_from_strings = 1;
DESC format(JSONEachRow, $$
                              {"value" : "42"}
                              {"value" : "424242424242"}
                         $$)
```

```response
┌─name──┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(Int64) │              │                    │         │                  │                │
└───────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_try_infer_named_tuples_from_objects">
  ##### input_format_json_try_infer_named_tuples_from_objects
</div>

이 설정을 활성화하면 JSON 객체에서 named tuple을 추론할 수 있습니다. 추론된 named tuple에는 샘플 데이터에서 대응하는 모든 JSON 객체의 모든 요소가 포함됩니다.
JSON 데이터가 희소하지 않다면 데이터 샘플에 가능한 모든 객체 키가 포함되므로 유용할 수 있습니다.

이 설정은 기본적으로 활성화되어 있습니다.

**예시**

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

```response title="Response"
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"array" : [{"a" : 42, "b" : "Hello"}, {}, {"c" : [1,2,3]}, {"d" : "2020-01-01"}]}')
```

```markdown title="Response"
┌─name──┬─type────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ array │ Array(Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Nullable(Date))) │              │                    │         │                  │                │
└───────┴─────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects">
  ##### input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects
</div>

이 설정을 활성화하면 JSON 객체에서 named tuple을 추론할 때(`input_format_json_try_infer_named_tuples_from_objects`가 활성화된 경우) 모호한 경로에서 예외를 발생시키는 대신 String type을 사용할 수 있습니다.
따라서 모호한 경로가 있더라도 JSON 객체를 named tuple로 읽을 수 있습니다.

기본적으로 비활성화되어 있습니다.

**예시**

설정이 비활성화된 경우:

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 0;
DESC format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```

```response title="Response"
Code: 636. DB::Exception: The table structure cannot be extracted from a JSONEachRow format file. Error:
Code: 117. DB::Exception: JSON objects have ambiguous data: in some objects path 'a' has type 'Int64' and in some - 'Tuple(b String)'. You can enable setting input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects to use String type for path 'a'. (INCORRECT_DATA) (version 24.3.1.1).
You can specify the structure manually. (CANNOT_EXTRACT_TABLE_STRUCTURE)
```

설정이 활성화된 경우:

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : "a" : 42}, {"obj" : {"a" : {"b" : "Hello"}}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```

```response title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(String))     │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
┌─obj─────────────────┐
│ ('42')              │
│ ('{"b" : "Hello"}') │
└─────────────────────┘
```

<div id="input_format_json_read_objects_as_strings">
  ##### input_format_json_read_objects_as_strings
</div>

이 설정을 활성화하면 중첩된 JSON 객체를 문자열로 읽을 수 있습니다.
이 설정을 사용하면 JSON 객체 타입을 사용하지 않고도 중첩된 JSON 객체를 읽을 수 있습니다.

이 설정은 기본적으로 활성화되어 있습니다.

참고: 이 설정을 활성화해도 `input_format_json_try_infer_named_tuples_from_objects` 설정이 비활성화된 경우에만 적용됩니다.

```sql
SET input_format_json_read_objects_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 0;
DESC format(JSONEachRow, $$
                             {"obj" : {"key1" : 42, "key2" : [1,2,3,4]}}
                             {"obj" : {"key3" : {"nested_key" : 1}}}
                         $$)
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_numbers_as_strings">
  ##### input_format_json_read_numbers_as_strings
</div>

이 설정을 활성화하면 숫자형 값을 문자열로 읽을 수 있습니다.

이 설정은 기본적으로 활성화되어 있습니다.

**예시**

```sql
SET input_format_json_read_numbers_as_strings = 1;
DESC format(JSONEachRow, $$
                                {"value" : 1055}
                                {"value" : "unknown"}
                         $$)
```

```response
┌─name──┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(String) │              │                    │         │                  │                │
└───────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_bools_as_numbers">
  ##### input_format_json_read_bools_as_numbers
</div>

이 설정을 활성화하면 Bool 값을 숫자로 읽을 수 있습니다.

이 설정은 기본적으로 활성화됩니다.

**예시:**

```sql
SET input_format_json_read_bools_as_numbers = 1;
DESC format(JSONEachRow, $$
                                {"value" : true}
                                {"value" : 42}
                         $$)
```

```response
┌─name──┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(Int64) │              │                    │         │                  │                │
└───────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_bools_as_strings">
  ##### input_format_json_read_bools_as_strings
</div>

이 설정을 사용하면 Bool 값을 문자열로 읽을 수 있습니다.

이 설정은 기본적으로 활성화되어 있습니다.

**예시:**

```sql
SET input_format_json_read_bools_as_strings = 1;
DESC format(JSONEachRow, $$
                                {"value" : true}
                                {"value" : "Hello, World"}
                         $$)
```

```response
┌─name──┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(String) │              │                    │         │                  │                │
└───────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_arrays_as_strings">
  ##### input_format_json_read_arrays_as_strings
</div>

이 설정을 활성화하면 JSON 배열 값을 문자열로 읽습니다.

이 설정은 기본적으로 활성화되어 있습니다.

**예시**

```sql
SET input_format_json_read_arrays_as_strings = 1;
SELECT arr, toTypeName(arr), JSONExtractArrayRaw(arr)[3] from format(JSONEachRow, 'arr String', '{"arr" : [1, "Hello", [1,2,3]]}');
```

```response
┌─arr───────────────────┬─toTypeName(arr)─┬─arrayElement(JSONExtractArrayRaw(arr), 3)─┐
│ [1, "Hello", [1,2,3]] │ String          │ [1,2,3]                                   │
└───────────────────────┴─────────────────┴───────────────────────────────────────────┘
```

<div id="input_format_json_infer_incomplete_types_as_strings">
  ##### input_format_json_infer_incomplete_types_as_strings
</div>

이 설정을 활성화하면 스키마 추론 중 데이터 샘플에 `Null`/`{}`/`[]`만 있는 JSON 키에 대해 String 타입을 사용할 수 있습니다.
JSON 포맷에서는 관련 설정이 모두 활성화되어 있으면(기본적으로 모두 활성화되어 있습니다) 모든 값을 String으로 읽을 수 있으므로, 타입을 알 수 없는 키에 String 타입을 사용하면 스키마 추론 중
`Cannot determine type for column 'column_name' by first 25000 rows of data, most likely this column contains only Nulls or empty Arrays/Maps`와 같은 오류를 방지할 수 있습니다.

예시:

```sql title="Query"
SET input_format_json_infer_incomplete_types_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 1;
DESCRIBE format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
```

```markdown title="Response"
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Array(Nullable(Int64)), b Nullable(String), c Nullable(String), d Nullable(String), e Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

┌─obj────────────────────────────┐
│ ([1,2,3],'hello',NULL,'{}',[]) │
└────────────────────────────────┘
```

<div id="csv">
  ### CSV
</div>

CSV 형식에서 ClickHouse는 구분 기호에 따라 행에서 컬럼 값을 추출합니다. ClickHouse는 숫자와 문자열을 제외한 모든 타입이 큰따옴표로 묶여 있기를 기대합니다. 값이 큰따옴표로 묶여 있으면 ClickHouse는 재귀 파서를 사용해
따옴표 안의 데이터를 파싱한 다음, 이에 가장 적합한 데이터 타입을 찾으려고 시도합니다. 값이 큰따옴표로 묶여 있지 않으면 ClickHouse는 이를 숫자로 파싱하려고 시도하고,
숫자가 아니면 문자열로 처리합니다.

일부 파서와 휴리스틱을 사용해 ClickHouse가 복잡한 타입을 추론하지 않도록 하려면 설정 `input_format_csv_use_best_effort_in_schema_inference`를 비활성화할 수 있으며,
그러면 ClickHouse는 모든 컬럼을 String으로 처리합니다.

설정 `input_format_csv_detect_header`가 활성화되어 있으면 ClickHouse는 스키마를 추론하는 동안 컬럼 이름(그리고 경우에 따라 타입)이 포함된 헤더를 감지하려고 시도합니다. 이 설정은 기본적으로 활성화되어 있습니다.

**예시:**

정수, Float, Bool, String:

```sql
DESC format(CSV, '42,42.42,true,"Hello,World!"')
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

따옴표로 묶지 않은 문자열:

```sql
DESC format(CSV, 'Hello world!,World hello!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Date, DateTime:

```sql
DESC format(CSV, '"2020-01-01","2020-01-01 00:00:00","2022-01-01 00:00:00.000"')
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

배열:

```sql
DESC format(CSV, '"[1,2,3]","[[1, 2], [], [3, 4]]"')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(CSV, $$"['Hello', 'world']","[['Abc', 'Def'], []]"$$)
```

```response
┌─name─┬─type───────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(String))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

배열에 null이 포함된 경우 ClickHouse는 다른 배열 요소의 타입을 사용합니다:

```sql
DESC format(CSV, '"[NULL, 42, NULL]"')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

맵:

```sql
DESC format(CSV, $$"{'key1' : 42, 'key2' : 24}"$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

중첩된 배열 및 맵:

```sql
DESC format(CSV, $$"[{'key1' : [[42, 42], []], 'key2' : [[null], [42]]}]"$$)
```

```response
┌─name─┬─type──────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Array(Nullable(Int64))))) │              │                    │         │                  │                │
└──────┴───────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

데이터에 null만 포함되어 있어 ClickHouse가 따옴표 안의 유형을 판별할 수 없는 경우, ClickHouse는 이를 String으로 처리합니다:

```sql
DESC format(CSV, '"[NULL, NULL]"')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

`input_format_csv_use_best_effort_in_schema_inference` 설정을 비활성화한 예시:

```sql
SET input_format_csv_use_best_effort_in_schema_inference = 0
DESC format(CSV, '"[1,2,3]",42.42,Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

헤더 자동 감지 예시(`input_format_csv_detect_header`가 활성화된 경우):

이름만 있는 경우:

```sql
SELECT * FROM format(CSV,
$$"number","string","array"
42,"Hello","[1, 2, 3]"
43,"World","[4, 5, 6]"
$$)
```

```response
┌─number─┬─string─┬─array───┐
│     42 │ Hello  │ [1,2,3] │
│     43 │ World  │ [4,5,6] │
└────────┴────────┴─────────┘
```

이름과 타입:

```sql
DESC format(CSV,
$$"number","string","array"
"UInt32","String","Array(UInt16)"
42,"Hello","[1, 2, 3]"
43,"World","[4, 5, 6]"
$$)
```

```response
┌─name───┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ UInt32        │              │                    │         │                  │                │
│ string │ String        │              │                    │         │                  │                │
│ array  │ Array(UInt16) │              │                    │         │                  │                │
└────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

헤더는 String 타입이 아닌 컬럼이 하나 이상 있는 경우에만 감지됩니다. 모든 컬럼이 String 타입이면 헤더는 감지되지 않습니다:

```sql
SELECT * FROM format(CSV,
$$"first_column","second_column"
"Hello","World"
"World","Hello"
$$)
```

```response
┌─c1───────────┬─c2────────────┐
│ first_column │ second_column │
│ Hello        │ World         │
│ World        │ Hello         │
└──────────────┴───────────────┘
```

<div id="csv-settings">
  #### CSV 설정
</div>

<div id="input_format_csv_try_infer_numbers_from_strings">
  ##### input_format_csv_try_infer_numbers_from_strings
</div>

이 설정을 활성화하면 문자열 값에서 숫자형 값을 추론할 수 있습니다.

이 설정은 기본적으로 비활성화되어 있습니다.

**예시:**

```sql
SET input_format_json_try_infer_numbers_from_strings = 1;
DESC format(CSV, '42,42.42');
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="tsv-tskv">
  ### TSV/TSKV
</div>

TSV/TSKV 포맷에서 ClickHouse는 표 형식 구분 기호에 따라 행에서 컬럼 값을 추출한 다음,
재귀 파서(parser)를 사용해 추출한 값을 파싱하여 가장 적절한 타입을 결정합니다. 타입을 결정할 수 없으면 ClickHouse는 이 값을 String으로 처리합니다.

일부 파서와 휴리스틱을 사용해 ClickHouse가 복합 타입을 판별하지 않도록 하려면 `input_format_tsv_use_best_effort_in_schema_inference`
설정을 비활성화하면 되며, 그러면 ClickHouse는 모든 컬럼을 Strings로 처리합니다.

`input_format_tsv_detect_header` 설정이 활성화되어 있으면, ClickHouse는 스키마(schema)를 추론하는 동안 컬럼 이름(그리고 경우에 따라 타입)이 포함된 헤더를 감지하려고 시도합니다. 이 설정은 기본적으로 활성화되어 있습니다.

**예시:**

정수, 부동소수점 수, Bool, 문자열:

```sql
DESC format(TSV, '42    42.42    true    Hello,World!')
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(TSKV, 'int=42    float=42.42    bool=true    string=Hello,World!\n')
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ int    │ Nullable(Int64)   │              │                    │         │                  │                │
│ float  │ Nullable(Float64) │              │                    │         │                  │                │
│ bool   │ Nullable(Bool)    │              │                    │         │                  │                │
│ string │ Nullable(String)  │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

날짜, DateTime:

```sql
DESC format(TSV, '2020-01-01    2020-01-01 00:00:00    2022-01-01 00:00:00.000')
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

배열:

```sql
DESC format(TSV, '[1,2,3]    [[1, 2], [], [3, 4]]')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(TSV, '[''Hello'', ''world'']    [[''Abc'', ''Def''], []]')
```

```response
┌─name─┬─type───────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(String))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

배열에 NULL이 포함된 경우, ClickHouse는 나머지 배열 요소의 타입을 사용합니다:

```sql
DESC format(TSV, '[NULL, 42, NULL]')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

튜플:

```sql
DESC format(TSV, $$(42, 'Hello, world!')$$)
```

```response
┌─name─┬─type─────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Tuple(Nullable(Int64), Nullable(String)) │              │                    │         │                  │                │
└──────┴──────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

맵:

```sql
DESC format(TSV, $${'key1' : 42, 'key2' : 24}$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Nested, 배열, 튜플 및 맵:

```sql
DESC format(TSV, $$[{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}]$$)
```

```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

데이터에 NULL 값만 포함되어 있어 ClickHouse가 유형을 판별할 수 없는 경우, String으로 처리합니다:

```sql
DESC format(TSV, '[NULL, NULL]')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

`input_format_tsv_use_best_effort_in_schema_inference` 설정을 비활성화한 예시:

```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]    42.42    Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

헤더 자동 감지 예시(`input_format_tsv_detect_header`가 활성화된 경우):

이름만:

```sql
SELECT * FROM format(TSV,
$$number    string    array
42    Hello    [1, 2, 3]
43    World    [4, 5, 6]
$$);
```

```response
┌─number─┬─string─┬─array───┐
│     42 │ Hello  │ [1,2,3] │
│     43 │ World  │ [4,5,6] │
└────────┴────────┴─────────┘
```

이름 및 타입:

```sql
DESC format(TSV,
$$number    string    array
UInt32    String    Array(UInt16)
42    Hello    [1, 2, 3]
43    World    [4, 5, 6]
$$)
```

```response
┌─name───┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ UInt32        │              │                    │         │                  │                │
│ string │ String        │              │                    │         │                  │                │
│ array  │ Array(UInt16) │              │                    │         │                  │                │
└────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

헤더는 적어도 하나의 컬럼이 `String`이 아닌 String 타입일 때만 감지됩니다. 모든 컬럼이 `String` 타입이면 헤더는 감지되지 않습니다:

```sql
SELECT * FROM format(TSV,
$$first_column    second_column
Hello    World
World    Hello
$$)
```

```response
┌─c1───────────┬─c2────────────┐
│ first_column │ second_column │
│ Hello        │ World         │
│ World        │ Hello         │
└──────────────┴───────────────┘
```

<div id="values">
  ### 값(Values)
</div>

Values 형식에서 ClickHouse는 행에서 컬럼 값을 추출한 후, 리터럴을 파싱하는 방식과 유사한 재귀 파서를 사용하여 파싱합니다.

**예시:**

정수(Integers), Floats, Bools, Strings:

```sql
DESC format(Values, $$(42, 42.42, true, 'Hello,World!')$$)
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Dates, DateTimes:

```sql
 DESC format(Values, $$('2020-01-01', '2020-01-01 00:00:00', '2022-01-01 00:00:00.000')$$)
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

배열:

```sql
DESC format(Values, '([1,2,3], [[1, 2], [], [3, 4]])')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

배열에 null이 포함된 경우, ClickHouse는 나머지 배열 요소의 타입을 사용합니다:

```sql
DESC format(Values, '([NULL, 42, NULL])')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

튜플:

```sql
DESC format(Values, $$((42, 'Hello, world!'))$$)
```

```response
┌─name─┬─type─────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Tuple(Nullable(Int64), Nullable(String)) │              │                    │         │                  │                │
└──────┴──────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

맵:

```sql
DESC format(Values, $$({'key1' : 42, 'key2' : 24})$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

중첩된 배열, 튜플 및 맵:

```sql
DESC format(Values, $$([{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}])$$)
```

```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

데이터에 null 값만 있어 ClickHouse가 유형을 판별할 수 없으면 예외가 발생합니다:

```sql
DESC format(Values, '([NULL, NULL])')
```

```response
Code: 652. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot determine type for column 'c1' by first 1 rows of data,
most likely this column contains only Nulls or empty Arrays/Maps.
...
```

`input_format_tsv_use_best_effort_in_schema_inference` 설정을 비활성화한 예시:

```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]    42.42    Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="custom-separated">
  ### CustomSeparated
</div>

CustomSeparated 포맷에서 ClickHouse는 먼저 지정된 구분 기호에 따라 행에서 모든 컬럼 값을 추출한 다음, 이스케이프 규칙에 따라 각 값의 데이터 타입을 추론합니다.

설정 `input_format_custom_detect_header`가 활성화되어 있으면, ClickHouse는 스키마를 추론하는 과정에서 컬럼 이름(경우에 따라 타입도 포함)이 있는 헤더를 감지합니다. 이 설정은 기본적으로 활성화되어 있습니다.

**예시**

```sql
SET format_custom_row_before_delimiter = '<row_before_delimiter>',
       format_custom_row_after_delimiter = '<row_after_delimiter>\n',
       format_custom_row_between_delimiter = '<row_between_delimiter>\n',
       format_custom_result_before_delimiter = '<result_before_delimiter>\n',
       format_custom_result_after_delimiter = '<result_after_delimiter>\n',
       format_custom_field_delimiter = '<field_delimiter>',
       format_custom_escaping_rule = 'Quoted'

DESC format(CustomSeparated, $$<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter>'Some string 1'<field_delimiter>[1, NULL, 3]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter>'Some string 3'<field_delimiter>[1, 2, NULL]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Float64)      │              │                    │         │                  │                │
│ c2   │ Nullable(String)       │              │                    │         │                  │                │
│ c3   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

헤더 자동 감지 예시 (`input_format_custom_detect_header`가 활성화된 경우):

```sql
SET format_custom_row_before_delimiter = '<row_before_delimiter>',
       format_custom_row_after_delimiter = '<row_after_delimiter>\n',
       format_custom_row_between_delimiter = '<row_between_delimiter>\n',
       format_custom_result_before_delimiter = '<result_before_delimiter>\n',
       format_custom_result_after_delimiter = '<result_after_delimiter>\n',
       format_custom_field_delimiter = '<field_delimiter>',
       format_custom_escaping_rule = 'Quoted'

DESC format(CustomSeparated, $$<result_before_delimiter>
<row_before_delimiter>'number'<field_delimiter>'string'<field_delimiter>'array'<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42.42<field_delimiter>'Some string 1'<field_delimiter>[1, NULL, 3]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter>'Some string 3'<field_delimiter>[1, 2, NULL]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─number─┬─string────────┬─array──────┐
│  42.42 │ Some string 1 │ [1,NULL,3] │
│   ᴺᵁᴸᴸ │ Some string 3 │ [1,2,NULL] │
└────────┴───────────────┴────────────┘
```

<div id="template">
  ### Template
</div>

Template 형식에서 ClickHouse는 먼저 지정된 템플릿에 따라 행에서 모든 컬럼 값을 추출한 다음, 각 값의 이스케이프 규칙에 따라 각 값의 데이터 타입을 추론합니다.

**예시**

`resultset` 파일에 다음과 같은 내용이 있다고 가정하겠습니다:

```bash
<result_before_delimiter>
${data}<result_after_delimiter>
```

그리고 내용이 다음과 같은 파일 `row_format`:

```text
<row_before_delimiter>${column_1:CSV}<field_delimiter_1>${column_2:Quoted}<field_delimiter_2>${column_3:JSON}<row_after_delimiter>
```

그런 다음 아래 쿼리를 실행할 수 있습니다:

```sql
SET format_template_rows_between_delimiter = '<row_between_delimiter>\n',
       format_template_row = 'row_format',
       format_template_resultset = 'resultset_format'

DESC format(Template, $$<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter_1>'Some string 1'<field_delimiter_2>[1, null, 2]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>\N<field_delimiter_1>'Some string 3'<field_delimiter_2>[1, 2, null]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─name─────┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ column_1 │ Nullable(Float64)      │              │                    │         │                  │                │
│ column_2 │ Nullable(String)       │              │                    │         │                  │                │
│ column_3 │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="regexp">
  ### Regexp
</div>

Template와 유사하게, Regexp 포맷에서는 ClickHouse가 먼저 지정된 정규식에 따라 행에서 모든 컬럼 값을 추출한 다음, 지정된 이스케이프 규칙에 따라 각 값의
데이터 타입 추론을 시도합니다.

**예시**

```sql
SET format_regexp = '^Line: value_1=(.+?), value_2=(.+?), value_3=(.+?)',
       format_regexp_escaping_rule = 'CSV'

DESC format(Regexp, $$Line: value_1=42, value_2="Some string 1", value_3="[1, NULL, 3]"
Line: value_1=2, value_2="Some string 2", value_3="[4, 5, NULL]"$$)
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)        │              │                    │         │                  │                │
│ c2   │ Nullable(String)       │              │                    │         │                  │                │
│ c3   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="settings-for-text-formats">
  ### 형식 관련 설정
</div>

<div id="input-format-max-rows-to-read-for-schema-inference">
  #### input_format_max_rows_to_read_for_schema_inference/input_format_max_bytes_to_read_for_schema_inference
</div>

이 설정은 스키마 추론 중 읽을 데이터의 양을 제어합니다.
더 많은 행/바이트를 읽을수록 스키마 추론에 더 많은 시간이 소요되지만, 타입을
정확하게 판별할 가능성도 높아집니다(특히 데이터에 null이 많이 포함된 경우).

기본값:

* `input_format_max_rows_to_read_for_schema_inference`의 경우 `25000`.
* `input_format_max_bytes_to_read_for_schema_inference`의 경우 `33554432`(32 Mb).

<div id="column-names-for-schema-inference">
  #### column_names_for_schema_inference
</div>

명시적인 컬럼 이름이 없는 포맷의 스키마 추론에 사용할 컬럼 이름 목록입니다. 지정한 이름은 기본값인 `c1,c2,c3,...` 대신 사용됩니다. 형식은 `column1,column2,column3,...`입니다.

**예시**

```sql
DESC format(TSV, 'Hello, World!    42    [1, 2, 3]') settings column_names_for_schema_inference = 'str,int,arr'
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ str  │ Nullable(String)       │              │                    │         │                  │                │
│ int  │ Nullable(Int64)        │              │                    │         │                  │                │
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-hints">
  #### schema_inference_hints
</div>

자동으로 결정된 타입 대신 스키마 추론에 사용할 컬럼 이름과 타입 목록입니다. 포맷: &#39;column&#95;name1 column&#95;type1, column&#95;name2 column&#95;type2, ...&#39;.
이 설정은 자동으로 결정되지 않은 컬럼의 타입을 지정하거나 스키마를 최적화하는 데 사용할 수 있습니다.

**예시**

```sql
DESC format(JSONEachRow, '{"id" : 1, "age" : 25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}') SETTINGS schema_inference_hints = 'age LowCardinality(UInt8), status Nullable(String)', allow_suspicious_low_cardinality_types=1
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ LowCardinality(UInt8)   │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ status  │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-make-columns-nullable">
  #### schema_inference_make_columns_nullable $
</div>

널 허용 여부 정보가 없는 포맷의 스키마 추론 시, 추론된 타입을 `Nullable`로 만들지 여부를 제어합니다. 가능한 값:

* 0 - 추론된 타입은 `Nullable`이 되지 않습니다,
* 1 - 추론된 모든 타입은 `Nullable`입니다,
* 2 또는 &#39;auto&#39; - 텍스트 형식의 경우, 스키마 추론 중 파싱되는 샘플에서 해당 컬럼에 `NULL`이 포함된 경우에만 추론된 타입이 `Nullable`이 됩니다. 강한 타입의 포맷(Parquet, ORC, Arrow)의 경우 널 허용 여부 정보는 파일 메타데이터에서 가져옵니다,
* 3 - 텍스트 형식의 경우 `Nullable`을 사용합니다. 강한 타입의 포맷의 경우 파일 메타데이터를 사용합니다.

기본값: 3.

**예시**

```sql
SET schema_inference_make_columns_nullable = 1;
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ status  │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET schema_inference_make_columns_nullable = 'auto';
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```

```response
┌─name────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Int64            │              │                    │         │                  │                │
│ age     │ Int64            │              │                    │         │                  │                │
│ name    │ String           │              │                    │         │                  │                │
│ status  │ Nullable(String) │              │                    │         │                  │                │
│ hobbies │ Array(String)    │              │                    │         │                  │                │
└─────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET schema_inference_make_columns_nullable = 0;
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```

```response

┌─name────┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Int64         │              │                    │         │                  │                │
│ age     │ Int64         │              │                    │         │                  │                │
│ name    │ String        │              │                    │         │                  │                │
│ status  │ String        │              │                    │         │                  │                │
│ hobbies │ Array(String) │              │                    │         │                  │                │
└─────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-integers">
  #### input_format_try_infer_integers
</div>

:::note
이 설정은 `JSON` 데이터 타입에는 적용되지 않습니다.
:::

활성화하면 ClickHouse는 형식의 스키마 추론 시 부동소수점 수 대신 정수를 추론하려고 합니다.
샘플 데이터에서 해당 컬럼의 모든 숫자가 정수이면 결과 유형은 `Int64`이고, 숫자 중 하나라도 부동소수점 수이면 결과 유형은 `Float64`입니다.
샘플 데이터에 정수만 포함되어 있고, 그중 하나 이상이 양수이며 `Int64` 오버플로우가 발생하면 ClickHouse는 `UInt64`를 추론합니다.

기본적으로 활성화되어 있습니다.

**예시**

```sql
SET input_format_try_infer_integers = 0
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2}
                         $$)
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Float64) │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_try_infer_integers = 1
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2}
                         $$)
```

```response
┌─name───┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Int64) │              │                    │         │                  │                │
└────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 18446744073709551615}
                         $$)
```

```response
┌─name───┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(UInt64) │              │                    │         │                  │                │
└────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2.2}
                         $$)
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Float64) │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-datetimes">
  #### input_format_try_infer_datetimes
</div>

활성화하면 ClickHouse는 형식의 스키마 추론에서 문자열 필드로부터 `DateTime` 또는 `DateTime64` 유형을 추론하려고 합니다.
샘플 데이터에서 특정 컬럼의 모든 필드가 datetime으로 성공적으로 파싱되면 결과 유형은 `DateTime` 또는 `DateTime64(9)`(datetime 중 하나라도 소수 부분이 있는 경우)입니다.
하나 이상의 필드가 datetime으로 파싱되지 않으면 결과 유형은 `String`입니다.

기본적으로 활성화되어 있습니다.

**예시**

```sql
SET input_format_try_infer_datetimes = 0;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```response
┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(String) │              │                    │         │                  │                │
│ datetime64 │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_try_infer_datetimes = 1;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "unknown", "datetime64" : "unknown"}
                         $$)
```

```response
┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(String) │              │                    │         │                  │                │
│ datetime64 │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-datetimes-only-datetime64">
  #### input_format_try_infer_datetimes_only_datetime64
</div>

활성화하면 `input_format_try_infer_datetimes`가 활성화되어 있을 때 datetime 값에 소수 부분이 없더라도 ClickHouse는 항상 `DateTime64(9)`로 추론합니다.

기본적으로 비활성화되어 있습니다.

**예시**

```sql
SET input_format_try_infer_datetimes = 1;
SET input_format_try_infer_datetimes_only_datetime64 = 1;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

참고: 스키마 추론 중 DateTime 파싱은 [date&#95;time&#95;input&#95;format](/ko/operations/settings/settings-formats.md#date_time_input_format) 설정을 따릅니다

<div id="input-format-try-infer-dates">
  #### input_format_try_infer_dates
</div>

활성화하면 ClickHouse는 형식의 스키마 추론에서 문자열 필드로부터 `Date` 유형을 추론하려고 시도합니다.
샘플 데이터에서 특정 컬럼의 모든 필드가 날짜로 성공적으로 파싱되면 결과 유형은 `Date`가 되며,
하나 이상의 필드가 날짜로 파싱되지 않으면 결과 유형은 `String`이 됩니다.

기본적으로 활성화되어 있습니다.

**예시**

```sql
SET input_format_try_infer_datetimes = 0, input_format_try_infer_dates = 0
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "2022-01-01"}
                         $$)
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_try_infer_dates = 1
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "2022-01-01"}
                         $$)
```

```response
┌─name─┬─type───────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(Date) │              │                    │         │                  │                │
└──────┴────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "unknown"}
                         $$)
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-exponent-floats">
  #### input_format_try_infer_exponent_floats
</div>

활성화하면 ClickHouse는 형식에서 지수 표기법의 부동소수점 수를 추론하려고 시도합니다(JSON은 지수 표기법의 숫자를 항상 추론하므로 제외).

기본적으로 비활성화되어 있습니다.

**예시**

```sql
SET input_format_try_infer_exponent_floats = 1;
DESC format(CSV,
$$1.1E10
2.3e-12
42E00
$$)
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Float64) │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="self-describing-formats">
  ## 자체 기술 포맷
</div>

자체 기술 포맷에는 데이터 구조에 대한 정보가 데이터 자체에 포함되어 있습니다.
예를 들어 설명이 담긴 헤더, 이진 타입 트리, 또는 일종의 테이블일 수 있습니다.
이러한 포맷의 파일에서 스키마를 자동으로 추론하기 위해 ClickHouse는 타입 정보가 포함된 데이터의 일부를 읽어
이를 ClickHouse 테이블의 스키마로 변환합니다.

<div id="formats-with-names-and-types">
  ### -WithNamesAndTypes 접미사가 있는 포맷
</div>

ClickHouse는 -WithNamesAndTypes 접미사가 붙은 일부 텍스트 포맷을 지원합니다. 이 접미사는 실제 데이터 앞에 컬럼 이름과 타입이 포함된 추가 2개 행이 데이터에 포함됨을 의미합니다.
이러한 포맷에서 스키마 추론을 수행할 때 ClickHouse는 처음 2개 행을 읽고 컬럼 이름과 타입을 추출합니다.

**예시**

```sql
DESC format(TSVWithNamesAndTypes,
$$num    str    arr
UInt8    String    Array(UInt8)
42    Hello, World!    [1,2,3]
$$)
```

```response
┌─name─┬─type─────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ num  │ UInt8        │              │                    │         │                  │                │
│ str  │ String       │              │                    │         │                  │                │
│ arr  │ Array(UInt8) │              │                    │         │                  │                │
└──────┴──────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-with-metadata">
  ### 메타데이터가 포함된 JSON 포맷
</div>

일부 JSON 입력 형식([JSON](/ko/interfaces/formats/JSON), [JSONCompact](/ko/interfaces/formats/JSONCompact), [JSONColumnsWithMetadata](/ko/interfaces/formats/JSONColumnsWithMetadata))은 컬럼 이름과 타입에 대한 메타데이터를 포함합니다.
이러한 포맷에 대해 스키마 추론을 수행할 때 ClickHouse는 이 메타데이터를 읽습니다.

**예시**

```sql
DESC format(JSON, $$
{
    "meta":
    [
        {
            "name": "num",
            "type": "UInt8"
        },
        {
            "name": "str",
            "type": "String"
        },
        {
            "name": "arr",
            "type": "Array(UInt8)"
        }
    ],

    "data":
    [
        {
            "num": 42,
            "str": "Hello, World",
            "arr": [1,2,3]
        }
    ],

    "rows": 1,

    "statistics":
    {
        "elapsed": 0.005723915,
        "rows_read": 1,
        "bytes_read": 1
    }
}
$$)
```

```response
┌─name─┬─type─────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ num  │ UInt8        │              │                    │         │                  │                │
│ str  │ String       │              │                    │         │                  │                │
│ arr  │ Array(UInt8) │              │                    │         │                  │                │
└──────┴──────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="avro">
  ### Avro
</div>

Avro 포맷에서 ClickHouse는 데이터로부터 스키마를 읽은 다음, 아래의 타입 매핑을 사용해 ClickHouse 스키마로 변환합니다:

| Avro 데이터 타입                        | ClickHouse 데이터 타입                                                              |
| ---------------------------------- | ------------------------------------------------------------------------------ |
| `boolean`                          | [Bool](../sql-reference/data-types/boolean.md)                                 |
| `int`                              | [Int32](../sql-reference/data-types/int-uint.md)                               |
| `int (date)` *                     | [Date32](../sql-reference/data-types/date32.md)                                |
| `long`                             | [Int64](../sql-reference/data-types/int-uint.md)                               |
| `float`                            | [Float32](../sql-reference/data-types/float.md)                                |
| `double`                           | [Float64](../sql-reference/data-types/float.md)                                |
| `bytes`, `string`                  | [String](../sql-reference/data-types/string.md)                                |
| `fixed`                            | [FixedString(N)](../sql-reference/data-types/fixedstring.md)                   |
| `enum`                             | [Enum](../sql-reference/data-types/enum.md)                                    |
| `array(T)`                         | [Array(T)](../sql-reference/data-types/array.md)                               |
| `union(null, T)`, `union(T, null)` | [Nullable(T)](../sql-reference/data-types/date.md)                             |
| `null`                             | [Nullable(Nothing)](../sql-reference/data-types/special-data-types/nothing.md) |
| `string (uuid)` *                  | [UUID](../sql-reference/data-types/uuid.md)                                    |
| `binary (decimal)` *               | [Decimal(P, S)](../sql-reference/data-types/decimal.md)                        |

* [Avro 논리 타입](https://avro.apache.org/docs/current/spec.html#Logical+Types)

그 외 Avro 타입은 지원되지 않습니다.

<div id="parquet">
  ### Parquet
</div>

Parquet 포맷에서 ClickHouse는 데이터로부터 스키마를 읽어 다음 타입 매핑에 따라 ClickHouse 스키마로 변환합니다:

| Parquet 데이터 타입               | ClickHouse 데이터 타입                                       |
| ---------------------------- | ------------------------------------------------------- |
| `BOOL`                       | [Bool](../sql-reference/data-types/boolean.md)          |
| `UINT8`                      | [UInt8](../sql-reference/data-types/int-uint.md)        |
| `INT8`                       | [Int8](../sql-reference/data-types/int-uint.md)         |
| `UINT16`                     | [UInt16](../sql-reference/data-types/int-uint.md)       |
| `INT16`                      | [Int16](../sql-reference/data-types/int-uint.md)        |
| `UINT32`                     | [UInt32](../sql-reference/data-types/int-uint.md)       |
| `INT32`                      | [Int32](../sql-reference/data-types/int-uint.md)        |
| `UINT64`                     | [UInt64](../sql-reference/data-types/int-uint.md)       |
| `INT64`                      | [Int64](../sql-reference/data-types/int-uint.md)        |
| `FLOAT`                      | [Float32](../sql-reference/data-types/float.md)         |
| `DOUBLE`                     | [Float64](../sql-reference/data-types/float.md)         |
| `DATE`                       | [Date32](../sql-reference/data-types/date32.md)         |
| `TIME (ms)`                  | [DateTime](../sql-reference/data-types/datetime.md)     |
| `TIMESTAMP`, `TIME (us, ns)` | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `STRING`, `BINARY`           | [String](../sql-reference/data-types/string.md)         |
| `DECIMAL`                    | [Decimal](../sql-reference/data-types/decimal.md)       |
| `LIST`                       | [Array](../sql-reference/data-types/array.md)           |
| `STRUCT`                     | [Tuple](../sql-reference/data-types/tuple.md)           |
| `MAP`                        | [Map](../sql-reference/data-types/map.md)               |

그 외 Parquet 타입은 지원되지 않습니다.

<div id="arrow">
  ### Arrow
</div>

Arrow 형식에서 ClickHouse는 데이터로부터 스키마를 읽고, 다음 타입 매핑에 따라 ClickHouse 스키마로 변환합니다.

| Arrow 데이터 타입                    | ClickHouse 데이터 타입                                       |
| ------------------------------- | ------------------------------------------------------- |
| `BOOL`                          | [Bool](../sql-reference/data-types/boolean.md)          |
| `UINT8`                         | [UInt8](../sql-reference/data-types/int-uint.md)        |
| `INT8`                          | [Int8](../sql-reference/data-types/int-uint.md)         |
| `UINT16`                        | [UInt16](../sql-reference/data-types/int-uint.md)       |
| `INT16`                         | [Int16](../sql-reference/data-types/int-uint.md)        |
| `UINT32`                        | [UInt32](../sql-reference/data-types/int-uint.md)       |
| `INT32`                         | [Int32](../sql-reference/data-types/int-uint.md)        |
| `UINT64`                        | [UInt64](../sql-reference/data-types/int-uint.md)       |
| `INT64`                         | [Int64](../sql-reference/data-types/int-uint.md)        |
| `FLOAT`, `HALF_FLOAT`           | [Float32](../sql-reference/data-types/float.md)         |
| `DOUBLE`                        | [Float64](../sql-reference/data-types/float.md)         |
| `DATE32`                        | [Date32](../sql-reference/data-types/date32.md)         |
| `DATE64`                        | [DateTime](../sql-reference/data-types/datetime.md)     |
| `TIMESTAMP`, `TIME32`, `TIME64` | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `STRING`, `BINARY`              | [String](../sql-reference/data-types/string.md)         |
| `DECIMAL128`, `DECIMAL256`      | [Decimal](../sql-reference/data-types/decimal.md)       |
| `LIST`                          | [Array](../sql-reference/data-types/array.md)           |
| `STRUCT`                        | [Tuple](../sql-reference/data-types/tuple.md)           |
| `MAP`                           | [Map](../sql-reference/data-types/map.md)               |

그 밖의 Arrow 타입은 지원되지 않습니다.

<div id="orc">
  ### ORC
</div>

ORC 포맷에서 ClickHouse는 데이터로부터 스키마를 읽어 들인 다음, 아래의 타입 매핑을 사용해 ClickHouse 스키마로 변환합니다:

| ORC 데이터 타입                           | ClickHouse 데이터 타입                                       |
| ------------------------------------ | ------------------------------------------------------- |
| `Boolean`                            | [Bool](../sql-reference/data-types/boolean.md)          |
| `Tinyint`                            | [Int8](../sql-reference/data-types/int-uint.md)         |
| `Smallint`                           | [Int16](../sql-reference/data-types/int-uint.md)        |
| `Int`                                | [Int32](../sql-reference/data-types/int-uint.md)        |
| `Bigint`                             | [Int64](../sql-reference/data-types/int-uint.md)        |
| `Float`                              | [Float32](../sql-reference/data-types/float.md)         |
| `Double`                             | [Float64](../sql-reference/data-types/float.md)         |
| `Date`                               | [Date32](../sql-reference/data-types/date32.md)         |
| `Timestamp`                          | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `String`, `Char`, `Varchar`,`BINARY` | [String](../sql-reference/data-types/string.md)         |
| `Decimal`                            | [Decimal](../sql-reference/data-types/decimal.md)       |
| `List`                               | [Array](../sql-reference/data-types/array.md)           |
| `Struct`                             | [Tuple](../sql-reference/data-types/tuple.md)           |
| `Map`                                | [Map](../sql-reference/data-types/map.md)               |

그 밖의 ORC 타입은 지원되지 않습니다.

<div id="native">
  ### Native
</div>

Native 포맷은 ClickHouse 내부에서 사용되며, 데이터에 스키마가 포함되어 있습니다.
스키마 추론 시 ClickHouse는 별도의 변환 없이 데이터에서 스키마를 읽습니다.

<div id="formats-with-external-schema">
  ## 외부 스키마가 있는 포맷
</div>

이러한 포맷은 데이터를 설명하는 스키마를 별도의 파일에 특정 스키마 언어로 작성해 두어야 합니다.
이러한 포맷의 파일에서 스키마를 자동으로 추론하려면, ClickHouse는 별도 파일에서 외부 스키마를 읽어 ClickHouse 테이블 스키마로 변환합니다.

<div id="protobuf">
  ### Protobuf
</div>

ClickHouse는 Protobuf 포맷의 스키마 추론에서 다음과 같은 타입 매핑을 사용합니다:

| Protobuf 데이터 타입               | ClickHouse 데이터 타입                                 |
| ----------------------------- | ------------------------------------------------- |
| `bool`                        | [UInt8](../sql-reference/data-types/int-uint.md)  |
| `float`                       | [Float32](../sql-reference/data-types/float.md)   |
| `double`                      | [Float64](../sql-reference/data-types/float.md)   |
| `int32`, `sint32`, `sfixed32` | [Int32](../sql-reference/data-types/int-uint.md)  |
| `int64`, `sint64`, `sfixed64` | [Int64](../sql-reference/data-types/int-uint.md)  |
| `uint32`, `fixed32`           | [UInt32](../sql-reference/data-types/int-uint.md) |
| `uint64`, `fixed64`           | [UInt64](../sql-reference/data-types/int-uint.md) |
| `string`, `bytes`             | [String](../sql-reference/data-types/string.md)   |
| `enum`                        | [Enum](../sql-reference/data-types/enum.md)       |
| `repeated T`                  | [Array(T)](../sql-reference/data-types/array.md)  |
| `message`, `group`            | [Tuple](../sql-reference/data-types/tuple.md)     |

<div id="capnproto">
  ### CapnProto
</div>

ClickHouse는 CapnProto 포맷의 스키마 추론에 다음 타입 매핑을 사용합니다:

| CapnProto 데이터 타입                   | ClickHouse 데이터 타입                                      |
| ---------------------------------- | ------------------------------------------------------ |
| `Bool`                             | [UInt8](../sql-reference/data-types/int-uint.md)       |
| `Int8`                             | [Int8](../sql-reference/data-types/int-uint.md)        |
| `UInt8`                            | [UInt8](../sql-reference/data-types/int-uint.md)       |
| `Int16`                            | [Int16](../sql-reference/data-types/int-uint.md)       |
| `UInt16`                           | [UInt16](../sql-reference/data-types/int-uint.md)      |
| `Int32`                            | [Int32](../sql-reference/data-types/int-uint.md)       |
| `UInt32`                           | [UInt32](../sql-reference/data-types/int-uint.md)      |
| `Int64`                            | [Int64](../sql-reference/data-types/int-uint.md)       |
| `UInt64`                           | [UInt64](../sql-reference/data-types/int-uint.md)      |
| `Float32`                          | [Float32](../sql-reference/data-types/float.md)        |
| `Float64`                          | [Float64](../sql-reference/data-types/float.md)        |
| `Text`, `Data`                     | [String](../sql-reference/data-types/string.md)        |
| `enum`                             | [Enum](../sql-reference/data-types/enum.md)            |
| `List`                             | [Array](../sql-reference/data-types/array.md)          |
| `struct`                           | [Tuple](../sql-reference/data-types/tuple.md)          |
| `union(T, Void)`, `union(Void, T)` | [Nullable(T)](../sql-reference/data-types/nullable.md) |

<div id="strong-typed-binary-formats">
  ## 타입이 명시된 바이너리 포맷
</div>

이러한 포맷에서는 직렬화된 각 값에 해당 값의 타입 정보(경우에 따라 이름 정보도 포함)가 들어 있지만, 테이블 전체에 대한 정보는 없습니다.
이러한 포맷의 스키마 추론에서는 ClickHouse가 데이터를 행 단위로 읽으면서(`input_format_max_rows_to_read_for_schema_inference`행 또는 `input_format_max_bytes_to_read_for_schema_inference`바이트까지) 데이터에서 각 값의 타입(경우에 따라 이름도 포함)을 추출한 다음, 이를 ClickHouse 타입으로 변환합니다.

<div id="msgpack">
  ### MsgPack
</div>

MsgPack 포맷에는 행 사이에 구분자가 없으므로, 이 포맷에서 스키마 추론을 사용하려면 설정 `input_format_msgpack_number_of_columns`으로 테이블의 컬럼 수를 지정해야 합니다. ClickHouse는 다음과 같은 타입 매핑을 사용합니다:

| MessagePack data type (`INSERT`)                                   | ClickHouse data type                                  |
| ------------------------------------------------------------------ | ----------------------------------------------------- |
| `int N`, `uint N`, `negative fixint`, `positive fixint`            | [Int64](../sql-reference/data-types/int-uint.md)      |
| `bool`                                                             | [UInt8](../sql-reference/data-types/int-uint.md)      |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [String](../sql-reference/data-types/string.md)       |
| `float 32`                                                         | [Float32](../sql-reference/data-types/float.md)       |
| `float 64`                                                         | [Float64](../sql-reference/data-types/float.md)       |
| `uint 16`                                                          | [Date](../sql-reference/data-types/date.md)           |
| `uint 32`                                                          | [DateTime](../sql-reference/data-types/datetime.md)   |
| `uint 64`                                                          | [DateTime64](../sql-reference/data-types/datetime.md) |
| `fixarray`, `array 16`, `array 32`                                 | [Array](../sql-reference/data-types/array.md)         |
| `fixmap`, `map 16`, `map 32`                                       | [Map](../sql-reference/data-types/map.md)             |

기본적으로 추론된 모든 타입은 `Nullable`로 감싸지지만, 설정 `schema_inference_make_columns_nullable`로 이를 변경할 수 있습니다.

<div id="bsoneachrow">
  ### BSONEachRow
</div>

BSONEachRow에서는 각 행이 BSON 문서로 표현됩니다. 스키마 추론 시 ClickHouse는 BSON 문서를 하나씩 읽어 데이터에서 값, 이름, 타입을 추출한 다음, 아래의 타입 매핑을 사용해 이를 ClickHouse 타입으로 변환합니다:

| BSON 유형                                                                                       | ClickHouse 타입                                                                                                 |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `\x08` boolean                                                                                | [Bool](../sql-reference/data-types/boolean.md)                                                                |
| `\x10` int32                                                                                  | [Int32](../sql-reference/data-types/int-uint.md)                                                              |
| `\x12` int64                                                                                  | [Int64](../sql-reference/data-types/int-uint.md)                                                              |
| `\x01` double                                                                                 | [Float64](../sql-reference/data-types/float.md)                                                               |
| `\x09` datetime                                                                               | [DateTime64](../sql-reference/data-types/datetime64.md)                                                       |
| `\x05` binary with`\x00` binary subtype, `\x02` string, `\x0E` symbol, `\x0D` JavaScript code | [String](../sql-reference/data-types/string.md)                                                               |
| `\x07` ObjectId,                                                                              | [FixedString(12)](../sql-reference/data-types/fixedstring.md)                                                 |
| `\x05` binary with `\x04` uuid subtype, size = 16                                             | [UUID](../sql-reference/data-types/uuid.md)                                                                   |
| `\x04` array                                                                                  | [Array](../sql-reference/data-types/array.md)/[Tuple](../sql-reference/data-types/tuple.md) (중첩 타입이 서로 다른 경우) |
| `\x03` document                                                                               | [Named Tuple](../sql-reference/data-types/tuple.md)/[Map](../sql-reference/data-types/map.md) (String 키 사용)   |

기본적으로 추론된 모든 타입은 `Nullable`로 처리되지만, 설정 `schema_inference_make_columns_nullable`을 사용해 변경할 수 있습니다.

<div id="formats-with-constant-schema">
  ## 고정된 스키마를 사용하는 포맷
</div>

이러한 포맷의 데이터는 항상 동일한 스키마를 사용합니다.

<div id="line-as-string">
  ### LineAsString
</div>

이 포맷에서는 ClickHouse가 데이터의 한 줄 전체를 `String` 데이터 타입의 단일 컬럼으로 읽어들입니다. 이 포맷에서 추론되는 타입은 항상 `String`이며, 컬럼 이름은 `line`입니다.

**예시**

```sql
DESC format(LineAsString, 'Hello\nworld!')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-as-string">
  ### JSONAsString
</div>

이 포맷에서는 ClickHouse가 데이터에 포함된 전체 JSON 객체를 `String` 데이터 타입의 단일 컬럼으로 읽어들입니다. 이 포맷에서 추론되는 타입은 항상 `String`이며, 컬럼 이름은 `json`입니다.

**예시**

```sql
DESC format(JSONAsString, '{"x" : 42, "y" : "Hello, World!"}')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-as-object">
  ### JSONAsObject
</div>

이 포맷에서는 ClickHouse가 데이터에 있는 JSON 객체 전체를 `JSON` 데이터 타입의 단일 컬럼으로 읽습니다. 이 포맷에서 추론되는 데이터 타입은 항상 `JSON`이며, 컬럼 이름은 `json`입니다.

**예시**

```sql
DESC format(JSONAsObject, '{"x" : 42, "y" : "Hello, World!"}');
```

```response
┌─name─┬─type─┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ JSON │              │                    │         │                  │                │
└──────┴──────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-modes">
  ## 스키마 추론 모드
</div>

데이터 파일 집합에 대한 스키마 추론은 `default`와 `union`의 2가지 모드로 작동할 수 있습니다.
모드는 `schema_inference_mode` 설정으로 제어됩니다.

<div id="default-schema-inference-mode">
  ### 기본 모드
</div>

기본 모드에서 ClickHouse는 모든 파일이 동일한 스키마를 가진다고 가정하고, 스키마를 추론하는 데 성공할 때까지 파일을 하나씩 읽습니다.

예시:

`data1.jsonl`, `data2.jsonl`, `data3.jsonl`의 3개 파일이 있고, 각 파일의 내용이 다음과 같다고 가정하겠습니다.

`data1.jsonl`:

```json
{"field1" :  1, "field2" :  null}
{"field1" :  2, "field2" :  null}
{"field1" :  3, "field2" :  null}
```

`data2.jsonl`:

```json
{"field1" :  4, "field2" :  "Data4"}
{"field1" :  5, "field2" :  "Data5"}
{"field1" :  6, "field2" :  "Data5"}
```

`data3.jsonl`:

```json
{"field1" :  7, "field2" :  "Data7", "field3" :  [1, 2, 3]}
{"field1" :  8, "field2" :  "Data8", "field3" :  [4, 5, 6]}
{"field1" :  9, "field2" :  "Data9", "field3" :  [7, 8, 9]}
```

이 3개의 파일에 대해 스키마 추론을 시도해 보겠습니다:

```sql title="Query"
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='default'
```

```response title="Response"
┌─name───┬─type─────────────┐
│ field1 │ Nullable(Int64)  │
│ field2 │ Nullable(String) │
└────────┴──────────────────┘
```

확인할 수 있듯이 파일 `data3.jsonl`의 `field3`는 없습니다.
이는 ClickHouse가 먼저 파일 `data1.jsonl`에서 스키마를 자동 추론하려고 했지만 `field2`에 null 값만 있어 실패했고,
이후 `data2.jsonl`에서 스키마 자동 추론에 성공하면서 파일 `data3.jsonl`의 데이터는 읽지 않았기 때문입니다.

<div id="default-schema-inference-mode-1">
  ### 유니언 모드
</div>

유니언 모드에서는 파일마다 서로 다른 스키마를 가질 수 있다고 가정하므로, ClickHouse는 모든 파일의 스키마를 추론한 다음 이를 공통 스키마로 통합합니다.

다음과 같은 내용이 들어 있는 3개의 파일 `data1.jsonl`, `data2.jsonl`, `data3.jsonl`이 있다고 가정해 보겠습니다:

`data1.jsonl`:

```json
{"field1" :  1}
{"field1" :  2}
{"field1" :  3}
```

`data2.jsonl`:

```json
{"field2" :  "Data4"}
{"field2" :  "Data5"}
{"field2" :  "Data5"}
```

`data3.jsonl`:

```json
{"field3" :  [1, 2, 3]}
{"field3" :  [4, 5, 6]}
{"field3" :  [7, 8, 9]}
```

이 3개 파일에 대해 스키마 추론을 사용해 보겠습니다:

```sql title="Query"
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='union'
```

```response title="Response"
┌─name───┬─type───────────────────┐
│ field1 │ Nullable(Int64)        │
│ field2 │ Nullable(String)       │
│ field3 │ Array(Nullable(Int64)) │
└────────┴────────────────────────┘
```

보시다시피 모든 파일의 모든 필드가 포함되어 있습니다.

참고:

* 일부 파일에는 최종 스키마의 일부 컬럼이 없을 수 있으므로, union mode는 컬럼 부분 집합 읽기를 지원하는 포맷(JSONEachRow, Parquet, TSVWithNames 등)에서만 지원되며 다른 포맷(CSV, TSV, JSONCompactEachRow 등)에서는 작동하지 않습니다.
* ClickHouse가 파일 중 하나에서 스키마를 추론하지 못하면 예외가 발생합니다.
* 파일이 많으면 모든 파일에서 스키마를 읽는 데 시간이 많이 걸릴 수 있습니다.

<div id="automatic-format-detection">
  ## 자동 포맷 감지
</div>

데이터 포맷이 지정되지 않았고 파일 확장자로도 확인할 수 없는 경우, ClickHouse는 파일 내용을 기반으로 포맷을 자동으로 감지합니다.

**예시:**

다음과 같은 내용의 `data`가 있다고 가정하겠습니다.

```csv
"a","b"
1,"Data1"
2,"Data2"
3,"Data3"
```

포맷이나 구조를 지정하지 않고도 이 파일을 확인하고 쿼리할 수 있습니다.

```sql
:) desc file(data);
```

```response
┌─name─┬─type─────────────┐
│ a    │ Nullable(Int64)  │
│ b    │ Nullable(String) │
└──────┴──────────────────┘
```

```sql
:) select * from file(data);
```

```response
┌─a─┬─b─────┐
│ 1 │ Data1 │
│ 2 │ Data2 │
│ 3 │ Data3 │
└───┴───────┘
```

:::note
ClickHouse는 일부 포맷만 감지할 수 있으며, 이 감지에도 시간이 걸리므로 포맷은 명시적으로 지정하는 것이 좋습니다.
:::