---
description: 'Amazon S3, Azure, HDFS 또는 로컬에 저장된 Apache Iceberg 테이블을
  테이블처럼 사용할 수 있는 읽기 전용 인터페이스를 제공합니다.'
sidebar_label: 'iceberg'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'reference'
---

Amazon S3, Azure, HDFS 또는 로컬에 저장된 Apache [Iceberg](https://iceberg.apache.org/) 테이블을 테이블처럼 사용할 수 있는 읽기 전용 인터페이스를 제공합니다.

<div id="syntax">
  ## 구문
</div>

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

<div id="arguments">
  ## 인수
</div>

각 인수에 대한 설명은 각각 테이블 함수 `s3`, `azureBlobStorage`, `HDFS`, `file`의 인수 설명과 같습니다.
`format`은 Iceberg 테이블의 데이터 파일 포맷을 의미합니다.

`icebergS3`에서는 선택적 `extra_credentials` 매개변수를 사용해 ClickHouse Cloud에서 역할 기반 접근을 위한 `role_arn`을 전달할 수 있습니다. 구성 단계는 [Secure S3](/ko/cloud/data-sources/secure-s3)를 참조하십시오.

<div id="returned-value">
  ### 반환 값
</div>

지정한 구조로 지정된 Iceberg 테이블의 데이터를 읽기 위한 테이블입니다.

<div id="example">
  ### 예시
</div>

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
ClickHouse는 현재 `icebergS3`, `icebergAzure`, `icebergHDFS` 및 `icebergLocal` 테이블 함수와 `IcebergS3`, `icebergAzure`, `IcebergHDFS` 및 `IcebergLocal` 테이블 엔진을 통해 Iceberg 포맷의 v1 및 v2를 읽을 수 있도록 지원합니다.
:::

<div id="defining-a-named-collection">
  ## 명명된 컬렉션 정의
</div>

다음은 URL과 자격 증명을 저장할 명명된 컬렉션을 구성하는 예시입니다:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

<div id="iceberg-writes-catalogs">
  ## 데이터 카탈로그 사용하기
</div>

Iceberg 테이블은 [REST 카탈로그](https://iceberg.apache.org/rest-catalog-spec/), [AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html), [Unity Catalog](https://www.unitycatalog.io/) 등 다양한 데이터 카탈로그와 함께 사용할 수도 있습니다.

:::important
카탈로그를 사용하는 경우 대부분 `DataLakeCatalog` 데이터베이스 엔진을 사용하는 것이 좋습니다. 이 엔진은 ClickHouse를 카탈로그에 연결해 테이블을 발견합니다. `IcebergS3` 테이블 엔진으로 개별 테이블을 수동 생성하는 대신 이 데이터베이스 엔진을 사용할 수 있습니다.
:::

이를 사용하려면 `IcebergS3` 엔진으로 테이블을 생성하고 필요한 설정을 지정하십시오.

예를 들어, MinIO 스토리지와 함께 REST 카탈로그를 사용하는 경우:

```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
```

또는 S3와 함께 AWS Glue Data Catalog를 사용할 경우:

```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
```

<div id="schema-evolution">
  ## 스키마 진화
</div>

현재 CH를 사용하면 시간이 지나면서 스키마가 변경된 Iceberg 테이블을 읽을 수 있습니다. 현재는 컬럼이 추가되거나 삭제되고 순서가 변경된 테이블 읽기를 지원합니다. 또한 값이 반드시 있어야 하는 컬럼을 NULL을 허용하는 컬럼으로 변경할 수도 있습니다. 추가로, 단순 타입에 대해 허용된 타입 변환도 지원하며, 구체적으로는 다음과 같습니다:  

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) where P&#39; &gt; P.

현재는 중첩된 구조를 변경하거나 배열 및 맵 내부 요소의 타입을 변경할 수 없습니다.

<div id="partition-pruning">
  ## 파티션 프루닝
</div>

ClickHouse는 Iceberg 테이블의 SELECT 쿼리에서 파티션 프루닝을 지원합니다. 이를 통해 관련 없는 데이터 파일을 스키핑하여 쿼리 성능을 최적화할 수 있습니다. 파티션 프루닝을 활성화하려면 `use_iceberg_partition_pruning = 1`로 설정하십시오. Iceberg 파티션 프루닝에 대한 자세한 내용은 https://iceberg.apache.org/spec/#partitioning 를 참조하십시오.

<div id="time-travel">
  ## 타임 트래블
</div>

ClickHouse는 Iceberg 테이블의 타임 트래블 기능을 지원하므로, 특정 타임스탬프 또는 스냅샷 ID를 지정해 과거 시점의 데이터를 쿼리할 수 있습니다.

<div id="deleted-rows">
  ## 삭제된 행이 포함된 테이블 처리
</div>

현재는 [position deletes](https://iceberg.apache.org/spec/#position-delete-files)가 있는 Iceberg 테이블만 지원됩니다.

다음 삭제 방식은 **지원되지 않습니다**:

* [Equality deletes](https://iceberg.apache.org/spec/#equality-delete-files)
* [Deletion vectors](https://iceberg.apache.org/spec/#deletion-vectors) (v3에서 도입됨)

<div id="basic-usage">
  ### 기본 사용법
</div>

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
```

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
```

참고: 동일한 쿼리에서는 `iceberg_timestamp_ms`와 `iceberg_snapshot_id` 매개변수를 동시에 지정할 수 없습니다.

<div id="important-considerations">
  ### 중요한 고려 사항
</div>

* **스냅샷(snapshot)**은 일반적으로 다음과 같은 경우 생성됩니다:

* 새 데이터가 테이블(table)에 기록될 때

* 데이터 컴팩션(compaction)이 수행될 때

* **스키마 변경(schema changes)은 일반적으로 스냅샷을 생성하지 않습니다** - 따라서 스키마 진화(schema evolution)를 거친 테이블에서 타임 트래블을 사용할 때 중요한 동작 차이가 발생합니다.

<div id="example-scenarios">
  ### 예시 시나리오
</div>

CH는 아직 Iceberg 테이블에 쓰기 작업을 지원하지 않으므로, 모든 시나리오는 Spark를 사용해 작성되었습니다.

<div id="scenario-1">
  #### 시나리오 1: 새 스냅샷 없이 발생한 스키마 변경
</div>

다음 작업 시퀀스를 살펴보십시오:

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

서로 다른 타임스탬프에서의 쿼리 결과:

* ts1 &amp; ts2: 원래 있던 2개 컬럼만 표시됩니다
* ts3: 3개 컬럼이 모두 표시되며, 첫 번째 행의 price는 NULL로 표시됩니다

<div id="scenario-2">
  #### 시나리오 2: 과거 스키마와 현재 스키마의 차이
</div>

현재 시점에서 실행한 타임 트래블 쿼리는 현재 테이블(table)과 다른 스키마(schema)를 보여줄 수 있습니다:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

이는 `ALTER TABLE`이 새 스냅샷을 생성하지 않으며, 현재 테이블(table)의 경우 Spark가 스냅샷이 아니라 최신 메타데이터 파일에서 `schema_id` 값을 가져오기 때문에 발생합니다.

<div id="scenario-3">
  #### 시나리오 3: 과거 스키마와 현재 스키마의 차이
</div>

두 번째는, 타임 트래블을 수행할 때 테이블에 데이터가 한 번도 기록되기 전의 상태는 조회할 수 없다는 점입니다:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

ClickHouse에서는 동작 방식이 Spark와 일관됩니다. 즉, Spark의 Select 쿼리를 ClickHouse의 Select 쿼리로 바꿔 생각해도 동일하게 작동합니다.

<div id="metadata-file-resolution">
  ## 메타데이터 파일 결정
</div>

ClickHouse에서 `iceberg` 테이블 함수를 사용할 때 시스템은 Iceberg 테이블 구조를 설명하는 올바른 metadata.json 파일을 찾아야 합니다. 이 결정 과정은 다음과 같이 이루어집니다:

<div id="candidate-search">
  ### 후보 검색(우선순위 순서)
</div>

1. **직접 경로 지정**:
   *`iceberg_metadata_file_path`를 설정하면 시스템은 이 정확한 경로를 Iceberg 테이블 디렉터리 경로와 결합해 사용합니다.

* 이 설정을 지정하면 다른 모든 해상도 설정은 무시됩니다.

2. **테이블 UUID 일치**:
   *`iceberg_metadata_table_uuid`가 지정되면 시스템은 다음과 같이 동작합니다:
   *`metadata` 디렉터리의 `.metadata.json` 파일만 확인합니다
   *지정한 UUID와 일치하는 `table-uuid` 필드를 포함한 파일만 필터링합니다(대소문자 구분 없음)

3. **기본 검색**:
   *위 두 설정이 모두 지정되지 않으면 `metadata` 디렉터리의 모든 `.metadata.json` 파일이 후보가 됩니다

<div id="most-recent-file">
  ### 가장 최신 메타데이터 File 선택
</div>

위 규칙을 사용해 후보 파일을 식별한 후, 시스템은 그중 가장 최신 파일을 판단합니다:

* `iceberg_recent_metadata_file_by_last_updated_ms_field`가 활성화된 경우:

* `last-updated-ms` 값이 가장 큰 파일이 선택됩니다

* 그렇지 않으면:

* 버전 번호가 가장 높은 파일이 선택됩니다

* (버전은 `V.metadata.json` 또는 `V-uuid.metadata.json` 형식의 파일 이름에서 `V`로 표시됩니다)

**참고**: 위에서 언급한 모든 설정은 테이블 함수 설정이며(전역 설정이나 쿼리 수준 설정이 아님), 아래와 같이 지정해야 합니다:

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**참고**: 일반적으로 Iceberg 카탈로그가 메타데이터 해석을 담당하지만, ClickHouse의 `iceberg` 테이블 함수는 S3에 저장된 파일을 Iceberg 테이블로 직접 해석하므로 이러한 해석 규칙을 이해하는 것이 중요합니다.

<div id="metadata-cache">
  ## 메타데이터 캐시
</div>

`Iceberg` 테이블 엔진과 테이블 함수는 manifest 파일, manifest 목록, metadata JSON 정보를 저장하는 메타데이터 캐시를 지원합니다. 캐시는 메모리에 저장됩니다. 이 기능은 `use_iceberg_metadata_files_cache` 설정으로 제어되며, 기본적으로 활성화되어 있습니다.

<div id="aliases">
  ## 별칭
</div>

테이블 함수 `iceberg`는 이제 `icebergS3`의 별칭으로 사용됩니다.

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — 파일 경로입니다. 유형: `LowCardinality(String)`.
* `_file` — 파일 이름입니다. 유형: `LowCardinality(String)`.
* `_size` — 파일 크기(바이트)입니다. 유형: `Nullable(UInt64)`. 파일 크기를 알 수 없으면 값은 `NULL`입니다.
* `_time` — 파일의 마지막 수정 시간입니다. 유형: `Nullable(DateTime)`. 시간을 알 수 없으면 값은 `NULL`입니다.
* `_etag` — 파일의 etag 값입니다. 유형: `LowCardinality(String)`. etag를 알 수 없으면 값은 `NULL`입니다.

<div id="writes-into-iceberg-table">
  ## Iceberg 테이블에 쓰기
</div>

버전 25.7부터 ClickHouse에서 사용자의 Iceberg 테이블 수정이 지원됩니다.

현재 이 기능은 실험적 기능이므로 먼저 활성화해야 합니다:

```sql
SET allow_insert_into_iceberg = 1;
```

<div id="create-iceberg-table">
  ### Iceberg 테이블 생성
</div>

직접 빈 Iceberg 테이블을 생성하려면 읽을 때와 동일한 명령을 사용하되, 스키마를 명시적으로 지정하십시오.
쓰기 작업은 Parquet, Avro, ORC와 같은 Iceberg 사양의 모든 데이터 포맷을 지원합니다.

<div id="example">
  ### 예시
</div>

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

참고: 버전 힌트 파일을 생성하려면 `iceberg_use_version_hint` 설정을 활성화하세요.
`metadata.json` 파일을 압축하려면 `iceberg_metadata_compression_method` 설정에 코덱 이름을 지정하세요.

<div id="writes-inserts">
  ### INSERT
</div>

새 테이블을 만든 후 일반적인 ClickHouse 구문으로 데이터를 삽입할 수 있습니다.

<div id="example">
  ### 예시
</div>

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-delete">
  ### DELETE
</div>

ClickHouse는 merge-on-read 포맷에서 추가 행을 삭제하는 작업도 지원합니다.
이 쿼리는 포지션 삭제 파일을 포함하는 새 스냅샷을 생성합니다.

<div id="example">
  ### 예시
</div>

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-schema-evolution">
  ### 스키마 진화
</div>

ClickHouse에서는 단순 타입(Tuple, Array, Map이 아닌)의 컬럼을 추가, 삭제, 수정하거나 이름을 변경할 수 있습니다.

<div id="example">
  ### 예시
</div>

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

<div id="iceberg-writes-compaction">
  ### 컴팩션
</div>

ClickHouse는 Iceberg 테이블의 컴팩션을 지원합니다. 현재는 메타데이터를 업데이트하면서 포지션 삭제 파일을 데이터 파일에 머지할 수 있습니다. 이전 스냅샷 ID와 타임스탬프는 변경되지 않으므로 동일한 값으로 타임 트래블 기능을 계속 사용할 수 있습니다.

사용 방법:

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-expire-snapshots">
  ### 스냅샷 만료
</div>

Iceberg 테이블은 각 INSERT, DELETE 또는 UPDATE 작업이 수행될 때마다 스냅샷이 누적됩니다. 시간이 지나면서 이로 인해 많은 수의 스냅샷과 관련 데이터 파일이 쌓일 수 있습니다. `expire_snapshots` 명령은 오래된 스냅샷을 제거하고, 유지되는 어떤 스냅샷에서도 더 이상 참조되지 않는 데이터 파일을 정리합니다.

**구문:**

```sql
ALTER TABLE iceberg_table EXECUTE expire_snapshots(
    ['timestamp']
    [, expire_before = 'timestamp']
    [, retention_period = '3d']
    [, retain_last = 100]
    [, snapshot_ids = [1, 2, 3, 4]]
    [, dry_run = 1]
);
```

기본적으로 어떤 스냅샷을 유지할지는 [보존 정책](#iceberg-snapshot-retention-policy)(테이블 속성 `min-snapshots-to-keep`, `max-snapshot-age-ms`, 및 ref별 재정의)으로 결정됩니다. `snapshot_ids`를 지정하면 보존 정책을 우회하며, 나열된 스냅샷만 만료 대상으로 고려됩니다.

**인수:**

* `'timestamp'` (위치 인수) 또는 `expire_before = 'timestamp'` — **서버의 시간대**로 해석되는 datetime 문자열입니다(예: `'2024-06-01 00:00:00'`). 안전장치로 동작합니다. `timestamp-ms`가 이 값과 같거나 이후인 스냅샷은 보존 정책상 원래 만료 대상이더라도 만료되지 않도록 보호됩니다. `snapshot_ids`와 함께 사용할 수도 있으며, 이 경우 나열된 스냅샷 중 timestamp와 같거나 그보다 최신인 스냇샷은 만료되지 않습니다.
* `retention_period = '<duration>'` — 이번 호출에만 테이블 수준의 `history.expire.max-snapshot-age-ms`를 재정의합니다. 이 기간보다 오래된 스냅샷(현재 시점 기준)이 만료 후보가 됩니다. 값은 하나 이상의 `{number}{unit}` 쌍을 이어 붙인 기간 문자열입니다. 지원 단위: `y` (365일), `w` (7일), `d` (24시간), `h` (60분), `m` (60초), `s` (1초), `ms` (1밀리초). 단위는 함께 조합할 수 있습니다. 예: `'3d'`, `'12h'`, `'1d12h30m'`, `'500ms'`.
* `retain_last = N` — 이번 호출에만 테이블 수준의 `history.expire.min-snapshots-to-keep`를 재정의합니다. 기간과 관계없이 최소 `N`개의 스냅샷은 항상 유지됩니다.
* `snapshot_ids = [id1, id2, ...]` — 나열된 스냅샷 ID만 정확히 만료시킵니다(현재 스냅샷, 브랜치 또는 태그가 참조하는 스냅샷은 제외). 이 모드는 보존 정책을 완전히 우회하며 `retention_period` 또는 `retain_last`와 함께 사용할 수 없습니다.
* `dry_run = 1` — 무엇이 만료될지를 계산하고, 새 메타데이터를 기록하거나 파일을 삭제하지 않은 상태에서 메트릭을 반환합니다.

:::note
`retention_period`와 `retain_last`는 **테이블 수준**의 기본 보존값만 재정의합니다. Iceberg 테이블 속성에 구성된 ref별(브랜치/태그) 보존 재정의(예: `refs.<branch>.min-snapshots-to-keep`)는 절대 재정의되지 않으며, 항상 테이블 메타데이터에 지정된 대로 적용됩니다.
:::

**예시:**

```sql
SET allow_insert_into_iceberg = 1;

-- Create some snapshots by inserting data
INSERT INTO iceberg_table VALUES (1);
INSERT INTO iceberg_table VALUES (2);
INSERT INTO iceberg_table VALUES (3);

-- Expire using retention policy only
ALTER TABLE iceberg_table EXECUTE expire_snapshots();

-- Expire with a safety fuse: protect snapshots newer than the timestamp (positional syntax)
ALTER TABLE iceberg_table EXECUTE expire_snapshots('2025-01-01 00:00:00');

-- Same using the named argument form
ALTER TABLE iceberg_table EXECUTE expire_snapshots(expire_before = '2025-01-01 00:00:00');

-- Override retention parameters for one execution
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '3d', retain_last = 10);

-- Expire explicit snapshots
ALTER TABLE iceberg_table EXECUTE expire_snapshots(snapshot_ids = [101, 102, 103]);

-- Dry-run preview (no metadata updates, no file deletes)
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '1d', dry_run = 1);
```

**출력:**

이 명령은 두 개의 컬럼(`metric_name String`, `metric_value Int64`)으로 구성된 테이블을 반환하며, 메트릭마다 1개의 행이 포함됩니다. 메트릭 이름은 [Iceberg 사양](https://iceberg.apache.org/docs/latest/spark-procedures/#output)을 따릅니다:

| metric&#95;name                       | 설명                            |
| ------------------------------------- | ----------------------------- |
| `deleted_data_files_count`            | 삭제된 데이터 파일 수                  |
| `deleted_position_delete_files_count` | 삭제된 포지션 삭제 파일 수               |
| `deleted_equality_delete_files_count` | 삭제된 equality 삭제 파일 수          |
| `deleted_manifest_files_count`        | 삭제된 manifest 파일 수             |
| `deleted_manifest_lists_count`        | 삭제된 manifest 목록 파일 수          |
| `deleted_statistics_files_count`      | 삭제된 통계 파일 수(현재는 항상 0)         |
| `dry_run`                             | dry-run 모드이면 `1`, 일반 실행이면 `0` |

이 명령은 다음 단계를 수행합니다:

1. 보존 정책(아래 참조)을 평가하여 어떤 스냅샷을 보존해야 하는지 결정합니다
2. timestamp 인수가 제공된 경우, 해당 timestamp 이상인 모든 스냅샷도 추가로 보호합니다
3. 정책에 따라 보존되지 않고 timestamp 보호 조건에도 해당하지 않는 스냅샷을 만료시킵니다
4. 만료된 스냅샷에만 전용으로 연결된 파일을 계산합니다
5. 일반 모드에서는 만료된 스냅샷이 제거된 새 metadata를 생성합니다
6. 일반 모드에서는 더 이상 참조되지 않는 manifest 목록, manifest 파일, 데이터 파일을 물리적으로 삭제합니다
7. `dry_run = 1` 모드에서는 5단계와 6단계를 건너뛰고 계산된 메트릭만 반환합니다

<div id="iceberg-snapshot-retention-policy">
  #### 스냅샷 보존 정책
</div>

`expire_snapshots` 명령은 [Iceberg 스냅샷 보존 정책](https://iceberg.apache.org/spec/#snapshot-retention-policy)을 따릅니다. 보존 설정은 Iceberg 테이블 속성과 참조별 재정의를 통해 구성됩니다:

| 속성                                     | 범위    | 기본값                                                                | 설명                                                |
| -------------------------------------- | ----- | ------------------------------------------------------------------ | ------------------------------------------------- |
| `history.expire.min-snapshots-to-keep` | Table | `iceberg_expire_default_min_snapshots_to_keep` (기본값 `1`)           | 각 브랜치의 조상 체인에서 유지할 최소 스냅샷 수                       |
| `history.expire.max-snapshot-age-ms`   | Table | `iceberg_expire_default_max_snapshot_age_ms` (기본값 `432000000`, 5일) | 브랜치에서 유지할 스냅샷의 최대 보존 기간(ms)                       |
| `history.expire.max-ref-age-ms`        | Table | `iceberg_expire_default_max_ref_age_ms` (기본값 `∞`)                  | 스냅샷 참조(브랜치 또는 태그) 자체가 제거되기 전까지 유지할 수 있는 최대 기간(ms) |

각 스냅샷 참조(Iceberg 메타데이터의 `refs`)는 참조별 필드인 `min-snapshots-to-keep`, `max-snapshot-age-ms`, `max-ref-age-ms`로 이를 재정의할 수 있습니다.

**보존 평가:**

* **각 브랜치**(`main` 포함): 브랜치 head부터 시작해 조상 체인을 따라갑니다. 다음 조건 중 하나라도 참이면 스냅샷이 유지됩니다:
  * 해당 스냅샷이 체인에서 처음 `min-snapshots-to-keep`개 중 하나인 경우
  * 스냅샷의 경과 시간이 `max-snapshot-age-ms` 이내인 경우(즉, `now - timestamp-ms <= max-snapshot-age-ms`)
* **태그**: 태그가 가리키는 스냅샷은 유지되며, 태그가 `max-ref-age-ms`를 초과한 경우에는 태그 참조가 제거됩니다
* `max-ref-age-ms`를 초과한 **`main`이 아닌 참조**는 완전히 제거됩니다(`main` 브랜치는 제거되지 않음)
* 존재하지 않는 스냅샷을 가리키는 **dangling 참조**는 경고와 함께 제거됩니다
* **현재 스냅샷은 보존 설정과 관계없이 항상 유지됩니다**

**필요한 권한:**

`ALTER TABLE EXECUTE` 권한이 필요합니다. 이 권한은 ClickHouse 액세스 제어 계층 구조에서 `ALTER TABLE`의 하위 권한입니다. 이 권한만 개별적으로 부여하거나 상위 권한을 통해 부여할 수 있습니다:

```sql
-- Grant only EXECUTE permission
GRANT ALTER TABLE EXECUTE ON my_iceberg_table TO my_user;

-- Or grant all ALTER TABLE permissions (includes ALTER TABLE EXECUTE)
GRANT ALTER TABLE ON my_iceberg_table TO my_user;
```

:::note

* Iceberg 형식 버전 2 테이블만 지원합니다(v1 스냅샷은 정리 시 파일을 안전하게 식별하는 데 필요한 `manifest-list`를 보장하지 않음)
* 현재 스냅샷은 지정된 timestamp보다 오래되었더라도 항상 유지됩니다
* `allow_insert_into_iceberg` setting이 활성화되어 있어야 합니다
* `allow_experimental_expire_snapshots` setting이 활성화되어 있어야 합니다
* ClickHouse가 메타데이터를 업데이트할 때는 카탈로그 자체의 권한 부여(REST 카탈로그 인증, AWS Glue IAM 등)가 별도로 적용됩니다
  :::

<div id="iceberg-remove-orphan-files">
  ### 고아 파일 제거
</div>

고아 파일은 Iceberg 테이블 메타데이터의 어떤 스냅샷에서도 참조되지 않는 스토리지 내 파일입니다. 이러한 파일은 쓰기 실패, compaction 후 불완전한 정리, 중단된 작업으로 인해 누적되며 스토리지가 무한정 증가할 수 있습니다. `remove_orphan_files` 명령은 이러한 고아 파일을 식별해 제거합니다.

**구문:**

```sql
-- Positional form: single unnamed older_than argument
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('timestamp')

-- Named form
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = 'timestamp',
    location = 'path',
    dry_run = 0|1
)

-- No arguments: use all defaults (older_than = 3 days ago)
ALTER TABLE iceberg_table EXECUTE remove_orphan_files()
```

**매개변수:**

| 매개변수         | 유형                   | 기본값                                                     | 설명                                                                                         |
| ------------ | -------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `older_than` | `String` (timestamp) | 3일 전 (`iceberg_orphan_files_older_than_seconds`로 구성 가능) | 마지막 수정 시간이 이 timestamp보다 이전인 파일만 고아 파일 후보로 간주합니다. 진행 중인 쓰기 작업의 파일이 삭제되지 않도록 방지하는 안전 장치입니다. |
| `location`   | `String`             | 테이블 위치                                                  | 스캔 범위를 테이블 위치 아래의 특정 하위 디렉터리(예: `'data/'` 또는 `'metadata/'`)로 제한합니다.                        |
| `dry_run`    | `UInt64`             | `0`                                                     | `1`로 설정하면 고아 파일을 식별하고, 실제로는 아무것도 삭제하지 않은 상태로 결과 요약을 반환합니다.                                 |

**예시:**

```sql
-- Remove orphan files older than a specific timestamp
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('2026-03-01 00:00:00');

-- Dry run: preview which files would be deleted
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(dry_run = 1);

-- Scan only the data directory
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = '2026-03-01 00:00:00',
    location = 'data/'
);

-- Combine positional older_than with named arguments
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    '2026-03-01 00:00:00',
    dry_run = 1
);
```

**출력:**

이 명령은 `metric_name` 및 `metric_value` 컬럼이 있는 테이블을 반환하며, 범주별 삭제된 파일 수(또는 dry&#95;run 모드에서 삭제될 파일 수)를 보여줍니다. 파일 범주는 파일 이름 규칙을 기반으로 하는 best-effort 휴리스틱으로 분류되며, 특정 패턴과 일치하지 않는 파일은 기본적으로 `deleted_data_files_count`에 집계됩니다:

| metric&#95;name                                     | metric&#95;value |
| --------------------------------------------------- | ---------------- |
| deleted&#95;data&#95;files&#95;count                | 5                |
| deleted&#95;position&#95;delete&#95;files&#95;count | 2                |
| deleted&#95;equality&#95;delete&#95;files&#95;count | 0                |
| deleted&#95;manifest&#95;files&#95;count            | 3                |
| deleted&#95;manifest&#95;lists&#95;count            | 1                |
| deleted&#95;metadata&#95;files&#95;count            | 0                |
| deleted&#95;statistics&#95;files&#95;count          | 0                |
| skipped&#95;missing&#95;metadata&#95;count          | 0                |
| failed&#95;deletions&#95;count                      | 0                |

**설정:**

| Setting                                   | Type     | Default           | Description                                |
| ----------------------------------------- | -------- | ----------------- | ------------------------------------------ |
| `allow_iceberg_remove_orphan_files`       | `Bool`   | `false`           | 기능을 활성화하기 위한 제어 설정(Experimental)입니다.       |
| `iceberg_orphan_files_older_than_seconds` | `UInt64` | `259200` (3 days) | 인수를 생략했을 때 적용되는 기본 `older_than` 임계값(초)입니다. |

:::note

* **Iceberg 형식 버전 2 (또는 그 이상)가 필요합니다.** 버전 1 테이블은 스냅샷에 `manifest-list` 포인터가 없어 안전하게 도달 가능한 파일 집합을 판별할 수 없으므로 허용되지 않습니다. v1 테이블에서 이 명령을 실행하면 `BAD_ARGUMENTS` 오류가 반환됩니다.
* `allow_insert_into_iceberg` 및 `allow_iceberg_remove_orphan_files` 설정이 모두 활성화되어 있어야 합니다
* 만료된 스냅샷에서만 참조되는 파일이 먼저 정리되도록 `remove_orphan_files` 전에 `expire_snapshots`를 실행하는 것이 좋습니다
* 삭제 전에 orphan files를 미리 확인하려면 `dry_run = 1`을 사용하십시오
* `older_than` 임계값은 진행 중인 쓰기 작업의 파일이 삭제되지 않도록 보호하며, 기본 3일 임계값은 충분한 안전 여유를 제공합니다
  :::

<div id="see-also">
  ## 관련 항목
</div>

* [Iceberg 엔진](/ko/engines/table-engines/integrations/iceberg.md)
* [Iceberg 클러스터 테이블 함수](/ko/sql-reference/table-functions/icebergCluster.md)