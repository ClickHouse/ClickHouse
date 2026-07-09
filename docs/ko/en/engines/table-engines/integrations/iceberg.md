---
description: '이 엔진은 Amazon S3, Azure, HDFS 및 로컬에 저장된 기존 Apache Iceberg
  테이블에 대한 읽기 전용 통합을 제공합니다.'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'Iceberg 테이블 엔진'
doc_type: 'reference'
---

:::warning
ClickHouse에서 Iceberg 데이터로 작업할 때는 [Iceberg 테이블 함수](/ko/sql-reference/table-functions/iceberg.md)을 사용하는 것을 권장합니다. 현재 Iceberg 테이블 함수는 충분한 기능을 제공하며, Iceberg 테이블에 대해 부분적인 읽기 전용 인터페이스를 제공합니다.

Iceberg 테이블 엔진도 사용할 수 있지만 몇 가지 제한이 있을 수 있습니다. ClickHouse는 외부에서 스키마가 변경되는 테이블을 지원하도록 원래 설계되지 않았기 때문에, 이 점이 Iceberg 테이블 엔진의 동작에 영향을 줄 수 있습니다. 그 결과, 일반 테이블에서는 작동하는 일부 기능을 사용할 수 없거나 제대로 작동하지 않을 수 있으며, 특히 기존 분석기를 사용하는 경우 더욱 그렇습니다.

최적의 호환성을 위해 Iceberg 테이블 엔진 지원이 계속 개선되는 동안에는 Iceberg 테이블 함수를 사용하는 것을 권장합니다.
:::

이 엔진은 Amazon S3, Azure, HDFS 및 로컬에 저장된 기존 Apache [Iceberg](https://iceberg.apache.org/) 테이블에 대한 읽기 전용 통합을 제공합니다.

<div id="create-table">
  ## 테이블 생성
</div>

Iceberg 테이블은 저장소에 이미 존재해야 하며, 이 명령으로는 새 테이블을 생성하기 위한 DDL 매개변수를 지정할 수 없습니다.

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression], [,extra_credentials])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## 엔진 인수
</div>

인수 설명은 각각 `S3`, `AzureBlobStorage`, `HDFS`, `File` 엔진의 인수 설명과 동일합니다.
`format`은 Iceberg 테이블의 데이터 파일 포맷을 나타냅니다.

`IcebergS3`에서는 선택적 `extra_credentials` 매개변수를 사용하여 ClickHouse Cloud에서 역할 기반 접근을 위한 `role_arn`을 전달할 수 있습니다. 구성 단계는 [Secure S3](/ko/cloud/data-sources/secure-s3)를 참조하십시오.

엔진 매개변수는 [이름이 지정된 컬렉션](../../../operations/named-collections.md)을 사용하여 지정할 수 있습니다.

<div id="example">
  ### 예시
</div>

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

이름이 지정된 컬렉션 사용:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

<div id="aliases">
  ## 별칭
</div>

`Iceberg` 테이블 엔진은 `disk` 설정을 기준으로 스토리지 백엔드를 자동으로 감지하고, 그에 따라 `IcebergS3`, `IcebergAzure` 또는 `IcebergLocal`을 사용합니다. `disk`를 지정하지 않으면 기본적으로 `IcebergS3` 구현을 사용합니다.

<div id="data-types">
  ## 데이터 타입
</div>

다음 표는 읽기 목적으로 스키마를 추론할 때 Iceberg 데이터 타입이 ClickHouse 데이터 타입에 어떻게 매핑되는지 보여줍니다.

<div id="primitive-types">
  ### 기본 타입
</div>

| Iceberg 타입         | ClickHouse 타입          | 참고                              |
| ------------------ | ---------------------- | ------------------------------- |
| `boolean`          | `Bool`                 |                                 |
| `int`              | `Int32`                |                                 |
| `long`, `bigint`   | `Int64`                |                                 |
| `float`            | `Float32`              |                                 |
| `double`           | `Float64`              |                                 |
| `date`             | `Date32`               |                                 |
| `time`             | `Int64`                | 자정 이후 경과한 마이크로초                 |
| `timestamp`        | `DateTime64(6)`        | 마이크로초, 시간대 없음                   |
| `timestamptz`      | `DateTime64(6, 'UTC')` | 마이크로초, UTC 시간대                  |
| `timestamp_ns`     | `DateTime64(9)`        | 나노초, 시간대 없음 (Iceberg v3부터만 지원)  |
| `timestamptz_ns`   | `DateTime64(9, 'UTC')` | 나노초, UTC 시간대 (Iceberg v3부터만 지원) |
| `string`, `binary` | `String`               |                                 |
| `uuid`             | `UUID`                 |                                 |
| `fixed(N)`         | `FixedString(N)`       |                                 |
| `decimal(P, S)`    | `Decimal(P, S)`        |                                 |

<div id="complex-types">
  ### 복합 타입
</div>

| Iceberg 타입 | ClickHouse 타입 |
| ---------- | ------------- |
| `list`     | `Array`       |
| `map`      | `Map`         |
| `struct`   | `Tuple`       |

<div id="schema-evolution">
  ## 스키마 진화
</div>

ClickHouse는 시간에 따라 스키마가 변경된 Iceberg 테이블을 읽을 수 있습니다. 여기에는 컬럼이 추가, 삭제 또는 재정렬된 테이블과, 컬럼이 필수(required)에서 널 허용(nullable)으로 변경된 테이블이 포함됩니다. 또한 다음과 같은 타입 변환이 지원됩니다.

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) where P&#39; &gt; P.

현재는 중첩 구조나 배열 및 맵 내 요소의 타입은 변경할 수 없습니다.

동적 스키마 추론을 사용해 생성한 테이블에서 생성 후 스키마가 변경된 경우 이를 읽으려면, 테이블 생성 시 allow&#95;dynamic&#95;metadata&#95;for&#95;data&#95;lakes = true를 설정하십시오.

<div id="partition-pruning">
  ## 파티션 프루닝
</div>

ClickHouse는 Iceberg 테이블의 SELECT 쿼리에서 파티션 프루닝을 지원하며, 관련 없는 데이터 파일을 스키핑하여 쿼리 성능을 최적화하는 데 도움이 됩니다. 파티션 프루닝을 활성화하려면 `use_iceberg_partition_pruning = 1`로 설정하십시오. Iceberg 파티셔닝에 관한 자세한 내용은 https://iceberg.apache.org/spec/#partitioning 를 참조하십시오.

<div id="time-travel">
  ## 시점 조회
</div>

ClickHouse는 Iceberg 테이블의 시점 조회를 지원하므로, 특정 타임스탬프 또는 스냅샷 ID를 사용해 과거 데이터를 쿼리할 수 있습니다.

<div id="deleted-rows">
  ## 삭제된 행이 포함된 테이블 처리
</div>

ClickHouse는 다음 삭제 방식을 사용하는 Iceberg 테이블 읽기를 지원합니다:

* [위치 삭제(Position deletes)](https://iceberg.apache.org/spec/#position-delete-files)
* [동등성 삭제(Equality deletes)](https://iceberg.apache.org/spec/#equality-delete-files) (버전 25.8+부터 지원)

다음 삭제 방식은 **지원되지 않습니다**:

* [삭제 벡터(Deletion vectors)](https://iceberg.apache.org/spec/#deletion-vectors) (v3에 도입됨)

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

참고: 하나의 쿼리에서 `iceberg_timestamp_ms`와 `iceberg_snapshot_id` 매개변수를 동시에 지정할 수는 없습니다.

<div id="important-considerations">
  ### 중요하게 고려할 사항
</div>

* **스냅샷**은 일반적으로 다음과 같은 경우에 생성됩니다:
  * 새 데이터가 테이블(table)에 기록될 때
  * 어떤 형태로든 데이터 compaction이 수행될 때

* **스키마 변경은 일반적으로 스냅샷을 생성하지 않습니다** - 이로 인해 스키마 진화를 거친 테이블에서 시점 조회를 사용할 때 중요한 동작 차이가 발생합니다.

<div id="example-scenarios">
  ### 예시 시나리오
</div>

CH는 아직 Iceberg 테이블에 쓰기를 지원하지 않으므로, 모든 시나리오는 Spark를 사용해 작성되었습니다.

<div id="scenario-1">
  #### 시나리오 1: 새 스냅샷 없이 발생한 스키마 변경
</div>

다음 작업 시퀀스를 살펴보겠습니다.

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

-- Query the table at each timestamp
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

서로 다른 타임스탬프의 쿼리 결과:

* ts1 및 ts2: 원래 있던 두 컬럼만 표시됩니다
* ts3: 세 컬럼이 모두 표시되며, 첫 번째 행의 price는 NULL로 표시됩니다

<div id="scenario-2">
  #### 시나리오 2: 과거 스키마와 현재 스키마의 차이
</div>

현재 시점에 실행한 시점 조회 쿼리에서는 현재 테이블과 다른 스키마가 표시될 수 있습니다:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number int, 
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

이는 `ALTER TABLE`이 새 스냅샷을 생성하지 않기 때문입니다. 현재 테이블에서는 Spark가 스냅샷이 아니라 최신 메타데이터 파일에서 `schema_id` 값을 가져옵니다.

<div id="scenario-3">
  #### 시나리오 3: 과거 스키마와 현재 스키마의 차이
</div>

두 번째는 시점 조회를 수행할 때 테이블(table)에 데이터가 기록되기 전 상태는 조회할 수 없다는 점입니다:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

ClickHouse에서는 동작 방식이 Spark와 동일합니다. Spark Select 쿼리를 ClickHouse Select 쿼리로 바꿔 생각하면 동일하게 작동합니다.

<div id="metadata-file-resolution">
  ## 메타데이터 파일 결정
</div>

ClickHouse에서 `Iceberg` 테이블 엔진을 사용할 때 시스템은 Iceberg 테이블 구조를 설명하는 올바른 metadata.json 파일을 찾아야 합니다. 이 결정 과정은 다음과 같이 이루어집니다:

<div id="candidate-search">
  ### 후보 검색
</div>

1. **직접 경로 지정**:

* `iceberg_metadata_file_path`를 설정하면 시스템은 이 정확한 경로를 Iceberg 테이블 디렉터리 경로와 결합해 사용합니다.
* 이 설정이 제공되면 다른 모든 확인 설정은 무시됩니다.

2. **테이블 UUID 일치**:

* `iceberg_metadata_table_uuid`가 지정되면 시스템은 다음을 수행합니다:
  * `metadata` 디렉터리의 `.metadata.json` 파일만 확인합니다
  * 지정한 UUID와 일치하는 `table-uuid` 필드가 포함된 파일만 필터링합니다(대소문자 구분 없음)

3. **기본 검색**:

* 위 두 설정이 모두 제공되지 않으면 `metadata` 디렉터리의 모든 `.metadata.json` 파일이 후보가 됩니다

<div id="most-recent-file">
  ### 가장 최근 파일 선택
</div>

위 규칙을 사용해 후보 파일을 식별한 후, 시스템은 그중 가장 최신 파일을 결정합니다.

* `iceberg_recent_metadata_file_by_last_updated_ms_field`가 활성화된 경우:
  * `last-updated-ms` 값이 가장 큰 파일이 선택됩니다

* 그렇지 않은 경우:
  * 버전 번호가 가장 높은 파일이 선택됩니다
  * (버전은 `V.metadata.json` 또는 `V-uuid.metadata.json` 형식의 파일 이름에서 `V`로 나타납니다)

**참고**: 별도로 명시하지 않는 한, 여기서 언급한 모든 설정은 엔진 수준 설정이며 아래와 같이 테이블 생성 시 지정해야 합니다:

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**참고**: 일반적으로 메타데이터 확인은 Iceberg Catalogs가 처리하지만, ClickHouse의 `Iceberg` 테이블 엔진은 S3에 저장된 파일을 Iceberg 테이블로 직접 해석하므로 이러한 확인 규칙을 이해하는 것이 중요합니다.

<div id="data-cache">
  ## 데이터 캐시
</div>

`Iceberg` 테이블 엔진과 테이블 함수는 `S3`, `AzureBlobStorage`, `HDFS` 스토리지와 마찬가지로 데이터 캐시를 지원합니다. 자세한 내용은 [여기](../../../engines/table-engines/integrations/s3.md#data-cache)를 참조하십시오.

<div id="metadata-cache">
  ## 메타데이터 캐시
</div>

`Iceberg` 테이블 엔진과 테이블 함수는 manifest 파일, manifest 목록, metadata JSON 정보를 저장하는 메타데이터 캐시를 지원합니다. 캐시는 메모리에 저장됩니다. 이 기능은 `use_iceberg_metadata_files_cache` 설정으로 제어되며, 기본적으로 활성화되어 있습니다.

<div id="async-metadata-prefetch">
  ## 비동기 메타데이터 프리페치
</div>

비동기 메타데이터 프리페치는 `Iceberg` 테이블(table) 생성 시 `iceberg_metadata_async_prefetch_period_ms`를 설정하여 활성화할 수 있습니다. 이 값을 0(기본값)으로 설정했거나 메타데이터 캐싱이 활성화되어 있지 않으면 비동기 프리페치는 비활성화됩니다.
이 기능을 활성화하려면 0이 아닌 밀리초 값을 지정해야 합니다. 이 값은 프리페치 사이클 간의 인터벌을 나타냅니다.

활성화되면 서버는 원격 카탈로그를 조회하고 새 메타데이터 버전을 감지하기 위한 반복적인 백그라운드 작업을 실행합니다. 그런 다음 이를 파싱하고 스냅샷을 재귀적으로 순회하면서 활성 manifest 목록 및 manifest 파일을 가져옵니다.
이미 메타데이터 캐시에 있는 파일은 다시 다운로드되지 않습니다. 각 프리페치 사이클이 끝나면 최신 메타데이터 스냅샷을 메타데이터 캐시에서 사용할 수 있습니다.

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS
    iceberg_metadata_async_prefetch_period_ms = 60000;
```

읽기 작업에서 비동기 메타데이터 프리페치를 최대한 활용하려면 `iceberg_metadata_staleness_ms` 매개변수를 쿼리 또는 세션 매개변수로 지정해야 합니다. 기본값(0 - 지정되지 않음)에서는 각 쿼리마다 서버가 원격 카탈로그에서 최신 메타데이터를 가져옵니다.
메타데이터 staleness 허용 범위를 지정하면 서버는 원격 카탈로그를 호출하지 않고도 메타데이터 스냅샷의 캐시된 버전을 사용할 수 있습니다. 캐시에 메타데이터 버전이 있고, 지정된 staleness 윈도우 내에 다운로드된 경우에는 해당 버전을 사용해 쿼리를 처리합니다.
그렇지 않으면 원격 카탈로그에서 최신 버전을 가져옵니다.

```sql
SELECT count() FROM icebench_table WHERE ...
SETTINGS iceberg_metadata_staleness_ms=120000
```

**참고**: 비동기 메타데이터 프리페치는 활성 `Iceberg` 테이블의 백그라운드 작업을 위한 서버 측 스레드 풀인 `ICEBERG_SCEDULE_POOL`에서 실행됩니다. 이 스레드 풀의 크기는 서버 구성 매개변수인 `iceberg_background_schedule_pool_size`로 제어되며, 기본값은 10입니다.

**참고**: 비동기 프리페치가 활성화된 경우, 현재는 메타데이터 캐시 크기가 모든 활성 테이블의 최신 메타데이터 스냅샷 전체를 저장할 만큼 충분하다는 것을 전제로 합니다.

<div id="see-also">
  ## 관련 항목
</div>

* [Iceberg 테이블 함수](/ko/sql-reference/table-functions/iceberg.md)