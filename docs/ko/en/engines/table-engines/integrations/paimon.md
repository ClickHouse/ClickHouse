---
description: '이 엔진은 Amazon S3, Azure, HDFS 및 로컬에 저장된 기존 Apache Paimon
  테이블과의 읽기 전용 통합을 제공합니다.'
sidebar_label: 'Paimon'
sidebar_position: 95
slug: /engines/table-engines/integrations/paimon
title: 'Paimon 테이블 엔진'
doc_type: 'reference'
---

이 엔진은 Amazon S3, Azure, HDFS 및 로컬에 저장된 기존 Apache [Paimon](https://paimon.apache.org/) 테이블과의 읽기 전용 통합을 제공합니다.
스냅샷 읽기, 증분 읽기, 그리고 엔진에서 제공하는 기본적인 파티션 프루닝을 지원합니다.

<div id="create-table">
  ## 테이블 생성
</div>

Paimon 테이블은 저장소에 이미 존재해야 하며, 이 명령은 새 테이블을 생성하는 DDL 매개변수를 받지 않습니다.
`Paimon*` 테이블 생성은 `allow_experimental_paimon_storage_engine`으로 제한되며(기본적으로 비활성화됨), `CREATE TABLE`을 실행하기 전에 먼저 이를 활성화해야 합니다.

```sql
SET allow_experimental_paimon_storage_engine = 1;

CREATE TABLE paimon_table_s3
    ENGINE = PaimonS3(url,  [, access_key_id, secret_access_key] [,format] [,compression])

CREATE TABLE paimon_table_azure
    ENGINE = PaimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

CREATE TABLE paimon_table_hdfs
    ENGINE = PaimonHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE paimon_table_local
    ENGINE = PaimonLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## 엔진 인수
</div>

인수 설명은 각각 `S3`, `AzureBlobStorage`, `HDFS`, `File` 엔진의 인수 설명과 동일합니다.
`format`은 Paimon 테이블의 데이터 파일 포맷을 의미합니다.

엔진 매개변수는 [이름이 지정된 컬렉션](../../../operations/named-collections.md)을 사용하여 지정할 수 있습니다.

<div id="example">
  ### 예시
</div>

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

이름이 지정된 컬렉션 사용하기:

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3(paimon_conf, filename = 'test_table')
```

<div id="capabilities">
  ## 기능
</div>

* 최신 테이블 스냅샷에서 데이터를 읽습니다.
* 활성화된 경우 커밋된 스냅샷 ID를 기준으로 증분 읽기를 수행합니다.
* `use_paimon_partition_pruning`이 활성화된 경우 파티션 프루닝을 수행합니다.
* 구성된 경우 백그라운드에서 메타데이터를 선택적으로 갱신합니다.
* Atomic/Replicated 데이터베이스를 사용할 때 테이블 UUID가 안정적으로 유지되므로 Keeper 경로에서 `{uuid}` 매크로를 사용할 수 있습니다.

<div id="settings">
  ## 설정
</div>

이 엔진은 해당 객체 스토리지 엔진과 동일한 설정을 사용하며, Paimon 전용 설정이 추가로 제공됩니다:

* `allow_experimental_paimon_storage_engine` — `Paimon`, `PaimonS3`, `PaimonAzure`, `PaimonHDFS`, `PaimonLocal` 테이블 엔진을 생성할 수 있도록 활성화합니다. 기본값: `0`(비활성화).
* `paimon_incremental_read` — 증분 읽기(incremental read) 모드를 활성화합니다.
* `paimon_metadata_refresh_interval_sec` — 초 단위의 백그라운드 메타데이터 갱신 주기입니다. 0보다 큰 값으로 설정하면 백그라운드 작업이 객체 스토리지에서 최신 스냅샷과 스키마를 주기적으로 가져옵니다. 기본값: 30.
* `paimon_keeper_path` — 증분 읽기 상태를 위한 Keeper 경로입니다. 테이블마다 반드시 설정해야 하며, 각 테이블별로 고유해야 합니다. `{database}`, `{table}`, `{uuid}``와` 같은 매크로를 지원합니다.
* `paimon_replica_name` — 증분 읽기 상태를 위한 레플리카 이름입니다. 레플리카마다 반드시 설정해야 하며, 각 레플리카별로 고유해야 합니다. `{replica}`와 같은 매크로를 지원합니다.

<div id="incremental-read-examples">
  ## 증분 읽기 예시
</div>

Keeper 상태를 이용한 증분 읽기:

```sql
CREATE TABLE paimon_inc
ENGINE = PaimonS3(paimon_conf, filename = 'paimon_all_types')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/{database}/{uuid}',
    paimon_replica_name = '{replica}';
```

<div id="query-level-settings-for-incremental-read">
  ### 증분 읽기를 위한 쿼리 수준 설정
</div>

다음 설정은 **쿼리 수준** 설정입니다(`CREATE TABLE`이 아니라 `SELECT ... SETTINGS`를 통해 전달됨). 이 설정은 증분 읽기의 쿼리별 동작을 제어합니다:

* `paimon_target_snapshot_id` — 지정된 스냅샷의 변경분만 읽습니다. Keeper의 커밋된 워터마크는 앞으로 진행되지 않으므로 동일한 스냅샷을 원하는 만큼 다시 읽을 수 있습니다. 기본값: `-1`(비활성화).
* `max_consume_snapshots` — 단일 증분 읽기에서 처리할 스냅샷의 최대 개수입니다. 소스에 아직 읽지 않은 스냅샷이 많이 누적된 경우, 배치 크기를 제어하기 위해 쿼리당 처리되는 개수를 제한합니다. `0`은 제한이 없음을 의미합니다. 기본값: `0`.

**특정 스냅샷 읽기** — 현재 워터마크와 관계없이 항상 스냅샷 1의 변경분을 반환합니다:

```sql
SELECT count()
FROM paimon_inc
SETTINGS paimon_target_snapshot_id = 1;
```

**배치별 스냅샷 수 제한** — 새 스냅샷 3개가 대기 중이면, 쿼리당 최대 2개까지만 처리합니다:

```sql
SELECT count()
FROM paimon_inc
SETTINGS max_consume_snapshots = 2;
```

<div id="paimon-to-mergetree-via-refresh-mv">
  ## 갱신 가능 구체화 뷰를 통한 Paimon에서 MergeTree로
</div>

`APPEND` 모드의 갱신 가능 구체화 뷰를 사용하면 Paimon 테이블의 데이터를 MergeTree 테이블로 지속적으로 동기화하는 종단 간 파이프라인을 구축할 수 있습니다. 각 갱신 주기마다 Paimon의 새로운 incremental 데이터만 읽어 대상 테이블에 추가합니다.

**1단계 — 증분 읽기 및 메타데이터 갱신을 활성화한 Paimon 원본 테이블을 생성합니다.**

아래 예시에서는 `PaimonLocal`을 사용합니다. 사용하는 스토리지 backend에 맞게 engine을 `PaimonS3`, `PaimonAzure`, `PaimonHDFS` 또는 자동 감지 기능이 있는 `Paimon` engine으로 대체하십시오:

```sql
SET allow_experimental_paimon_storage_engine = 1;

-- Local storage
CREATE TABLE paimon_mv_source
ENGINE = PaimonLocal('/path/to/paimon/table')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;

-- S3 storage (the `Paimon` engine defaults to the S3 implementation when no `disk` is specified)
CREATE TABLE paimon_mv_source
ENGINE = Paimon('http://minio:9000/bucket/path/to/table', 'access_key', 'secret_key')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;
```

`paimon_metadata_refresh_interval_sec`는 백그라운드 메타데이터 갱신 간격을 초 단위로 설정합니다. 0보다 크면 백그라운드 작업이 주기적으로 객체 스토리지에서 최신 스냅샷과 스키마를 가져오므로, MV 갱신 주기가 메타데이터 업데이트를 트리거하는 쿼리를 기다리지 않고도 새로 커밋된 데이터를 볼 수 있습니다. 기본값은 30입니다. 객체 스토리지 및 Keeper I/O가 과도해지지 않도록 여러 테이블에 사용할 때는 주의하십시오.

**2단계 — MergeTree 대상 테이블을 생성합니다(Paimon 테이블에서 스키마 복제):**

```sql
CREATE TABLE paimon_mv_dest AS paimon_mv_source
ENGINE = MergeTree()
ORDER BY tuple();
```

**3단계 — 갱신 가능 구체화 뷰 생성:**

```sql
CREATE MATERIALIZED VIEW paimon_mv
REFRESH EVERY 10 SECOND
APPEND
TO paimon_mv_dest
AS SELECT * FROM paimon_mv_source;
```

10초마다 MV가 `SELECT * FROM paimon_mv_source`를 실행합니다. 이 쿼리는 마지막으로 커밋된 스냅샷 이후 추가된 행만 반환하며, 해당 행을 `paimon_mv_dest`에 추가합니다.

**정리:**

```sql
SYSTEM STOP VIEW paimon_mv;
DROP VIEW IF EXISTS paimon_mv SYNC;
DROP TABLE IF EXISTS paimon_mv_dest SYNC;
DROP TABLE IF EXISTS paimon_mv_source SYNC;
```

:::note
백그라운드 갱신으로 DDL 작업이 차단되지 않도록, 삭제하기 전에 MV를 중지하십시오.
:::

<div id="limitations">
  ## 제한 사항
</div>

* 증분 읽기를 사용하려면 Keeper(ZooKeeper)가 구성되어 있어야 합니다.
* 증분 읽기를 사용하려면 `paimon_keeper_path`를 설정해야 하며, 테이블마다 고유해야 합니다.
* 동일한 Keeper 경로 내에서는 `paimon_replica_name`이 레플리카마다 고유해야 합니다.
* 증분 읽기는 at-most-once 전달 방식을 사용합니다. 커밋된 스냅샷은 데이터가 실제로 소비되기 전에, 데이터 파일을 수집하는 시점에 다음으로 진행됩니다. 파일 수집 후 쿼리가 실패하면 건너뛴 스냅샷은 retry해도 다시 읽지 않습니다.
* 이 테이블 엔진은 읽기 전용이며, 데이터 수정은 지원되지 않습니다.
* 증분 읽기는 Paimon source의 과거 데이터 삭제를 처리하지 않습니다. upstream Paimon 데이터가 삭제되거나 업데이트되더라도 ClickHouse MergeTree 대상 테이블에 이미 기록된 해당 행은 자동으로 제거되지 않습니다. 오래된 데이터를 정리하려면 MergeTree 테이블에 대해 `ALTER TABLE ... DELETE`를 수동으로 실행해야 합니다.

<div id="aliases">
  ## 별칭
</div>

`Paimon` 테이블 엔진은 `disk` 설정을 기준으로 스토리지 백엔드를 자동으로 감지한 뒤, 그에 따라 `PaimonS3`, `PaimonAzure` 또는 `PaimonLocal`을 사용합니다. `disk`를 지정하지 않으면 기본적으로 `PaimonS3` 구현이 사용됩니다.

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — 파일 경로입니다. 유형: `LowCardinality(String)`.
* `_file` — 파일 이름입니다. 유형: `LowCardinality(String)`.
* `_size` — 파일 크기(바이트)입니다. 유형: `Nullable(UInt64)`. 파일 크기를 알 수 없는 경우 값은 `NULL`입니다.
* `_time` — 파일의 마지막 수정 시간입니다. 유형: `Nullable(DateTime)`. 시간을 알 수 없는 경우 값은 `NULL`입니다.
* `_etag` — 파일의 etag입니다. 유형: `LowCardinality(String)`. etag를 알 수 없는 경우 값은 `NULL`입니다.

<div id="data-types-supported">
  ## 지원되는 데이터 타입
</div>

| Paimon 데이터 타입                     | ClickHouse 데이터 타입         |
| --------------------------------- | ------------------------- |
| BOOLEAN                           | Int8                      |
| TINYINT                           | Int8                      |
| SMALLINT                          | Int16                     |
| INTEGER                           | Int32                     |
| BIGINT                            | Int64                     |
| FLOAT                             | Float32                   |
| DOUBLE                            | Float64                   |
| STRING,VARCHAR,BYTES,VARBINARY    | String                    |
| DATE                              | Date                      |
| TIME(p),TIME                      | Time(&#39;UTC&#39;)       |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | DateTime64                |
| TIMESTAMP(p)                      | DateTime64(&#39;UTC&#39;) |
| CHAR                              | FixedString(1)            |
| BINARY(n)                         | FixedString(n)            |
| DECIMAL(P,S)                      | Decimal(P,S)              |
| ARRAY                             | Array                     |
| MAP                               | Map                       |

<div id="partition-supported">
  ## 지원되는 파티션 키
</div>

Paimon 파티션 키에서 지원되는 데이터 타입:

* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`