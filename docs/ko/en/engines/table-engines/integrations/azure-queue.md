---
description: '이 엔진은 Azure Blob Storage 생태계와 통합되어
  스트리밍 데이터를 가져올 수 있습니다.'
sidebar_label: 'AzureQueue'
sidebar_position: 181
slug: /engines/table-engines/integrations/azure-queue
title: 'AzureQueue 테이블 엔진'
doc_type: 'reference'
---

이 엔진은 [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) 생태계와 통합되어 스트리밍 데이터를 가져올 수 있습니다.

<div id="creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE test (name String, value UInt32)
    ENGINE = AzureQueue(...)
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    ...
```

**엔진 매개변수**

`AzureQueue`의 매개변수는 `AzureBlobStorage` 테이블 엔진에서 지원하는 매개변수와 동일합니다. 매개변수 섹션은 [여기](../../../engines/table-engines/integrations/azureBlobStorage.md)를 참조하십시오.

[AzureBlobStorage](/ko/engines/table-engines/integrations/azureBlobStorage) 테이블 엔진과 마찬가지로 로컬 Azure Storage 개발에는 Azurite 에뮬레이터를 사용할 수 있습니다. 자세한 내용은 [여기](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage)를 참조하십시오.

**예시**

```sql
CREATE TABLE azure_queue_engine_table
(
    `key` UInt64,
    `data` String
)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
SETTINGS mode = 'unordered'
```

<div id="settings">
  ## 설정
</div>

지원되는 설정은 대부분 `S3Queue` 테이블 엔진과 동일하지만, `s3queue_` 접두사는 사용하지 않습니다. [전체 설정 목록](../../../engines/table-engines/integrations/s3queue.md#settings)을 참조하십시오.
테이블에 구성된 설정 목록을 확인하려면 `system.azure_queue_settings` 테이블을 사용하십시오. `24.10`부터 사용할 수 있습니다.

아래는 AzureQueue에서만 지원되며 S3Queue에는 적용되지 않는 설정입니다.

<div id="after_processing_move_connection_string">
  ### `after_processing_move_connection_string`
</div>

대상이 다른 Azure 컨테이너인 경우, 성공적으로 처리된 파일을 이동할 Azure Blob Storage용 연결 문자열입니다.

가능한 값:

* String.

기본값: 빈 문자열입니다.

<div id="after_processing_move_container">
  ### `after_processing_move_container`
</div>

대상이 다른 Azure 컨테이너인 경우, 성공적으로 처리된 파일을 이동할 컨테이너 이름입니다.

가능한 값:

* String.

기본값: 빈 문자열.

예시:

```sql
CREATE TABLE azure_queue_engine_table
(
    `key` UInt64,
    `data` String
)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
SETTINGS
    mode = 'unordered',
    after_processing = 'move',
    after_processing_move_connection_string = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    after_processing_move_container = 'dst-container';
```

<div id="select">
  ## AzureQueue 테이블 엔진에서 SELECT
</div>

AzureQueue 테이블에서는 기본적으로 SELECT 쿼리를 사용할 수 없습니다. 이는 데이터를 한 번 읽은 후 큐에서 제거하는 일반적인 큐 패턴을 따르기 때문입니다. SELECT를 금지하는 이유는 실수로 인한 데이터 손실을 방지하기 위해서입니다.
하지만 경우에 따라서는 유용할 수 있습니다. 이를 사용하려면 `stream_like_engine_allow_direct_select` 설정을 `True`로 지정해야 합니다.
AzureQueue 엔진에는 SELECT 쿼리용 특별 설정인 `commit_on_select`가 있습니다. 읽은 후에도 큐에 데이터를 유지하려면 `False`로, 제거하려면 `True`로 설정하십시오.

<div id="description">
  ## 설명
</div>

`SELECT`는 스트리밍 가져오기에는 그다지 유용하지 않습니다(디버깅 용도는 예외). 각 파일은 한 번만 가져올 수 있기 때문입니다. 대신 [구체화된 뷰(Materialized View)](../../../sql-reference/statements/create/view.md)를 사용해 실시간 스레드를 만드는 것이 더 실용적입니다. 이를 위해 다음을 수행하십시오.

1. 엔진을 사용해 Azure Blob Storage의 지정된 경로에서 데이터를 소비하는 테이블을 만들고, 이를 데이터 스트림으로 간주합니다.
2. 원하는 구조의 테이블을 만듭니다.
3. 엔진의 데이터를 변환해 앞서 만든 테이블에 저장하는 구체화된 뷰를 만듭니다.

`MATERIALIZED VIEW`가 엔진에 연결되면 백그라운드에서 데이터 수집이 시작됩니다.

엔진 인수는 `AzureQueue(connection_string, container_name, blobpath, format[, compression])` 형식입니다.

예시:

```sql
CREATE TABLE azure_queue_engine_table (key UInt64, data String)
  ENGINE=AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
  SETTINGS
      mode = 'unordered';

CREATE TABLE stats (key UInt64, data String)
  ENGINE = MergeTree() ORDER BY key;

CREATE MATERIALIZED VIEW consumer TO stats
  AS SELECT key, data FROM azure_queue_engine_table;

SELECT * FROM stats ORDER BY key;
```

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — 파일 경로입니다.
* `_file` — 파일 이름입니다.

가상 컬럼에 대한 자세한 내용은 [여기](../../../engines/table-engines/index.md#table_engines-virtual_columns)를 참조하십시오.

<div id="introspection">
  ## 내부 검사
</div>

테이블 설정 `enable_logging_to_queue_log=1`을 사용해 해당 테이블의 로깅을 활성화합니다.

내부 검사 기능은 [S3Queue 테이블 엔진](/ko/engines/table-engines/integrations/s3queue#introspection)과 동일하지만, 몇 가지 분명한 차이점이 있습니다:

1. 서버 버전이 &gt;= 25.1이면 큐의 인메모리 상태에 `system.azure_queue_metadata_cache`를 사용합니다. 이전 버전에서는 `system.s3queue_metadata_cache`를 사용합니다(`azure` 테이블의 정보도 포함됨).
2. 예를 들어 기본 ClickHouse 구성을 통해 `system.azure_queue_log`를 활성화합니다.

```xml
  <azure_queue_log>
    <database>system</database>
    <table>azure_queue_log</table>
  </azure_queue_log>
```

이 영속 테이블은 `system.s3queue_metadata_cache`와 동일한 정보를 제공하지만, 처리된 파일과 실패한 파일에 대한 정보입니다.

테이블 구조는 다음과 같습니다:

```sql

CREATE TABLE system.azure_queue_log
(
    `hostname` LowCardinality(String) COMMENT 'Hostname',
    `event_date` Date COMMENT 'Event date of writing this log row',
    `event_time` DateTime COMMENT 'Event time of writing this log row',
    `database` String COMMENT 'The name of a database where current S3Queue table lives.',
    `table` String COMMENT 'The name of S3Queue table.',
    `uuid` String COMMENT 'The UUID of S3Queue table',
    `file_name` String COMMENT 'File name of the processing file',
    `rows_processed` UInt64 COMMENT 'Number of processed rows',
    `status` Enum8('Processed' = 0, 'Failed' = 1) COMMENT 'Status of the processing file',
    `processing_start_time` Nullable(DateTime) COMMENT 'Time of the start of processing the file',
    `processing_end_time` Nullable(DateTime) COMMENT 'Time of the end of processing the file',
    `exception` String COMMENT 'Exception message if happened'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
COMMENT 'Contains logging entries with the information files processes by S3Queue engine.'

```

예시:

```sql
SELECT *
FROM system.azure_queue_log
LIMIT 1
FORMAT Vertical

Row 1:
──────
hostname:              clickhouse
event_date:            2024-12-16
event_time:            2024-12-16 13:42:47
database:              default
table:                 azure_queue_engine_table
uuid:                  1bc52858-00c0-420d-8d03-ac3f189f27c8
file_name:             test_1.csv
rows_processed:        3
status:                Processed
processing_start_time: 2024-12-16 13:42:47
processing_end_time:   2024-12-16 13:42:47
exception:

1 row in set. Elapsed: 0.002 sec.

```