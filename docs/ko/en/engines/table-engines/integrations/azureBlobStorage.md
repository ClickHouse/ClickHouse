---
description: '이 엔진은 Azure Blob Storage 에코시스템과 통합됩니다.'
sidebar_label: 'Azure Blob Storage'
sidebar_position: 10
slug: /engines/table-engines/integrations/azureBlobStorage
title: 'AzureBlobStorage 테이블 엔진'
doc_type: '참고'
---

이 엔진은 [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) 에코시스템과 통합됩니다.

<div id="create-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE azure_blob_storage_table (name String, value UInt32)
    ENGINE = AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, partition_strategy, partition_columns_in_data_file, extra_credentials(client_id=, tenant_id=)])
    [PARTITION BY expr]
    [SETTINGS ...]
```

<div id="engine-parameters">
  ### 엔진 매개변수
</div>

* `endpoint` — container 및 prefix가 포함된 AzureBlobStorage endpoint URL입니다. 사용하는 인증 메서드에 따라 필요한 경우 선택적으로 account&#95;name을 포함할 수 있습니다. (`http://azurite1:{port}/[account_name]{container_name}/{data_prefix}`) 또는 storage&#95;account&#95;url, account&#95;name 및 container를 사용해 이러한 매개변수를 별도로 제공할 수도 있습니다. prefix를 지정하려면 endpoint를 사용해야 합니다.
* `endpoint_contains_account_name` - 이 플래그는 endpoint에 account&#95;name이 포함되는지 지정하는 데 사용되며, account&#95;name은 특정 인증 메서드에서만 필요합니다. (기본값: true)
* `connection_string|storage_account_url` — connection&#95;string에는 account name 및 key가 포함됩니다([연결 문자열 만들기](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)). 또는 여기에서 storage account url을 제공하고, account name 및 account key를 별도 매개변수로 제공할 수도 있습니다(account&#95;name 및 account&#95;key 매개변수 참조).
* `container_name` - 컨테이너 이름
* `blobpath` - 파일 경로입니다. 읽기 전용 모드에서 다음 와일드카드를 지원합니다: `*`, `**`, `?`, `{abc,def}` 및 `{N..M}`. 여기서 `N`, `M`은 숫자이고, `'abc'`, `'def'`는 문자열입니다.
* `account_name` - storage&#95;account&#95;url을 사용하는 경우 여기에서 account name을 지정할 수 있습니다
* `account_key` - storage&#95;account&#95;url을 사용하는 경우 여기에서 account key를 지정할 수 있습니다
* `format` — 파일의 [포맷](/ko/interfaces/formats.md)입니다.
* `compression` — 지원되는 값: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. 기본적으로 파일 확장자를 기준으로 압축을 자동 감지합니다. (`auto`로 설정한 경우와 동일합니다).
* `partition_strategy` – 옵션: `wildcard` 또는 `hive`. `wildcard`는 경로에 `{_partition_id}`가 있어야 하며, 이 값은 파티션 키로 대체됩니다. `hive`는 와일드카드를 허용하지 않으며, 경로를 테이블 루트로 가정하고, 파일 이름으로 Snowflake ID를 사용하며 파일 확장자로 파일 포맷을 사용하는 Hive 스타일 파티션 디렉터리를 생성합니다. 기본값은 `file_like_engine_default_partition_strategy` 설정입니다(`26.6`보다 이전 `compatibility` 설정에서는 `wildcard`, 그 외에는 `hive`).
* `partition_columns_in_data_file` - `hive` 파티션 전략에서만 사용됩니다. 데이터 파일에 파티션 컬럼이 기록되어 있을 것으로 예상할지 여부를 ClickHouse에 지정합니다. 기본값은 `false`입니다.
* `extra_credentials` - 인증에 `client_id` 및 `tenant_id`를 사용합니다. extra&#95;credentials가 제공되면 `account_name` 및 `account_key`보다 우선 적용됩니다.

**예시**

로컬 Azure Storage 개발에는 Azurite 에뮬레이터를 사용할 수 있습니다. 자세한 내용은 [여기](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage)에서 확인하십시오. Azurite의 로컬 instance를 사용하는 경우, 아래 명령에서는 `http://azurite1:10000` 대신 `http://localhost:10000`을 사용해야 할 수 있습니다. 여기서는 Azurite가 host `azurite1`에서 사용 가능하다고 가정합니다.

```sql
CREATE TABLE test_table (key UInt64, data String)
    ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV');

INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT * FROM test_table;
```

```text
┌─key──┬─data──┐
│  1   │   a   │
│  2   │   b   │
│  3   │   c   │
└──────┴───────┘
```

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — 파일 경로. 유형: `LowCardinality(String)`.
* `_file` — 파일 이름. 유형: `LowCardinality(String)`.
* `_size` — 파일 크기(바이트). 유형: `Nullable(UInt64)`. 크기를 알 수 없으면 값은 `NULL`입니다.
* `_time` — 파일의 최종 수정 시간. 유형: `Nullable(DateTime)`. 시간을 알 수 없으면 값은 `NULL`입니다.

<div id="authentication">
  ## 인증
</div>

현재 인증 방법은 3가지입니다.

* `Managed Identity` - `endpoint`, `connection_string` 또는 `storage_account_url`을 제공하면 사용할 수 있습니다.
* `SAS 토큰` - `endpoint`, `connection_string` 또는 `storage_account_url`을 제공하면 사용할 수 있습니다. URL에 `?`가 있으면 이를 통해 식별됩니다. 예시는 [azureBlobStorage](/ko/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens)를 참조하십시오.
* `Workload Identity` - `endpoint` 또는 `storage_account_url`을 제공하면 사용할 수 있습니다. 구성에서 `use_workload_identity` 매개변수가 설정되어 있으면 인증에 [Workload Identity](https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/identity/azure-identity#authenticate-azure-hosted-applications)를 사용합니다.

<div id="data-cache">
  ### 데이터 캐시
</div>

`Azure` 테이블 엔진은 로컬 디스크의 데이터 캐시를 지원합니다.
파일 시스템 캐시 구성 옵션과 사용 방법은 이 [섹션](/ko/operations/storing-data.md/#using-local-cache)을 참조하십시오.
캐싱은 스토리지 객체의 경로와 ETag를 기준으로 이루어지므로, ClickHouse는 오래된 캐시 버전을 읽지 않습니다.

캐시를 활성화하려면 `filesystem_cache_name = '<name>'` 및 `enable_filesystem_cache = 1` 설정을 사용하십시오.

```sql
SELECT *
FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV')
SETTINGS filesystem_cache_name = 'cache_for_azure', enable_filesystem_cache = 1;
```

1. ClickHouse 설정 파일에 다음 섹션을 추가하십시오:

```xml
<clickhouse>
    <filesystem_caches>
        <cache_for_azure>
            <path>path to cache directory</path>
            <max_size>10Gi</max_size>
        </cache_for_azure>
    </filesystem_caches>
</clickhouse>
```

2. ClickHouse `storage_configuration` 섹션의 캐시 구성(따라서 캐시 저장소도)을 재사용합니다. [여기에서 설명한](/ko/operations/storing-data.md/#using-local-cache)

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — 선택 사항입니다. 대부분의 경우 파티션 키는 필요하지 않으며, 필요하더라도 대개 월 단위보다 더 세분화된 파티션 키는 필요하지 않습니다. 파티셔닝은 쿼리 속도를 높이지 않습니다(`ORDER BY` 표현식과 달리). 지나치게 세분화된 파티셔닝은 절대 사용하지 마십시오. 클라이언트 식별자나 이름을 기준으로 데이터를 파티셔닝하지 마십시오(대신 클라이언트 식별자나 이름을 `ORDER BY` 표현식의 첫 번째 컬럼으로 지정하십시오).

월별로 파티셔닝하려면 `toYYYYMM(date_column)` 표현식을 사용하십시오. 여기서 `date_column`은 [Date](/ko/sql-reference/data-types/date.md) 타입의 날짜 컬럼입니다. 이 경우 파티션 이름의 포맷은 `"YYYYMM"`입니다.

<div id="partition-strategy">
  #### 파티션 전략
</div>

`wildcard`: 파일 경로의 `{_partition_id}` 와일드카드를 실제 파티션 키로 대체합니다. 읽기는 지원되지 않습니다. `26.6`보다 오래된 `compatibility` 설정에서만 기본값으로 선택되며, 그 외에는 `hive`가 기본값입니다(`file_like_engine_default_partition_strategy` 설정 참조).

`hive`는 읽기와 쓰기 모두에 Hive 스타일 파티셔닝을 사용합니다. 읽기는 재귀적 글롭 패턴을 사용해 구현됩니다. 쓰기 시에는 다음 포맷으로 파일을 생성합니다: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

참고: `hive` 파티션 전략을 사용할 때 `use_hive_partitioning` 설정은 영향을 미치지 않습니다.

`hive` 파티션 전략의 예시:

```sql
arthur :) create table azure_table (year UInt16, country String, counter UInt8) ENGINE=AzureBlobStorage(account_name='devstoreaccount1', account_key='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', storage_account_url = 'http://localhost:30000/devstoreaccount1', container='cont', blob_path='hive_partitioned', format='Parquet', compression='auto', partition_strategy='hive') PARTITION BY (year, country);

arthur :) insert into azure_table values (2020, 'Russia', 1), (2021, 'Brazil', 2);

arthur :) select _path, * from azure_table;

   ┌─_path──────────────────────────────────────────────────────────────────────┬─year─┬─country─┬─counter─┐
1. │ cont/hive_partitioned/year=2020/country=Russia/7351305360873664512.parquet │ 2020 │ Russia  │       1 │
2. │ cont/hive_partitioned/year=2021/country=Brazil/7351305360894636032.parquet │ 2021 │ Brazil  │       2 │
   └────────────────────────────────────────────────────────────────────────────┴──────┴─────────┴─────────┘
```

<div id="see-also">
  ## 관련 항목
</div>

[Azure Blob Storage 테이블 함수](/ko/sql-reference/table-functions/azureBlobStorage)