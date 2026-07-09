---
description: 'Azure Blob
  Storage에서 파일을 선택하거나 삽입할 수 있는 테이블 형식의 인터페이스를 제공합니다. s3 함수와 유사합니다.'
keywords: ['azure blob storage']
sidebar_label: 'azureBlobStorage'
sidebar_position: 10
slug: /sql-reference/table-functions/azureBlobStorage
title: 'azureBlobStorage'
doc_type: '참고'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="azureblobstorage-table-function">
  # azureBlobStorage 테이블 함수
</div>

[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs)에서 파일을 조회/삽입할 수 있는 테이블과 같은 인터페이스를 제공합니다. 이 테이블 함수는 [s3 함수](../../sql-reference/table-functions/s3.md)와 유사합니다.

<div id="syntax">
  ## 구문
</div>

<Tabs>
  <TabItem value="connection_string" label="연결 문자열" default>
    자격 증명은 연결 문자열에 내장되어 있으므로 `account_name`/`account_key`를 별도로 지정할 필요가 없습니다:

    ```sql
    azureBlobStorage(connection_string, container_name, blobpath [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="storage_account_url" label="스토리지 계정 URL">
    `account_name` 및 `account_key`를 별도 인수로 지정해야 합니다:

    ```sql
    azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="named_collection" label="명명된 컬렉션">
    지원되는 키의 전체 목록은 아래의 [이름이 지정된 컬렉션](#named-collections)을 참조하십시오:

    ```sql
    azureBlobStorage(named_collection[, option=value [,..]])
    ```
  </TabItem>
</Tabs>

<div id="arguments">
  ## 인수
</div>

| 인수                               | 설명                                                                                                                                                                                                                                                                                                                                                                                          |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection_string`              | 자격 증명(계정 이름 + 계정 키 또는 SAS 토큰)이 포함된 연결 문자열입니다. 이 형식을 사용할 때는 `account_name` 및 `account_key`를 별도로 전달하면 **안 됩니다**. [연결 문자열 구성](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)을 참조하십시오. |
| `storage_account_url`            | 스토리지 계정 엔드포인트 URL입니다. 예: `https://myaccount.blob.core.windows.net/`. 이 형식을 사용할 때는 `account_name` 및 `account_key`도 **반드시** 함께 전달해야 합니다.                                                                                                                                                                                                                                                      |
| `container_name`                 | 컨테이너 이름입니다.                                                                                                                                                                                                                                                                                                                                                                                 |
| `blobpath`                       | 파일 경로입니다. 읽기 전용 모드에서는 다음 와일드카드를 지원합니다: `*`, `**`, `?`, `{abc,def}` 및 `{N..M}`. 여기서 `N`, `M`은 숫자이고 `'abc'`, `'def'`는 문자열입니다.                                                                                                                                                                                                                                                                 |
| `account_name`                   | 스토리지 계정 이름입니다. SAS 없이 `storage_account_url`을 사용할 때 **필수**이며, `connection_string`을 사용할 때는 전달하면 **안 됩니다**.                                                                                                                                                                                                                                                                                    |
| `account_key`                    | 스토리지 계정 키입니다. SAS 없이 `storage_account_url`을 사용할 때 **필수**이며, `connection_string`을 사용할 때는 전달하면 **안 됩니다**.                                                                                                                                                                                                                                                                                     |
| `format`                         | 파일의 [포맷](/ko/sql-reference/formats)입니다.                                                                                                                                                                                                                                                                                                                                                        |
| `compression`                    | 지원되는 값: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. 기본적으로 파일 확장자를 기준으로 압축을 자동 감지합니다(`auto`로 설정한 경우와 동일).                                                                                                                                                                                                                                                                         |
| `structure`                      | 테이블 구조입니다. 포맷은 `'column1_name column1_type, column2_name column2_type, ...'`입니다.                                                                                                                                                                                                                                                                                                            |
| `partition_strategy`             | 선택 사항입니다. 지원되는 값: `WILDCARD` 또는 `HIVE`. `WILDCARD`는 경로에 `{_partition_id}`가 있어야 하며, 이 값은 파티션 키로 대체됩니다. `HIVE`는 와일드카드를 허용하지 않고, 경로를 테이블 루트로 간주하며, 파일 이름으로 Snowflake IDs를 사용하고 파일 포맷을 확장자로 하는 Hive 스타일 파티션 디렉터리를 생성합니다. 기본값은 `file_like_engine_default_partition_strategy` 설정입니다(`26.6`보다 이전 `compatibility` 설정에서는 `WILDCARD`, 그 외에는 `HIVE`).                                                  |
| `partition_columns_in_data_file` | 선택 사항입니다. `HIVE` 파티션 전략에서만 사용됩니다. 데이터 파일에 파티션 컬럼이 기록되어 있다고 예상할지 여부를 ClickHouse에 지정합니다. 기본값은 `false`입니다.                                                                                                                                                                                                                                                                                     |
| `extra_credentials`              | 인증에 `client_id` 및 `tenant_id`를 사용합니다. `extra_credentials`가 제공되면 `account_name` 및 `account_key`보다 우선 적용됩니다.                                                                                                                                                                                                                                                                                  |

<div id="named-collections">
  ## 이름이 지정된 컬렉션
</div>

[이름이 지정된 컬렉션](/ko/operations/named-collections)을 사용해 인수를 전달할 수도 있습니다. 이 경우 다음 키를 사용할 수 있습니다:

| Key                   | Required | Description                                                                                    |
| --------------------- | -------- | ---------------------------------------------------------------------------------------------- |
| `container`           | Yes      | 컨테이너 이름입니다. 위치 인수 `container_name`에 해당합니다.                                                |
| `blob_path`           | Yes      | 파일 경로(선택적 와일드카드 포함)입니다. 위치 인수 `blobpath`에 해당합니다.                                               |
| `connection_string`   | No*      | 자격 증명이 포함된 연결 문자열입니다. *`connection_string` 또는 `storage_account_url` 중 하나를 제공해야 합니다. |
| `storage_account_url` | No*      | 스토리지 계정 엔드포인트 URL입니다. *`connection_string` 또는 `storage_account_url` 중 하나를 제공해야 합니다.            |
| `account_name`        | No       | `storage_account_url`을 사용할 때 필요합니다                                                             |
| `account_key`         | No       | `storage_account_url`을 사용할 때 필요합니다                                                             |
| `format`              | No       | 파일 포맷입니다.                                                                                      |
| `compression`         | No       | 압축 형식입니다.                                                                                      |
| `structure`           | No       | 테이블 구조입니다.                                                                                     |
| `client_id`           | No       | 인증에 사용하는 클라이언트 ID입니다.                                                                          |
| `tenant_id`           | No       | 인증에 사용하는 테넌트 ID입니다.                                                                            |

:::note
명명된 컬렉션의 키 이름은 위치 인수 이름과 다릅니다: `container` (`container_name`이 아님) 및 `blob_path` (`blobpath`가 아님).
:::

**예시:**

```sql
CREATE NAMED COLLECTION azure_my_data AS
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'mycontainer',
    blob_path = 'data/*.parquet',
    account_name = 'myaccount',
    account_key = 'mykey...==',
    format = 'Parquet';

SELECT *
FROM azureBlobStorage(azure_my_data)
LIMIT 5;
```

쿼리 시점에 명명된 컬렉션의 값을 재정의할 수도 있습니다:

```sql
SELECT *
FROM azureBlobStorage(azure_my_data, blob_path = 'other_data/*.csv', format = 'CSVWithNames')
LIMIT 5;
```

<div id="returned_value">
  ## 반환 값
</div>

지정된 파일의 데이터를 읽거나 쓰기 위한, 지정된 구조의 테이블입니다.

<div id="examples">
  ## 예시
</div>

<div id="reading-with-storage-account-url">
  ### `storage_account_url` 형식으로 읽기
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'https://myaccount.blob.core.windows.net/',
    'mycontainer',
    'data/*.parquet',
    'myaccount',
    'mykey...==',
    'Parquet'
)
LIMIT 5;
```

<div id="reading-with-connection-string">
  ### `connection_string` 형식을 사용하여 읽기
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'data/*.csv',
    'CSVWithNames'
)
LIMIT 5;
```

<div id="writing-with-partitions">
  ### 파티션별 쓰기
</div>

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_{_partition_id}.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
) PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

그런 다음 특정 파티션을 다시 읽어옵니다:

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_1.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
);
```

```response
┌─column1─┬─column2─┬─column3─┐
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — 파일 경로입니다. 유형: `LowCardinality(String)`.
* `_file` — 파일 이름입니다. 유형: `LowCardinality(String)`.
* `_size` — 파일 크기(바이트)입니다. 유형: `Nullable(UInt64)`. 파일 크기를 알 수 없으면 값은 `NULL`입니다.
* `_time` — 파일의 마지막 수정 시각입니다. 유형: `Nullable(DateTime)`. 시각을 알 수 없으면 값은 `NULL`입니다.

<div id="partitioned-write">
  ## 파티션별 쓰기
</div>

<div id="partition-strategy">
  ### 파티션 전략
</div>

`INSERT` 쿼리에서만 지원됩니다.

`WILDCARD`: 파일 경로의 `{_partition_id}` 와일드카드를 실제 파티션 키로 대체합니다. `26.6`보다 이전의 `compatibility` 설정에서만 기본값으로 선택되며, 그 외에는 기본값이 `HIVE`입니다(`file_like_engine_default_partition_strategy` 설정 참고).

`HIVE`는 읽기와 쓰기 모두에 대해 Hive 스타일 파티셔닝을 구현합니다. 다음 포맷으로 파일을 생성합니다: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

**`HIVE` 파티션 전략 예시**

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root',
    format = 'CSVWithNames',
    compression = 'auto',
    structure = 'year UInt16, country String, id Int32',
    partition_strategy = 'hive'
) PARTITION BY (year, country)
VALUES (2020, 'Russia', 1), (2021, 'Brazil', 2);
```

```result
SELECT _path, * FROM azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root/**.csvwithnames'
)

   ┌─_path───────────────────────────────────────────────────────────────────────────┬─id─┬─year─┬─country─┐
1. │ cont/azure_table_root/year=2021/country=Brazil/7351307847391293440.csvwithnames │  2 │ 2021 │ Brazil  │
2. │ cont/azure_table_root/year=2020/country=Russia/7351307847378710528.csvwithnames │  1 │ 2020 │ Russia  │
   └─────────────────────────────────────────────────────────────────────────────────┴────┴──────┴─────────┘
```

<div id="hive-style-partitioning">
  ## use_hive_partitioning 설정
</div>

이 설정은 읽기 시 ClickHouse가 Hive 스타일로 파티셔닝된 파일을 파싱하도록 하는 힌트입니다. 쓰기에는 영향을 주지 않습니다. 읽기와 쓰기를 대칭적으로 처리하려면 `partition_strategy` 인수를 사용하십시오.

`use_hive_partitioning` 설정을 1로 지정하면 ClickHouse가 경로(`/name=value/`)에서 Hive 스타일 파티셔닝을 감지하고, 쿼리에서 파티션 컬럼을 가상 컬럼으로 사용할 수 있습니다. 이러한 가상 컬럼은 파티셔닝된 경로에 있는 이름과 동일한 이름을 가집니다.

**예시**

Hive 스타일 파티셔닝으로 생성된 가상 컬럼 사용

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Shared Access Signature(SAS) 사용
</div>

Shared Access Signature(SAS)는 Azure Storage 컨테이너 또는 파일에 제한된 액세스 권한을 부여하는 URI입니다. 스토리지 계정 키를 공유하지 않고도 스토리지 계정 리소스에 대해 시간 제한이 있는 액세스를 제공할 때 사용합니다. 자세한 내용은 [여기](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature)를 참조하십시오.

`azureBlobStorage` 함수는 Shared Access Signature(SAS)를 지원합니다.

[Blob SAS 토큰](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers)에는 대상 blob, 권한, 유효 기간을 포함해 요청을 인증하는 데 필요한 모든 정보가 들어 있습니다. blob URL을 구성하려면 blob 서비스 엔드포인트에 SAS 토큰을 추가합니다. 예를 들어 엔드포인트가 `https://clickhousedocstest.blob.core.windows.net/`이면 요청은 다음과 같이 됩니다:

```sql
SELECT count()
FROM azureBlobStorage('BlobEndpoint=https://clickhousedocstest.blob.core.windows.net/;SharedAccessSignature=sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.425 sec.
```

또는 사용자는 생성된 [Blob SAS URL](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers)을 사용할 수 있습니다:

```sql
SELECT count()
FROM azureBlobStorage('https://clickhousedocstest.blob.core.windows.net/?sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.153 sec.
```

<div id="related">
  ## 관련 항목
</div>

* [AzureBlobStorage 테이블 엔진](/ko/engines/table-engines/integrations/azureBlobStorage.md)