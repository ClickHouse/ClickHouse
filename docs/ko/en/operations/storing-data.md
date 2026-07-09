---
description: 'highlight-next-line 문서'
sidebar_label: '데이터 저장용 외부 디스크'
sidebar_position: 68
slug: /operations/storing-data
title: '데이터 저장용 외부 디스크'
doc_type: 'guide'
---

ClickHouse에서 처리되는 데이터는 일반적으로 ClickHouse 서버가 실행 중인 머신의
로컬 파일 시스템에 저장됩니다. 이를 위해서는 대용량 디스크가 필요하며,
비용이 많이 들 수 있습니다. 데이터를 로컬에 저장하지 않도록 다양한 스토리지 옵션을 지원합니다:

1. [Amazon S3](https://aws.amazon.com/s3/) 객체 스토리지
2. [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs)
3. 지원되지 않음: Hadoop 분산 파일 시스템([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html))

<br />

:::note
ClickHouse는 외부 테이블 엔진도 지원합니다. 이는 이 페이지에서 설명하는 외부 스토리지 옵션과는 다릅니다. 외부 테이블 엔진을 사용하면
Parquet와 같은 범용 파일 포맷으로 저장된 데이터를 읽을 수 있습니다. 이 페이지에서는
ClickHouse `MergeTree` 계열 또는 `Log` 계열 테이블의 스토리지 구성을 설명합니다.

1. `Amazon S3` 디스크에 저장된 데이터로 작업하려면 [S3](/ko/engines/table-engines/integrations/s3.md) 테이블 엔진을 사용하십시오.
2. Azure Blob Storage에 저장된 데이터로 작업하려면 [AzureBlobStorage](/ko/engines/table-engines/integrations/azureBlobStorage.md) 테이블 엔진을 사용하십시오.
3. Hadoop 분산 파일 시스템에 저장된 데이터로 작업하려면(지원되지 않음) [HDFS](/ko/engines/table-engines/integrations/hdfs.md) 테이블 엔진을 사용하십시오.
   :::

<div id="configuring-external-storage">
  ## 외부 스토리지 구성
</div>

[`MergeTree`](/ko/engines/table-engines/mergetree-family/mergetree.md) 및 [`Log`](/ko/engines/table-engines/log-family/log.md)
계열 테이블 엔진은 각각 `s3`,
`azure_blob_storage`, `hdfs`(지원되지 않음) 타입의 디스크를 사용해 데이터를 `S3`, `AzureBlobStorage`, `HDFS`(지원되지 않음)에 저장할 수 있습니다.

디스크 구성에는 다음이 필요합니다.

1. `type` 섹션. 값은 `s3`, `azure_blob_storage`, `hdfs`(지원되지 않음), `local_blob_storage`, `web` 중 하나여야 합니다.
2. 특정 외부 스토리지 유형에 대한 구성

ClickHouse 버전 24.1부터는 새로운 구성 옵션을 사용할 수 있습니다.
이 옵션을 사용하려면 다음을 지정해야 합니다.

1. 값이 `object_storage`인 `type`
2. `object_storage_type`. 값은 `s3`, `azure_blob_storage`(`24.3`부터는 `azure`만 가능), `hdfs`(지원되지 않음), `local_blob_storage`(`24.3`부터는 `local`만 가능), `web` 중 하나여야 합니다.

<br />

선택적으로 `metadata_type`을 지정할 수 있으며(기본값은 `local`), `plain`, `web`, 그리고 `24.4`부터는 `plain_rewritable`로도 설정할 수 있습니다.
`plain` 메타데이터 유형의 사용법은 [plain storage section](/ko/operations/storing-data#plain-storage)에 설명되어 있습니다. `web` 메타데이터 유형은 `web` 객체 스토리지 유형에서만 사용할 수 있습니다. `local` 메타데이터 유형은 메타데이터 파일을 로컬에 저장하며(각 메타데이터 파일에는 객체 스토리지의 파일에 대한 매핑과 해당 파일의 추가 메타데이터 정보가 포함됩니다).

예시:

```xml
<s3>
    <type>s3</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

는 다음 구성과 동일합니다(`24.1` 버전 기준):

```xml
<s3>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>local</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

다음과 같은 구성:

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

와 같습니다:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

전체 스토리지 구성 예시는 다음과 같습니다:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

버전 24.1부터는 다음과 같이 보일 수도 있습니다:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>local</metadata_type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

모든 `MergeTree` 테이블의 기본 저장소 옵션으로 특정 종류의 저장소를 사용하려면,
다음 섹션을 설정 파일에 추가하십시오:

```xml
<clickhouse>
    <merge_tree>
        <storage_policy>s3</storage_policy>
    </merge_tree>
</clickhouse>
```

특정 테이블에 특정 스토리지 정책을 적용하려면,
테이블을 생성할 때 설정에서 이를 정의할 수 있습니다:

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3';
```

`storage_policy` 대신 `disk`를 사용할 수도 있습니다. 이 경우 설정 파일에 `storage_policy` 섹션이
반드시 있을 필요는 없으며, `disk`
섹션만으로도 충분합니다.

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = 's3';
```

<div id="refresh-parts-interval-and-table-disk">
  ## refresh_parts_interval and table_disk
</div>

이 설정은 파트가 외부에서 작성될 수 있고, 스토리지에서 메타데이터를 다시 탐지해 갱신해야 하는 복제되지 않은 MergeTree 테이블을 위한 것입니다.

MergeTree 설정 `refresh_parts_interval`은 기본 스토리지에서 데이터 파트 목록을 주기적으로 갱신합니다(예: 외부에서 작성된 파트를 인식하기 위해). 여기서 중요한 차이는 **레플리카 간 공유되는 메타데이터**와 **레플리카 로컬 메타데이터**(예: 레플리카마다 로컬 메타데이터를 사용하는 S3)입니다. 메타데이터가 공유될 때만 새 파트가 모든 레플리카에 표시됩니다. 객체 스토리지를 사용한다고 해서 메타데이터까지 공유된다는 뜻은 아닙니다.

* **객체 스토리지(예: `disk = 's3'`)를 사용한다고 해서 메타데이터가 공유되는 것은 아닙니다.** 메타데이터가 레플리카별로 로컬에 저장되는 경우(기본값), 각 레플리카는 객체 스토리지의 blob을 가리키는 포인터를 독립적으로 관리합니다. 한 레플리카에서 발생한 변경은 다른 레플리카에 보이지 않습니다. 이 경우 각 레플리카가 읽는 메타데이터가 레플리카 로컬이므로, `refresh_parts_interval`을 사용해도 레플리카 간에 새 파트가 표시되지 않습니다.

* **자동 파트 갱신이 동작하려면 파일 시스템 메타데이터가 공유되어야 합니다**(또는 테이블이 테이블 소유의 readonly 메타데이터를 사용해 갱신이 적용 가능해야 합니다). `table_disk = true`를 테이블 로컬 디스크와 함께 설정하는 것(예: `SETTINGS disk = disk(type=object_storage, ...), table_disk = true`)은 올바른 동작 방식을 얻는 한 가지 방법입니다. 이 경우 테이블이 메타데이터 수명 주기를 관리하고 스토리지는 readonly로 취급되므로, `refresh_parts_interval`이 실행되어 외부에서 추가된 파트를 탐지할 수 있습니다.

* **전역으로 정의된 디스크**(예: `storage_configuration`의 `disk = 's3'`)와 기본 로컬 메타데이터를 사용하는 경우, 각 레플리카는 자체 메타데이터 상태를 가집니다. blob이 S3에 있더라도 `refresh_parts_interval`의 관점에서는 해당 스토리지가 공유된 것으로 간주되지 않으므로, ClickHouse 외부 또는 다른 레플리카에서 생성된 새 파트는 감지되지 않습니다.

자동 파트 갱신을 사용하려면 메타데이터가 공유되도록 구성하거나, 위와 같이 `table_disk = true`를 사용하는 테이블 수준 디스크를 사용하십시오. 레플리카 로컬 메타데이터와 함께 `refresh_parts_interval`에만 의존하면 예상한 대로 파트가 갱신되지 않습니다.

:::note
`refresh_parts_interval`은 ReplicatedMergeTree 테이블에서는 사용되지 않습니다.
복제된 테이블은 이미 복제 메커니즘을 통해 파트를 동기화합니다.
이 설정은 파트가 외부에서 작성되고 메타데이터 갱신이 필요한 복제되지 않은 MergeTree 테이블에만 적용됩니다.
:::

<div id="dynamic-configuration">
  ## 동적 구성
</div>

설정 파일에서 디스크를 미리 정의하지 않고도 스토리지 구성을 지정할 수 있으며,
`CREATE`/`ATTACH` 쿼리 설정에서 구성할 수도 있습니다.

다음 예시 쿼리는 위의 동적 디스크 구성을 바탕으로,
URL에 저장된 테이블의 데이터를 캐시하기 위해 로컬 디스크를 사용하는 방법을 보여줍니다.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=web,
    endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
  );
  -- highlight-end
```

아래 예시에서는 외부 스토리지에 캐시를 추가합니다.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
-- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
-- highlight-end
```

아래에서 강조 표시된 설정을 보면 `type=web` 디스크가
`type=cache` 디스크 안에 중첩되어 있음을 확인할 수 있습니다.

:::note
이 예시에서는 `type=web`을 사용하지만, 로컬 디스크를 포함한 모든 디스크 유형을 동적으로 구성할 수
있습니다. 로컬 디스크를 사용하려면 path 인수가
server 구성 매개변수 `custom_local_disks_base_directory` 내에 있어야 하며, 이 매개변수에는
기본값이 없으므로 로컬 디스크를 사용할 때는 이 값도 함께 설정해야 합니다.
:::

구성 파일 기반 구성과 SQL로 정의된 구성을 조합하는 것도
가능합니다:

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  -- highlight-end
```

여기서 `web`은 서버 구성 파일에 정의된 항목입니다:

```xml
<storage_configuration>
    <disks>
        <web>
            <type>web</type>
            <endpoint>'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'</endpoint>
        </web>
    </disks>
</storage_configuration>
```

<div id="s3-storage">
  ### S3 Storage 사용하기
</div>

<div id="required-parameters-s3">
  #### 필수 매개변수
</div>

| 매개변수                | 설명                                                                                                                                                     |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `endpoint`          | `path` 또는 `virtual hosted` [방식](https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html)의 S3 엔드포인트 URL입니다. 데이터 저장을 위한 버킷과 루트 경로를 포함해야 합니다. |
| `access_key_id`     | 인증에 사용되는 S3 액세스 키 ID입니다.                                                                                                                               |
| `secret_access_key` | 인증에 사용되는 S3 시크릿 액세스 키입니다.                                                                                                                              |

<div id="optional-parameters-s3">
  #### 선택적 매개변수
</div>

| 매개변수                                                                                                    | 설명                                                                                                                                                                                                                                | 기본값                                      |
| ------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `region`                                                                                                | S3 리전 이름.                                                                                                                                                                                                                         | *                                        |
| `support_batch_delete`                                                                                  | 배치 삭제 지원 여부를 확인할지 설정합니다. Google Cloud Storage (GCS)는 배치 삭제를 지원하지 않으므로, GCS를 사용할 때는 `false`로 설정하십시오.                                                                                                                               | `true`                                   |
| `use_environment_credentials`                                                                           | 환경 변수 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`가 있으면 해당 값에서 AWS 자격 증명을 읽습니다. 참고: 환경 변수의 자격 증명은 모든 S3 디스크에서 공유됩니다. 디스크별로 서로 다른 자격 증명을 사용하려면, 대신 각 디스크에 `access_key_id`와 `secret_access_key`를 명시적으로 지정하십시오. | `false`                                  |
| `use_insecure_imds_request`                                                                             | `true`인 경우, Amazon EC2 metadata에서 자격 증명을 가져올 때 보안되지 않은 IMDS 요청을 사용합니다.                                                                                                                                                            | `false`                                  |
| `expiration_window_seconds`                                                                             | 만료 시점 기준 자격 증명이 만료되었는지 확인할 때 적용되는 유예 기간(초 단위).                                                                                                                                                                                    | `120`                                    |
| `proxy`                                                                                                 | S3 endpoint용 프록시 구성입니다. `proxy` 블록 내부의 각 `uri` 요소에는 프록시 URL을 지정해야 합니다.                                                                                                                                                            | -                                        |
| `connect_timeout_ms`                                                                                    | 소켓 연결 timeout(밀리초 단위).                                                                                                                                                                                                            | `10000` (10초)                            |
| `request_timeout_ms`                                                                                    | 요청 timeout을 밀리초 단위로 지정합니다.                                                                                                                                                                                                        | `5000` (5초)                              |
| `retry_attempts`                                                                                        | 실패한 요청에 대한 retry 시도 횟수입니다.                                                                                                                                                                                                        | `10`                                     |
| `single_read_retries`                                                                                   | 읽기 중 connection이 끊어졌을 때의 retry 시도 횟수입니다.                                                                                                                                                                                          | `4`                                      |
| `min_bytes_for_seek`                                                                                    | 순차 읽기 대신 seek 작업을 사용할 최소 바이트 수입니다.                                                                                                                                                                                                | `1 MB`                                   |
| `metadata_path`                                                                                         | S3 메타데이터 파일을 저장할 로컬 파일 시스템 경로입니다.                                                                                                                                                                                                 | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`                                                                                     | `true`이면 시작 시 디스크 접근 확인을 건너뜁니다.                                                                                                                                                                                                   | `false`                                  |
| `header`                                                                                                | 지정한 HTTP header를 요청에 추가합니다. 여러 번 지정할 수 있습니다.                                                                                                                                                                                      | *                                        |
| `server_side_encryption_customer_key_base64`                                                            | SSE-C로 암호화된 S3 객체에 액세스하는 데 필요한 헤더입니다.                                                                                                                                                                                             | -                                        |
| `server_side_encryption_kms_key_id`                                                                     | [SSE-KMS 암호화](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html)가 적용된 S3 객체에 액세스하는 데 필요한 헤더입니다. 빈 문자열이면 AWS 관리형 S3 키를 사용합니다.                                                                          | *                                        |
| `server_side_encryption_kms_encryption_context`                                                         | SSE-KMS용 암호화 컨텍스트 header(`server_side_encryption_kms_key_id`와 함께 사용).                                                                                                                                                             | -                                        |
| `server_side_encryption_kms_bucket_key_enabled`                                                         | SSE-KMS용 S3 버킷 키를 활성화합니다(`server_side_encryption_kms_key_id`와 함께 사용).                                                                                                                                                             | 버킷 수준의 설정과 일치합니다                         |
| `s3_max_put_rps`                                                                                        | 스로틀링이 적용되기 전 초당 최대 PUT 요청 수입니다.                                                                                                                                                                                                   | `0` (제한 없음)                              |
| `s3_max_put_burst`                                                                                      | RPS 제한에 도달하기 전 최대 동시 PUT 요청 수입니다.                                                                                                                                                                                                 | `s3_max_put_rps`와 동일합니다                  |
| `s3_max_get_rps`                                                                                        | 스로틀링이 적용되기 전 초당 최대 GET 요청 수입니다.                                                                                                                                                                                                   | `0` (제한 없음)                              |
| `s3_max_get_burst`                                                                                      | RPS 제한에 도달하기 전 최대 동시 GET 요청 수입니다.                                                                                                                                                                                                 | `s3_max_get_rps`와 동일                     |
| `read_resource`                                                                                         | [스케줄링](/ko/operations/workload-scheduling.md) 읽기 요청에 사용할 리소스 이름입니다.                                                                                                                                                                  | 빈 문자열(비활성화됨)                             |
| `write_resource`                                                                                        | 쓰기 요청 [스케줄링](/ko/operations/workload-scheduling.md)에 사용할 리소스 이름.                                                                                                                                                                     | 빈 문자열(비활성화됨)                             |
| `key_template`                                                                                          | [re2](https://github.com/google/re2/wiki/Syntax) 구문을 사용해 객체 키 생성 포맷을 정의합니다. `storage_metadata_write_full_object_key` flag가 필요합니다. `endpoint`의 `root path`와는 함께 사용할 수 없습니다. `key_compatibility_prefix`가 필요합니다.                     | *                                        |
| `key_compatibility_prefix`                                                                              | `key_template`와 함께 사용해야 합니다. 이전 메타데이터 버전을 읽을 수 있도록 `endpoint`의 기존 `root path`를 지정합니다.                                                                                                                                             | -                                        |
| `read_only`                                                                                             | 디스크에서 읽기만 허용됩니다.                                                                                                                                                                                                                  | *                                        |
| :::note                                                                                                 |                                                                                                                                                                                                                                   |                                          |
| Google Cloud Storage (GCS)도 `s3` 타입으로 지원됩니다. 자세한 내용은 [GCS backed MergeTree](/ko/integrations/gcs)를 참조하십시오. |                                                                                                                                                                                                                                   |                                          |
| :::                                                                                                     |                                                                                                                                                                                                                                   |                                          |

<div id="plain-storage">
  ### Plain Storage 사용
</div>

`22.10`에서는 한 번만 쓸 수 있는 스토리지를 제공하는 새로운 디스크 유형 `s3_plain`이 도입되었습니다.
이 디스크의 구성 매개변수는 `s3` 디스크 유형과 동일합니다.
`s3` 디스크 유형과 달리 데이터를 있는 그대로 저장합니다. 다시 말해,
무작위로 생성된 blob 이름 대신 일반 파일 이름을 사용하며
(ClickHouse가 로컬 디스크에 파일을 저장하는 방식과 동일), 어떤
메타데이터도 로컬에 저장하지 않습니다. 예를 들어, 메타데이터는 `s3`의 데이터로부터 파생됩니다.

이 디스크 유형은 기존 데이터에 대해 머지를 실행할 수 없고 새 데이터를
삽입할 수도 없으므로, 테이블의 정적 버전을 유지할 수 있습니다. 이 디스크 유형의 사용 사례 중 하나는 여기에 백업을 생성하는 것이며, 이는
`BACKUP TABLE data TO Disk('plain_disk_name', 'backup_name')`를 통해 수행할 수 있습니다. 이후
`RESTORE TABLE data AS data_restored FROM Disk('plain_disk_name', 'backup_name')`를 실행하거나
`ATTACH TABLE data (...) ENGINE = MergeTree() SETTINGS disk = 'plain_disk_name'`를 사용할 수 있습니다.

구성:

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

`24.1`부터는 `plain` 메타데이터 유형을 사용해 모든 객체 스토리지 디스크(`s3`, `azure`, `hdfs` (지원되지 않음), `local`)를 구성할 수 있습니다.

구성:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>azure</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

<div id="s3-plain-rewritable-storage">
  ### S3 Plain Rewritable Storage 사용하기
</div>

새로운 디스크 유형 `s3_plain_rewritable`가 `24.4`에 도입되었습니다.
`s3_plain` 디스크 유형과 마찬가지로 메타데이터 파일용 추가 스토리지가
필요하지 않습니다. 대신 메타데이터는 S3에 저장됩니다.
`s3_plain` 디스크 유형과 달리 `s3_plain_rewritable`는 머지를 수행할 수
있으며 `INSERT` 작업도 지원합니다.
[뮤테이션](/ko/sql-reference/statements/alter#mutations)과 테이블 복제는 지원되지 않습니다.

이 디스크 유형은 복제되지 않은 `MergeTree` 테이블에 적합합니다. `s3` 디스크
유형도 복제되지 않은 `MergeTree` 테이블에 적합하지만, 테이블의 로컬
메타데이터가 필요 없고 제한된 작업만 지원된다는 점을 감수할 수 있다면
`s3_plain_rewritable` 디스크 유형을 선택할 수 있습니다. 예를 들어,
시스템 테이블에 유용할 수 있습니다.

구성:

```xml
<s3_plain_rewritable>
    <type>s3_plain_rewritable</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

와 같습니다

```xml
<s3_plain_rewritable>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain_rewritable</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

`24.5`부터는 `plain_rewritable` 메타데이터 유형을 사용해
모든 객체 스토리지 디스크
(`s3`, `azure`, `local`)를 구성할 수 있습니다.

<div id="azure-blob-storage">
  ### Azure Blob Storage 사용하기
</div>

`MergeTree` 계열 테이블 엔진은 `azure_blob_storage` 유형의 디스크를 사용해 [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/)에 데이터를 저장할 수 있습니다.

구성 예시:

```xml
<storage_configuration>
    ...
    <disks>
        <blob_storage_disk>
            <type>azure_blob_storage</type>
            <storage_account_url>http://account.blob.core.windows.net</storage_account_url>
            <container_name>container</container_name>
            <account_name>account</account_name>
            <account_key>pass123</account_key>
            <metadata_path>/var/lib/clickhouse/disks/blob_storage_disk/</metadata_path>
            <cache_path>/var/lib/clickhouse/disks/blob_storage_disk/cache/</cache_path>
            <skip_access_check>false</skip_access_check>
        </blob_storage_disk>
    </disks>
    ...
</storage_configuration>
```

<div id="azure-blob-storage-connection-parameters">
  #### 연결 매개변수
</div>

| 매개변수                       | 설명                                                                                                                                                    | 기본값                 |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| `storage_account_url` (필수) | Azure Blob Storage 스토리지 계정 URL입니다. 예시: `http://account.blob.core.windows.net` 또는 `http://azurite1:10000/devstoreaccount1`입니다.                         | -                   |
| `container_name`           | 대상 Container 이름입니다.                                                                                                                                   | `default-container` |
| `container_already_exists` | Container 생성 동작을 제어합니다: <br />- `false`: 새 Container를 생성합니다 <br />- `true`: 기존 Container에 직접 연결합니다 <br />- 설정하지 않음: Container가 존재하는지 확인한 후 필요하면 생성합니다 | -                   |

인증 매개변수(디스크는 사용 가능한 모든 메서드와 **및** Managed Identity Credential을 시도합니다):

| 매개변수                | 설명                                                |
| ------------------- | ------------------------------------------------- |
| `connection_string` | connection string을 사용한 인증에 사용합니다.                 |
| `account_name`      | Shared Key를 사용한 인증에 사용합니다(`account_key`와 함께 사용).  |
| `account_key`       | Shared Key를 사용한 인증에 사용합니다(`account_name`와 함께 사용). |

<div id="azure-blob-storage-limit-parameters">
  #### 제한 매개변수
</div>

| 매개변수                                 | 설명                                               |
| ------------------------------------ | ------------------------------------------------ |
| `s3_max_single_part_upload_size`     | Blob Storage에 단일 블록을 업로드할 때의 최대 크기입니다.           |
| `min_bytes_for_seek`                 | 탐색 가능한 영역의 최소 크기입니다.                             |
| `max_single_read_retries`            | Blob Storage에서 데이터 청크를 읽을 때의 최대 재시도 횟수입니다.       |
| `max_single_download_retries`        | Blob Storage에서 읽기 가능한 버퍼를 다운로드할 때의 최대 재시도 횟수입니다. |
| `thread_pool_size`                   | `IDiskRemote` 인스턴스화에 사용할 최대 스레드 수입니다.            |
| `s3_max_inflight_parts_for_one_file` | 단일 객체에 대해 동시에 수행할 수 있는 PUT 요청의 최대 수입니다.          |

<div id="azure-blob-storage-other-parameters">
  #### 기타 매개변수
</div>

| 매개변수                        | Description                                                      | Default Value                            |
| -------------------------------- | ---------------------------------------------------------------- | ---------------------------------------- |
| `metadata_path`                  | Blob Storage의 메타데이터 파일을 저장할 로컬 파일 시스템 경로입니다.                     | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`              | `true`이면 시작 시 디스크 액세스 검사를 건너뜁니다.                                 | `false`                                  |
| `read_resource`                  | [스케줄링](/ko/operations/workload-scheduling.md) 읽기 요청에 사용할 리소스 이름입니다. | 빈 문자열(비활성화)                              |
| `write_resource`                 | [스케줄링](/ko/operations/workload-scheduling.md) 쓰기 요청에 사용할 리소스 이름입니다. | 빈 문자열(비활성화)                              |
| `metadata_keep_free_space_bytes` | 예약해 둘 메타데이터 디스크의 여유 공간 크기입니다.                                    | -                                        |

작동하는 구성 예시는 통합 테스트 디렉터리에서 확인할 수 있습니다(예: [test&#95;merge&#95;tree&#95;azure&#95;blob&#95;storage](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_merge_tree_azure_blob_storage/configs/config.d/storage_conf.xml) 또는 [test&#95;azure&#95;blob&#95;storage&#95;zero&#95;copy&#95;replication](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_azure_blob_storage_zero_copy_replication/configs/config.d/storage_conf.xml)).

:::note Zero-copy 복제는 프로덕션 환경에 사용할 준비가 되어 있지 않습니다
zero-copy 복제는 ClickHouse 버전 22.8 이상에서 기본적으로 비활성화되어 있습니다. 이 기능은 프로덕션 환경에서 사용하는 것을 권장하지 않습니다.
:::

<div id="using-hdfs-storage-unsupported">
  ## HDFS 스토리지 사용(지원되지 않음)
</div>

이 예시 구성에서는 다음과 같습니다.

* 디스크 유형은 `hdfs`입니다(지원되지 않음)
* 데이터는 `hdfs://hdfs1:9000/clickhouse/`에 저장됩니다

참고로 HDFS는 지원되지 않으므로 사용 시 문제가 발생할 수 있습니다. 문제가 발생하면 수정 사항이 포함된 pull request를 보내주십시오.

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <hdfs>
                <type>hdfs</type>
                <endpoint>hdfs://hdfs1:9000/clickhouse/</endpoint>
                <skip_access_check>true</skip_access_check>
            </hdfs>
            <hdd>
                <type>local</type>
                <path>/</path>
            </hdd>
        </disks>
        <policies>
            <hdfs>
                <volumes>
                    <main>
                        <disk>hdfs</disk>
                    </main>
                    <external>
                        <disk>hdd</disk>
                    </external>
                </volumes>
            </hdfs>
        </policies>
    </storage_configuration>
</clickhouse>
```

HDFS는 일부 드문 경우에는 작동하지 않을 수 있다는 점에 유의하십시오.

<div id="encrypted-virtual-file-system">
  ### 데이터 암호화 사용
</div>

[S3](/ko/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3), [HDFS](#using-hdfs-storage-unsupported) (지원되지 않음) 외부 디스크 또는 로컬 디스크에 저장된 데이터를 암호화할 수 있습니다. 암호화 모드를 활성화하려면 설정 파일에서 유형이 `encrypted`인 디스크를 정의하고, 데이터가 저장될 디스크를 선택해야 합니다. `encrypted` 디스크는 기록되는 모든 파일을 실시간으로 암호화하며, `encrypted` 디스크에서 파일을 읽을 때는 자동으로 복호화합니다. 따라서 `encrypted` 디스크도 일반 디스크처럼 사용할 수 있습니다.

디스크 구성 예시:

```xml
<disks>
  <disk1>
    <type>local</type>
    <path>/path1/</path>
  </disk1>
  <disk2>
    <type>encrypted</type>
    <disk>disk1</disk>
    <path>path2/</path>
    <key>_16_ascii_chars_</key>
  </disk2>
</disks>
```

예를 들어, ClickHouse가 어떤 테이블의 데이터를 `disk1`의 `store/all_1_1_0/data.bin` 파일에 기록하면, 이 파일은 실제로 `/path1/store/all_1_1_0/data.bin` 경로의 물리 디스크에 기록됩니다.

같은 파일을 `disk2`에 기록하면, 실제로는 암호화 모드로 `/path1/path2/store/all_1_1_0/data.bin` 경로의 물리 디스크에 기록됩니다.

<div id="required-parameters-encrypted-disk">
  ### 필수 매개변수
</div>

| 매개변수   | 유형     | 설명                                                                                    |
| ------ | ------ | ------------------------------------------------------------------------------------- |
| `type` | String | 암호화된 디스크를 생성하려면 `encrypted`로 설정해야 합니다.                                                |
| `disk` | String | 기반 스토리지에 사용할 디스크의 유형입니다.                                                              |
| `key`  | Uint64 | 암호화 및 복호화에 사용할 키입니다. `key_hex`를 사용해 16진수로 지정할 수 있습니다. `id` 속성을 사용하면 여러 키를 지정할 수 있습니다. |

<div id="optional-parameters-encrypted-disk">
  ### 선택적 매개변수
</div>

| 매개변수             | 유형     | 기본값           | 설명                                                                                                               |
| ---------------- | ------ | ------------- | ---------------------------------------------------------------------------------------------------------------- |
| `path`           | String | 루트 디렉터리       | 데이터가 저장되는 디스크 상의 위치입니다.                                                                                          |
| `current_key_id` | String | -             | 암호화에 사용되는 키 ID입니다. 지정된 모든 키를 복호화에 사용할 수 있습니다.                                                                    |
| `algorithm`      | Enum   | `AES_128_CTR` | 암호화 알고리즘입니다. 옵션: <br />- `AES_128_CTR` (16바이트 키) <br />- `AES_192_CTR` (24바이트 키) <br />- `AES_256_CTR` (32바이트 키) |

디스크 구성 예시:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <disk_s3>
                <type>s3</type>
                <endpoint>...
            </disk_s3>
            <disk_s3_encrypted>
                <type>encrypted</type>
                <disk>disk_s3</disk>
                <algorithm>AES_128_CTR</algorithm>
                <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
                <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
        </disks>
    </storage_configuration>
</clickhouse>
```

<div id="using-local-cache">
  ### 로컬 캐시 사용
</div>

버전 22.3부터 스토리지 구성의 디스크에 로컬 캐시를 구성할 수 있습니다.
버전 22.3~22.7에서는 `s3` 디스크 유형에서만 캐시가 지원됩니다. 버전 &gt;= 22.8에서는 모든 디스크 유형(S3, Azure, Local, Encrypted 등)에서 캐시가 지원됩니다.
버전 &gt;= 23.5에서는 원격 디스크 유형(S3, Azure, HDFS(지원되지 않음))에서만 캐시가 지원됩니다.
캐시는 `LRU` 캐시 정책을 사용합니다.

버전 22.8 이상에 대한 구성 예시:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
            </s3>
            <cache>
                <type>cache</type>
                <disk>s3</disk>
                <path>/s3_cache/</path>
                <max_size>10Gi</max_size>
            </cache>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>cache</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

22.8 이전 버전에 대한 구성 예시:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
                <data_cache_enabled>1</data_cache_enabled>
                <data_cache_max_size>10737418240</data_cache_max_size>
            </s3>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

File Cache **디스크 구성 설정**:

이 설정은 디스크 구성 섹션에 정의해야 합니다.

| 매개변수                             | 유형    | Default    | Description                                                                                                                  |
| ------------------------------------- | ------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `path`                                | String  | -          | **필수**. 캐시를 저장할 디렉터리의 경로입니다.                                                                                                 |
| `max_size`                            | Size    | -          | **필수**. 바이트 단위 또는 사람이 읽기 쉬운 형식(예: `10Gi`)의 최대 캐시 크기입니다. 한도에 도달하면 파일은 LRU 정책에 따라 제거됩니다. `ki`, `Mi`, `Gi` 포맷을 지원합니다(v22.10부터). |
| `cache_on_write_operations`           | Boolean | `false`    | `INSERT` 쿼리 및 백그라운드 머지에 대해 write-through 캐시를 활성화합니다. 쿼리별로 `enable_filesystem_cache_on_write_operations` 설정으로 재정의할 수 있습니다.    |
| `enable_filesystem_query_cache_limit` | Boolean | `false`    | `max_query_cache_size`를 기준으로 쿼리별 캐시 크기 제한을 활성화합니다.                                                                           |
| `enable_cache_hits_threshold`         | Boolean | `false`    | 활성화하면 데이터를 여러 번 읽은 경우에만 캐시합니다.                                                                                               |
| `cache_hits_threshold`                | Integer | `0`        | 데이터가 캐시되기 전에 필요한 읽기 횟수입니다(`enable_cache_hits_threshold` 필요).                                                                 |
| `enable_bypass_cache_with_threshold`  | Boolean | `false`    | 큰 읽기 범위에 대해서는 캐시를 건너뜁니다.                                                                                                     |
| `bypass_cache_threshold`              | Size    | `256Mi`    | 캐시 우회를 트리거하는 읽기 범위 크기입니다(`enable_bypass_cache_with_threshold` 필요).                                                           |
| `max_file_segment_size`               | Size    | `8Mi`      | 바이트 단위 또는 사람이 읽기 쉬운 형식의 단일 캐시 파일 최대 크기입니다.                                                                                   |
| `max_elements`                        | Integer | `10000000` | 캐시 파일의 최대 개수입니다.                                                                                                             |
| `load_metadata_threads`               | Integer | `16`       | 시작 시 캐시 메타데이터를 로드하는 데 사용할 스레드 수입니다.                                                                                          |
| `use_split_cache`                     | Boolean | `false`    | 파일을 시스템용/데이터용으로 분리해 사용합니다.                                                                                                   |
| `split_cache_ratio`                   | Double  | `0.1`      | split&#95;cache 사용 시 전체 캐시 크기에서 시스템 세그먼트가 차지하는 비율입니다.                                                                        |

> **참고**: 크기 값은 `ki`, `Mi`, `Gi` 등의 단위를 지원합니다(예: `10Gi`).

<div id="file-cache-query-profile-settings">
  ## File Cache 쿼리/프로필 설정
</div>

| 설정                                                                      | 유형      | 기본값                     | 설명                                                                                                                                |
| ----------------------------------------------------------------------- | ------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `enable_filesystem_cache`                                               | Boolean | `true`                  | `cache` 디스크 유형을 사용하더라도 쿼리별로 캐시 사용을 활성화하거나 비활성화합니다.                                                                                |
| `read_from_filesystem_cache_if_exists_otherwise_bypass_cache`           | Boolean | `false`                 | 활성화하면 데이터가 있을 때만 캐시를 사용하며, 새 데이터는 캐시되지 않습니다.                                                                                      |
| `enable_filesystem_cache_on_write_operations`                           | Boolean | `false` (Cloud: `true`) | write-through 캐시를 활성화합니다. 캐시 구성에서 `cache_on_write_operations`가 필요합니다.                                                             |
| `enable_filesystem_cache_log`                                           | Boolean | `false`                 | `system.filesystem_cache_log`에 자세한 캐시 사용 로깅을 활성화합니다.                                                                              |
| `filesystem_cache_allow_background_download`                            | Boolean | `true`                  | 부분적으로 다운로드된 세그먼트의 다운로드를 백그라운드에서 완료하도록 허용합니다. 현재 쿼리/세션에서 다운로드를 포그라운드로 유지하려면 비활성화하십시오.                                              |
| `max_query_cache_size`                                                  | Size    | `false`                 | 쿼리별 최대 캐시 크기입니다. 캐시 구성에서 `enable_filesystem_query_cache_limit`가 필요합니다.                                                            |
| `filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit` | Boolean | `true`                  | `max_query_cache_size`에 도달했을 때의 동작을 제어합니다: <br />- `true`: 새 데이터 다운로드를 중지합니다 <br />- `false`: 새 데이터를 위한 공간을 확보하기 위해 기존 데이터를 제거합니다 |

:::warning
캐시 구성 설정과 캐시 쿼리 설정은 최신 ClickHouse 버전에 해당합니다.
이전 버전에서는 일부 기능이 지원되지 않을 수 있습니다.
:::

<div id="cache-system-tables-file-cache">
  #### 캐시 system tables
</div>

| Table Name                    | Description              | Requirements                                |
| ----------------------------- | ------------------------ | ------------------------------------------- |
| `system.filesystem_cache`     | 파일 시스템 캐시의 현재 상태를 표시합니다. | 없음                                          |
| `system.filesystem_cache_log` | 쿼리별 상세 캐시 사용 통계를 제공합니다.  | `enable_filesystem_cache_log = true`가 필요합니다 |

<div id="cache-commands-file-cache">
  #### 캐시 명령어
</div>

<div id="system-clear-filesystem-cache-on-cluster">
  ##### `SYSTEM CLEAR|DROP FILESYSTEM CACHE (<cache_name>) (ON CLUSTER)` -- `ON CLUSTER`
</div>

이 명령은 `<cache_name>`을 지정하지 않은 경우에만 지원됩니다

<div id="show-filesystem-caches">
  ##### `SHOW FILESYSTEM CACHES`
</div>

서버에 구성된 파일 시스템 캐시의 목록을 표시합니다.
(`22.8` 이하 버전에서는 이 명령의 이름이 `SHOW CACHES`였습니다)

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="describe-filesystem-cache">
  ##### `DESCRIBE FILESYSTEM CACHE '<cache_name>'`
</div>

특정 캐시의 구성과 몇 가지 일반적인 통계를 표시합니다.
캐시 이름은 `SHOW FILESYSTEM CACHES` 명령으로 확인할 수 있습니다. (`22.8` 이하
버전에서는 이 명령의 이름이 `DESCRIBE CACHE`였습니다)

```sql title="Query"
DESCRIBE FILESYSTEM CACHE 's3_cache'
```

```text title="Response"
┌────max_size─┬─max_elements─┬─max_file_segment_size─┬─boundary_alignment─┬─cache_on_write_operations─┬─cache_hits_threshold─┬─current_size─┬─current_elements─┬─path───────┬─background_download_threads─┬─enable_bypass_cache_with_threshold─┐
│ 10000000000 │      1048576 │             104857600 │            4194304 │                         1 │                    0 │         3276 │               54 │ /s3_cache/ │                           2 │                                  0 │
└─────────────┴──────────────┴───────────────────────┴────────────────────┴───────────────────────────┴──────────────────────┴──────────────┴──────────────────┴────────────┴─────────────────────────────┴────────────────────────────────────┘
```

| 캐시 관련 현재 메트릭              | 캐시 관련 비동기 메트릭          | 캐시 관련 프로파일 이벤트                                                                            |
| ------------------------- | ---------------------- | ----------------------------------------------------------------------------------------- |
| `FilesystemCacheSize`     | `FilesystemCacheBytes` | `CachedReadBufferReadFromSourceBytes`, `CachedReadBufferReadFromCacheBytes`               |
| `FilesystemCacheElements` | `FilesystemCacheFiles` | `CachedReadBufferReadFromSourceMicroseconds`, `CachedReadBufferReadFromCacheMicroseconds` |
|                           |                        | `CachedReadBufferCacheWriteBytes`, `CachedReadBufferCacheWriteMicroseconds`               |
|                           |                        | `CachedWriteBufferCacheWriteBytes`, `CachedWriteBufferCacheWriteMicroseconds`             |

<div id="web-storage">
  ### 정적 Web 저장소 사용(읽기 전용)
</div>

이 디스크는 읽기 전용입니다. 데이터는 읽기만 가능하며 수정할 수 없습니다. 새 테이블은
`ATTACH TABLE` 쿼리로 이 디스크에 로드됩니다(아래 예시 참조). 로컬 디스크는
실제로 사용되지 않으며, 각 `SELECT` 쿼리는 필요한 데이터를 가져오기 위한 `http` 요청을
발생시킵니다. 테이블 데이터를 수정하려고 하면 모두 예외가 발생하므로,
다음 유형의 쿼리는 허용되지 않습니다: [`CREATE TABLE`](/ko/sql-reference/statements/create/table.md),
[`ALTER TABLE`](/ko/sql-reference/statements/alter/index.md), [`RENAME TABLE`](/ko/sql-reference/statements/rename#rename-table),
[`DETACH TABLE`](/ko/sql-reference/statements/detach.md) 및 [`TRUNCATE TABLE`](/ko/sql-reference/statements/truncate.md).
Web 저장소는 읽기 전용 용도로 사용할 수 있습니다. 예를 들어 샘플
데이터를 호스팅하거나 데이터를 마이그레이션하는 데 활용할 수 있습니다. `clickhouse-static-files-uploader`라는 도구를 사용하면
지정한 테이블의 데이터 디렉터리를 준비할 수 있습니다(`SELECT data_paths FROM system.tables WHERE name = 'table_name'`).
필요한 각 테이블마다 파일이 들어 있는 디렉터리를 얻게 됩니다. 이 파일들은 예를 들어
정적 파일을 제공하는 웹 서버에 업로드할 수 있습니다. 이 준비가 끝나면
`DiskWeb`을 통해 이 테이블을 임의의 ClickHouse 서버에 로드할 수 있습니다.

이 예시 구성에서는:

* 디스크 유형은 `web`입니다
* 데이터는 `http://nginx:80/test1/`에 호스팅됩니다
* 로컬 저장소의 캐시가 사용됩니다

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>http://nginx:80/test1/</endpoint>
            </web>
            <cached_web>
                <type>cache</type>
                <disk>web</disk>
                <path>cached_web_cache/</path>
                <max_size>100000000</max_size>
            </cached_web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
            <cached_web>
                <volumes>
                    <main>
                        <disk>cached_web</disk>
                    </main>
                </volumes>
            </cached_web>
        </policies>
    </storage_configuration>
</clickhouse>
```

:::tip
웹 데이터셋을 정기적으로 사용할 예정이 아니라면, 저장소는 쿼리 내에서 임시로 구성할 수도 있습니다. [동적 구성](#dynamic-configuration)을 참조하고
설정 파일 편집은 건너뛰십시오.

[데모 데이터세트](https://github.com/ClickHouse/web-tables-demo)는 GitHub에서 제공됩니다. 웹
저장소용 자체 테이블을 준비하려면 [clickhouse-static-files-uploader](/ko/operations/utilities/static-files-disk-uploader) 도구를 참조하십시오.
:::

이 `ATTACH TABLE` 쿼리에서 제공된 `UUID`는 데이터의 디렉터리 이름과 일치하며, 엔드포인트는 GitHub raw 콘텐츠의 URL입니다.

```sql
-- highlight-next-line
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      );
  -- highlight-end
```

바로 사용할 수 있는 테스트 예제입니다. 이 구성을 config에 추가해야 합니다:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>https://clickhouse-datasets.s3.yandex.net/disk-with-static-files-tests/test-hits/</endpoint>
            </web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
        </policies>
    </storage_configuration>
</clickhouse>
```

그런 다음 다음 쿼리를 실행하십시오:

```sql
ATTACH TABLE test_hits UUID '1ae36516-d62d-4218-9ae3-6516d62da218'
(
    WatchID UInt64,
    JavaEnable UInt8,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RegionID UInt32,
    UserID UInt64,
    CounterClass Int8,
    OS UInt8,
    UserAgent UInt8,
    URL String,
    Referer String,
    URLDomain String,
    RefererDomain String,
    Refresh UInt8,
    IsRobot UInt8,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    FlashMinor2 String,
    NetMajor UInt8,
    NetMinor UInt8,
    UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    MobilePhone UInt8,
    MobilePhoneModel String,
    Params String,
    IPNetworkID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    IsArtifical UInt8,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,
    SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,
    PageCharset String,
    CodeVersion UInt32,
    IsLink UInt8,
    IsDownload UInt8,
    IsNotBounce UInt8,
    FUniqID UInt64,
    HID UInt32,
    IsOldCounter UInt8,
    IsEvent UInt8,
    IsParameter UInt8,
    DontCountHits UInt8,
    WithHash UInt8,
    HitColor FixedString(1),
    UTCEventTime DateTime,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),
    SocialNetwork String,
    SocialAction String,
    HTTPError UInt16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    RedirectTiming Int32,
    DOMInteractiveTiming Int32,
    DOMContentLoadedTiming Int32,
    DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,
    LoadEventEndTiming Int32,
    NSToDOMContentLoadedTiming Int32,
    FirstPaintTiming Int32,
    RedirectCount Int8,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    GoalsReached Array(UInt32),
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    RefererHash UInt64,
    URLHash UInt64,
    CLID UInt32,
    YCLID UInt64,
    ShareService String,
    ShareURL String,
    ShareTitle String,
    ParsedParams Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    IslandID FixedString(16),
    RequestNum UInt32,
    RequestTry UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy='web';
```

<div id="required-parameters-s3">
  #### 필수 매개변수
</div>

| 매개변수       | 설명                                                                          |
| ---------- | --------------------------------------------------------------------------- |
| `type`     | `web`입니다. 그렇지 않으면 디스크가 생성되지 않습니다.                                           |
| `endpoint` | `path` 형식의 endpoint URL입니다. Endpoint URL에는 업로드된 데이터가 저장되는 루트 경로가 포함되어야 합니다. |

<div id="optional-parameters-s3">
  #### 선택적 매개변수
</div>

| 매개변수                           | Description                      | Default Value   |
| ----------------------------------- | -------------------------------- | --------------- |
| `min_bytes_for_seek`                | 순차 읽기 대신 seek 작업을 사용하는 최소 바이트 수  | `1` MB          |
| `remote_fs_read_backoff_threashold` | 원격 디스크에서 데이터를 읽으려고 할 때의 최대 대기 시간 | `10000` seconds |
| `remote_fs_read_backoff_max_tries`  | 백오프를 적용해 읽기를 재시도하는 최대 횟수         | `5`             |

쿼리가 `DB:Exception Unreachable URL` 예외와 함께 실패하면 다음 설정을 조정해 볼 수 있습니다: [http&#95;connection&#95;timeout](/ko/operations/settings/settings.md/#http_connection_timeout), [http&#95;receive&#95;timeout](/ko/operations/settings/settings.md/#http_receive_timeout), [keep&#95;alive&#95;timeout](/ko/operations/server-configuration-parameters/settings#keep_alive_timeout).

업로드할 파일을 가져오려면 다음을 실행하십시오:
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>` (`--metadata-path`는 쿼리 `SELECT data_paths FROM system.tables WHERE name = 'table_name'`로 확인할 수 있습니다).

`endpoint`로 파일을 로드할 때는 `<endpoint>/store/` 경로에 로드해야 하지만, 구성에는 `endpoint`만 포함되어야 합니다.

서버가 시작되면서 테이블을 로드할 때 디스크 로드 중 URL에 접근할 수 없으면 모든 오류가 처리됩니다. 이 경우 오류가 있었다면 `DETACH TABLE table_name` -&gt; `ATTACH TABLE table_name`을 통해 테이블을 다시 로드해 다시 표시되도록 할 수 있습니다. 서버 시작 시 메타데이터가 성공적으로 로드되었다면 테이블은 즉시 사용할 수 있습니다.

단일 HTTP 읽기 중 최대 재시도 횟수를 제한하려면 [http&#95;max&#95;single&#95;read&#95;retries](/ko/operations/storing-data#web-storage) 설정을 사용하십시오.

<div id="zero-copy">
  ### zero-copy 복제(프로덕션 환경에는 아직 적합하지 않음)
</div>

`S3` 및 `HDFS`(지원되지 않음) 디스크에서 zero-copy 복제를 사용할 수는 있지만, 권장되지는 않습니다. zero-copy 복제는 데이터가 여러 머신의 원격 스토리지에 저장되어 있고 이를 동기화해야 할 때, 데이터 자체는 복제하지 않고 메타데이터(데이터 파트의 경로)만 복제하는 방식을 의미합니다.

:::note zero-copy 복제는 프로덕션 환경에 아직 적합하지 않습니다
ClickHouse 버전 22.8 이상에서는 zero-copy 복제가 기본적으로 비활성화되어 있습니다. 이 기능은 프로덕션 환경에서 사용하지 않는 것이 좋습니다.
:::