---
description: '이 엔진은 Amazon S3에 있는 기존 Delta Lake 테이블과의 읽기 전용 통합을 제공합니다.'
sidebar_label: 'DeltaLake'
sidebar_position: 40
slug: /engines/table-engines/integrations/deltalake
title: 'DeltaLake 테이블 엔진'
doc_type: '참고'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="deltalake-table-engine">
  # DeltaLake 테이블 엔진
</div>

이 엔진은 S3, GCP 및 Azure 스토리지에 있는 기존 [Delta Lake](https://github.com/delta-io/delta) 테이블과 통합되며, 읽기와 쓰기를 모두 지원합니다(v25.10부터).

<div id="create-table">
  ## DeltaLake 테이블 생성
</div>

DeltaLake 테이블을 생성하려면 해당 테이블이 이미 S3, GCP 또는 Azure 스토리지에 존재해야 합니다. 아래 명령은 새 테이블을 생성하기 위한 DDL 매개변수를 받지 않습니다.

<Tabs>
  <TabItem value="S3" label="S3" default>
    **구문**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
    ```

    **엔진 매개변수**

    * `url` — 기존 Delta Lake 테이블 경로가 포함된 버킷 URL입니다.
    * `aws_access_key_id`, `aws_secret_access_key` - [AWS](https://aws.amazon.com/) 계정 사용자의 장기 자격 증명입니다. 요청을 인증하는 데 사용할 수 있습니다. 이 매개변수는 선택 사항입니다. 자격 증명을 지정하지 않으면 설정 파일의 값을 사용합니다.
    * `extra_credentials` - 선택 사항입니다. ClickHouse Cloud에서 역할 기반 접근을 위한 `role_arn`을 전달하는 데 사용됩니다. 구성 단계는 [Secure S3](/ko/cloud/data-sources/secure-s3)를 참조하십시오.

    엔진 매개변수는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용해 지정할 수 있습니다.

    **예시**

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
    ```

    이름이 지정된 컬렉션 사용:

    ```xml
    <clickhouse>
        <named_collections>
            <deltalake_conf>
                <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
                <access_key_id>ABC123</access_key_id>
                <secret_access_key>Abc+123</secret_access_key>
            </deltalake_conf>
        </named_collections>
    </clickhouse>
    ```

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake(deltalake_conf, filename = 'test_table')
    ```
  </TabItem>

  <TabItem value="GCP" label="GCP" default>
    **구문**

    ```sql
    -- HTTPS URL 사용(권장)
    CREATE TABLE table_name
    ENGINE = DeltaLake('https://storage.googleapis.com/<bucket>/<path>/', '<access_key_id>', '<secret_access_key>')
    ```

    :::note[지원되지 않는 gsutil URI]
    `gs://clickhouse-docs-example-bucket`와 같은 gsutil URI는 지원되지 않습니다. `https://storage.googleapis.com`으로 시작하는 URL을 사용하십시오.
    :::

    **인수**

    * `url` — Delta Lake 테이블의 GCS 버킷 URL입니다. `https://storage.googleapis.com/<bucket>/<path>/`
      형식(GCS XML API endpoint)을 사용해야 하며, `gs://<bucket>/<path>/` 형식은 자동으로 변환됩니다.
    * `access_key_id` — GCS 액세스 키입니다. Google Cloud Console → Cloud Storage → Settings → Interoperability에서 생성하십시오.
    * `secret_access_key` — GCS 시크릿입니다.

    **이름이 지정된 컬렉션**

    이름이 지정된 컬렉션을 사용할 수도 있습니다.
    예시는 다음과 같습니다.

    ```sql
    CREATE NAMED COLLECTION gcs_creds AS
    access_key_id = '<access_key>',
    secret_access_key = '<secret>';

    CREATE TABLE gcpDeltaLake
    ENGINE = DeltaLake(gcs_creds, url = 'https://storage.googleapis.com/<bucket>/<path>')
    ```
  </TabItem>

  <TabItem value="Azure" label="Azure" default>
    **구문**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    ```

    **인수**

    * `connection_string` — Azure 연결 문자열
    * `storage_account_url` — Azure 스토리지 계정 URL(예: https://account.blob.core.windows.net)
    * `container_name` — Azure 컨테이너 이름
    * `blobpath` — 컨테이너 내 Delta Lake 테이블 경로
    * `account_name` — Azure 스토리지 계정 이름
    * `account_key` — Azure 스토리지 계정 키
  </TabItem>
</Tabs>

<div id="insert-data">
  ## DeltaLake 테이블로 데이터 쓰기
</div>

DeltaLake 테이블 엔진으로 테이블을 생성한 후에는 다음과 같이 데이터를 삽입할 수 있습니다.

```sql
SET allow_delta_lake_writes = 1;

INSERT INTO deltalake(id, firstname, lastname, gender, age)
VALUES (1, 'John', 'Smith', 'M', 32);
```

:::note
테이블 엔진을 사용한 쓰기는 delta kernel을 통해서만 지원됩니다.
Azure에 대한 쓰기는 아직 지원되지 않지만, S3 및 GCS에 대해서는 작동합니다.

Delta Lake 쓰기는 베타 기능이며 `SET allow_delta_lake_writes = 1`로 활성화해야 합니다(26.7 버전부터 사용 가능하며, 그 이전 버전에서는 `SET allow_experimental_delta_lake_writes = 1`을 사용하십시오).
:::

<div id="data-cache">
  ### 데이터 캐시
</div>

`DeltaLake` 테이블 엔진과 테이블 함수는 `S3`, `AzureBlobStorage`, `HDFS` 스토리지와 마찬가지로 데이터 캐싱을 지원합니다. 자세한 내용은 [&quot;S3 테이블 엔진&quot;](../../../engines/table-engines/integrations/s3.md#data-cache)을 참조하십시오.

<div id="see-also">
  ## 관련 항목
</div>

* [deltaLake 테이블 함수](../../../sql-reference/table-functions/deltalake.md)