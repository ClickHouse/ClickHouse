---
description: 'DataLakeCatalog 데이터베이스 엔진을 사용하면 ClickHouse를 외부 데이터 카탈로그에 연결하고 데이터 복제 없이 개방형 테이블 포맷 데이터를 쿼리할 수 있습니다'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

`DataLakeCatalog` 데이터베이스 엔진을 사용하면 ClickHouse를 외부
데이터 카탈로그에 연결하고 데이터 복제 없이 개방형 테이블 포맷 데이터를 쿼리할 수 있습니다.
이로써 ClickHouse는 기존 데이터 레이크 인프라와
원활하게 연동되는 강력한 쿼리 엔진이 됩니다.

<div id="supported-catalogs">
  ## 지원되는 카탈로그
</div>

`DataLakeCatalog` 엔진은 다음 데이터 카탈로그를 지원합니다.

* **AWS Glue Catalog** - AWS 환경의 Iceberg 테이블에 사용됩니다
* **Databricks Unity Catalog** - Delta Lake 및 Iceberg 테이블에 사용됩니다
* **Hive Metastore** - 전통적인 Hadoop 에코시스템용 카탈로그입니다
* **REST Catalogs** - Iceberg REST 사양을 지원하는 모든 카탈로그입니다

<div id="creating-a-database">
  ## 데이터베이스 생성
</div>

`DataLakeCatalog` 엔진을 사용하려면 아래 관련 설정을 활성화해야 합니다:

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

`DataLakeCatalog` 엔진을 사용하는 데이터베이스는 다음 구문으로 생성할 수 있습니다:

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

다음 설정을 지원합니다:

| Setting                 | Description                                                                                                                                                                                                              |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `catalog_type`          | 카탈로그 유형: `glue`, `unity` (Delta), `rest` (Iceberg), `hive`, `onelake` (Iceberg)                                                                                                                                          |
| `warehouse`             | 카탈로그에서 사용할 warehouse/데이터베이스 이름입니다.                                                                                                                                                                                       |
| `catalog_credential`    | 카탈로그 인증에 사용할 자격 증명입니다(예: API Key 또는 토큰).                                                                                                                                                                                 |
| `auth_header`           | 카탈로그 서비스 인증에 사용할 사용자 지정 HTTP 헤더입니다.                                                                                                                                                                                      |
| `auth_scope`            | 인증에 사용할 OAuth2 scope입니다(OAuth를 사용하는 경우).                                                                                                                                                                                 |
| `storage_endpoint`      | 기본 스토리지의 endpoint URL입니다.                                                                                                                                                                                                |
| `oauth_server_uri`      | 인증에 사용할 OAuth2 authorization server의 URI입니다.                                                                                                                                                                             |
| `vended_credentials`    | 카탈로그에서 제공한 자격 증명을 사용할지 여부를 나타내는 Boolean 값입니다(AWS S3 및 Azure ADLS Gen2 지원).                                                                                                                                               |
| `aws_access_key_id`     | S3/Glue 액세스에 사용할 AWS access key ID입니다(vended credentials를 사용하지 않는 경우).                                                                                                                                                   |
| `aws_secret_access_key` | S3/Glue 액세스에 사용할 AWS secret access key입니다(vended credentials를 사용하지 않는 경우).                                                                                                                                               |
| `region`                | 서비스의 AWS 리전입니다(예: `us-east-1`).                                                                                                                                                                                          |
| `dlf_access_key_id`     | DLF 액세스에 사용할 access key ID입니다.                                                                                                                                                                                           |
| `dlf_access_key_secret` | DLF 액세스에 사용할 access key Secret입니다.                                                                                                                                                                                       |
| `force_add_bucket`      | 카탈로그가 제공한 테이블 위치와 `storage_endpoint`를 사용해 객체 스토리지 URL을 구성할 때, endpoint에 이미 버킷/Container 이름이 포함되어 있더라도 해당 이름을 앞에 추가합니다. 기본값은 `false`입니다. 버킷 없이 경로를 반환하므로 URL 구성 단계에서 버킷을 추가해야 하는 카탈로그(Polaris 스타일 경로)의 경우 `true`로 설정하십시오. |

<div id="examples">
  ## 예시
</div>

`DataLakeCatalog` 엔진 사용 예시는 아래 섹션에서 확인할 수 있습니다:

* [Unity Catalog](/ko/use-cases/data-lake/unity-catalog)
* [Glue Catalog](/ko/use-cases/data-lake/glue-catalog)
* OneLake Catalog
  `allow_experimental_database_iceberg` 또는 `allow_database_iceberg`를 활성화하면 사용할 수 있습니다.

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint)
SETTINGS
    catalog_type = 'onelake',
    warehouse = warehouse,
    onelake_tenant_id = tenant_id,
    oauth_server_uri = server_uri,
    auth_scope = auth_scope,
    onelake_client_id = client_id,
    onelake_client_secret = client_secret;
SHOW TABLES IN database_name;
SELECT count() from database_name.table_name;
```