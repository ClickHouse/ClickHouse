---
description: 'Google
  Cloud Storage의 데이터를 `SELECT` 및 `INSERT`할 수 있는 테이블 형태의 인터페이스를 제공합니다.
  `Storage Object User` IAM 역할이 필요합니다.'
keywords: ['gcs', '버킷']
sidebar_label: 'gcs'
sidebar_position: 70
slug: /sql-reference/table-functions/gcs
title: 'gcs'
doc_type: 'reference'
---

[Google Cloud Storage](https://cloud.google.com/storage/)의 데이터를 `SELECT` 및 `INSERT`할 수 있는 테이블 형태의 인터페이스를 제공합니다. [`Storage Object User` IAM 역할](https://cloud.google.com/storage/docs/access-control/iam-roles)이 필요합니다.

이 함수는 [S3 테이블 함수](../../sql-reference/table-functions/s3.md)의 별칭입니다.

클러스터에 여러 레플리카가 있는 경우, 삽입을 병렬화하기 위해 [s3Cluster function](../../sql-reference/table-functions/s3Cluster.md)(GCS와 함께 작동함)을 대신 사용할 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
gcs(url [, NOSIGN | hmac_key, hmac_secret] [,format] [,structure] [,compression_method])
gcs(named_collection[, option=value [,..]])
```

:::tip GCS
GCS 테이블 함수는 GCS XML API와 HMAC 키를 사용해 Google Cloud Storage와 연동됩니다.
엔드포인트와 HMAC에 대한 자세한 내용은 [Google 상호운용성 문서](https://cloud.google.com/storage/docs/interoperability)를 참조하십시오.
:::

<div id="arguments">
  ## 인수
</div>

| 인수                           | 설명                                                                                                                                          |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                        | 파일의 버킷 경로입니다. `readonly` 모드에서는 `*`, `**`, `?`, `{abc,def}`, `{N..M}` 와일드카드를 지원합니다. 여기서 `N`, `M`은 숫자이고 `'abc'`, `'def'`는 문자열입니다.             |
| `NOSIGN`                     | 이 키워드를 자격 증명 대신 지정하면 모든 요청에 서명하지 않습니다.                                                                                                      |
| `hmac_key` and `hmac_secret` | 지정한 엔드포인트에 사용할 자격 증명을 지정하는 키입니다. 선택 사항입니다.                                                                                                  |
| `format`                     | 파일의 [포맷](/ko/sql-reference/formats)입니다.                                                                                                        |
| `structure`                  | 테이블 구조입니다. 포맷은 `'column1_name column1_type, column2_name column2_type, ...'`입니다.                                                            |
| `compression_method`         | 이 매개변수는 선택 사항입니다. 지원되는 값은 `none`, `gzip` 또는 `gz`, `brotli` 또는 `br`, `xz` 또는 `LZMA`, `zstd` 또는 `zst`입니다. 기본적으로 파일 확장자를 기준으로 압축 방식을 자동 감지합니다. |

:::note GCS
Google XML API의 엔드포인트가 JSON API와 다르므로 GCS 경로는 다음 형식을 사용합니다:

```text
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
```

이고 ~~https://storage.cloud.google.com~~는 아닙니다.
:::

인수는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용해서도 전달할 수 있습니다. 이 경우 `url`, `format`, `structure`, `compression_method`는 동일하게 동작하며, 몇 가지 추가 매개변수도 지원됩니다.

| 매개변수                          | 설명                                                                                                                                                                                                |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `access_key_id`               | `hmac_key`, 선택 사항입니다.                                                                                                                                                                             |
| `secret_access_key`           | `hmac_secret`, 선택 사항입니다.                                                                                                                                                                          |
| `filename`                    | 지정하면 URL에 추가됩니다.                                                                                                                                                                                  |
| `use_environment_credentials` | 기본적으로 활성화되어 있으며, 환경 변수 `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`를 사용해 추가 매개변수를 전달할 수 있습니다. |
| `no_sign_request`             | 기본적으로 비활성화되어 있습니다.                                                                                                                                                                                |
| `expiration_window_seconds`   | 기본값은 120입니다.                                                                                                                                                                                      |

<div id="returned_value">
  ## 반환 값
</div>

지정된 파일에서 데이터를 읽거나 쓰는 데 사용되는, 지정된 구조의 테이블(table)입니다.

<div id="examples">
  ## 예시
</div>

GCS 파일 `https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz`에서 처음 2개의 행을 선택합니다. 압축 방식은 `.gz` 파일 확장자를 기준으로 자동 감지됩니다.

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

위와 동일한 쿼리이지만, 자동 감지에 의존하지 않고 `gzip` 압축 방식을 명시적으로 지정했습니다:

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32', 'gzip')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="usage">
  ## 사용 예시
</div>

GCS에 다음 URI를 가진 여러 파일이 있다고 가정하겠습니다.

* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;4.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;4.csv&#39;

파일명 끝이 1부터 3까지의 숫자인 파일의 행 수를 계산합니다.

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      18 │
└─────────┘
```

이 두 디렉터리의 모든 파일에 있는 총 행 수를 계산합니다:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      24 │
└─────────┘
```

:::warning
파일 목록에 앞에 0이 붙은 숫자 범위가 포함된 경우, 각 자릿수별로 중괄호를 사용하는 구문이나 `?`를 사용하십시오.
:::

`file-000.csv`, `file-001.csv`, ... , `file-999.csv`라는 이름의 파일에 있는 전체 행 수를 계산합니다:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'name String, value UInt32');
```

```text
┌─count()─┐
│      12 │
└─────────┘
```

파일 `test-data.csv.gz`에 데이터를 삽입합니다:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

기존 테이블의 데이터를 파일 `test-data.csv.gz`에 삽입합니다:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

글롭 **를 사용하면 디렉터리를 재귀적으로 탐색할 수 있습니다. 아래 예시에서는 `my-test-bucket-768` 디렉터리 아래의 모든 파일을 재귀적으로 가져옵니다:

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

다음은 `my-test-bucket` 버킷 내의 모든 폴더에 있는 `test-data.csv.gz` 파일에서 재귀적으로 데이터를 가져옵니다:

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

프로덕션 환경에서는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md) 사용을 권장합니다. 예시는 다음과 같습니다:

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = '***',
        secret_access_key = '***';
SELECT count(*)
FROM gcs(creds, url='https://s3-object-url.csv')
```

<div id="partitioned-write">
  ## 파티션별 쓰기
</div>

`GCS` 테이블에 데이터를 삽입할 때 `PARTITION BY` 표현식을 지정하면 각 파티션 값마다 별도의 파일이 생성됩니다. 데이터를 여러 파일로 나누면 읽기 작업의 효율을 높이는 데 도움이 됩니다.

**예시**

1. 키에 파티션 ID를 사용하면 별도의 파일이 생성됩니다:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```

그 결과 데이터는 `file_x.csv`, `file_y.csv`, `file_z.csv`의 세 파일에 기록됩니다.

2. 버킷 이름에 partition ID를 사용하면 서로 다른 버킷에 파일이 생성됩니다:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```

그 결과, 데이터는 서로 다른 버킷의 3개 파일인 `my_bucket_1/file.csv`, `my_bucket_10/file.csv`, `my_bucket_20/file.csv`에 기록됩니다.

<div id="related">
  ## 관련 항목
</div>

* [S3 테이블 함수](s3.md)
* [S3 엔진](../../engines/table-engines/integrations/s3.md)