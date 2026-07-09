---
description: 'Amazon
  S3의 Apache Hudi 테이블에 대해 읽기 전용 테이블 형식 인터페이스를 제공합니다.'
sidebar_label: 'hudi'
sidebar_position: 85
slug: /sql-reference/table-functions/hudi
title: 'hudi'
doc_type: 'reference'
---

Amazon S3의 Apache [Hudi](https://hudi.apache.org/) 테이블에 대해 읽기 전용 테이블 형식 인터페이스를 제공합니다.

<div id="syntax">
  ## 구문
</div>

```sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## 인수
</div>

| 인수                                           | 설명                                                                                                                                                                                                                                                                      |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                                        | S3에 있는 기존 Hudi 테이블 경로를 포함한 버킷 URL입니다.                                                                                                                                                                                                                                   |
| `aws_access_key_id`, `aws_secret_access_key` | [AWS](https://aws.amazon.com/) 계정 사용자의 장기 자격 증명입니다. 이를 사용해 요청을 인증할 수 있습니다. 이 매개변수는 선택 사항입니다. 자격 증명을 지정하지 않으면 ClickHouse 구성에 설정된 값을 사용합니다. 자세한 내용은 [Using S3 for Data Storage](/ko/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)를 참조하십시오. |
| `format`                                     | 파일의 [포맷](/ko/interfaces/formats)입니다.                                                                                                                                                                                                                                       |
| `structure`                                  | 테이블의 구조입니다. 포맷은 `'column1_name column1_type, column2_name column2_type, ...'`입니다.                                                                                                                                                                                       |
| `compression`                                | 이 매개변수는 선택 사항입니다. 지원되는 값은 `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`입니다. 기본적으로 파일 확장자를 기준으로 압축이 자동 감지됩니다.                                                                                                                                                    |
| `extra_credentials`                          | 이 매개변수는 선택 사항입니다. ClickHouse Cloud에서 역할 기반 접근을 위해 `role_arn`을 전달하는 데 사용됩니다. 구성 단계는 [Secure S3](/ko/cloud/data-sources/secure-s3)를 참조하십시오.                                                                                                                                  |

<div id="returned_value">
  ## 반환 값
</div>

S3에 있는 지정된 Hudi 테이블의 데이터를 읽기 위한, 지정된 구조의 테이블입니다.

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — 파일 경로입니다. 유형: `LowCardinality(String)`.
* `_file` — 파일 이름입니다. 유형: `LowCardinality(String)`.
* `_size` — 파일 크기(바이트)입니다. 유형: `Nullable(UInt64)`. 파일 크기를 알 수 없으면 값은 `NULL`입니다.
* `_time` — 파일의 마지막 수정 시간입니다. 유형: `Nullable(DateTime)`. 시간을 알 수 없으면 값은 `NULL`입니다.
* `_etag` — 파일의 etag입니다. 유형: `LowCardinality(String)`. etag를 알 수 없으면 값은 `NULL`입니다.

<div id="related">
  ## 관련
</div>

* [Hudi 엔진](/ko/engines/table-engines/integrations/hudi.md)
* [Hudi cluster 테이블 함수](/ko/sql-reference/table-functions/hudiCluster.md)