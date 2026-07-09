---
description: '이 엔진은 Amazon S3에 있는 기존 Apache Hudi 테이블에 대한 읽기 전용 통합을 제공합니다.'
sidebar_label: 'Hudi'
sidebar_position: 86
slug: /engines/table-engines/integrations/hudi
title: 'Hudi 테이블 엔진'
doc_type: 'reference'
---

이 엔진은 Amazon S3에 있는 기존 Apache [Hudi](https://hudi.apache.org/) 테이블에 대한 읽기 전용 통합을 제공합니다.

<div id="create-table">
  ## 테이블 생성
</div>

Hudi 테이블은 S3에 이미 존재해야 합니다. 이 명령은 새 테이블을 생성하는 DDL 매개변수를 받지 않습니다.

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**엔진 매개변수**

* `url` — 기존 Hudi 테이블의 경로가 포함된 버킷 URL입니다.
* `aws_access_key_id`, `aws_secret_access_key` - [AWS](https://aws.amazon.com/) 계정 사용자의 장기 자격 증명입니다. 이를 사용하여 요청을 인증할 수 있습니다. 이 매개변수는 선택 사항입니다. 자격 증명을 지정하지 않으면 설정 파일의 자격 증명이 사용됩니다.
* `extra_credentials` - 선택 사항입니다. ClickHouse Cloud에서 역할 기반 접근을 위해 `role_arn`을 전달하는 데 사용됩니다. 구성 단계는 [Secure S3](/ko/cloud/data-sources/secure-s3)를 참조하십시오.

엔진 매개변수는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용하여 지정할 수 있습니다.

**예시**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

이름이 지정된 컬렉션 사용하기:

```xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123</access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

<div id="see-also">
  ## 관련 항목
</div>

* [Hudi 테이블 함수](/ko/sql-reference/table-functions/hudi.md)