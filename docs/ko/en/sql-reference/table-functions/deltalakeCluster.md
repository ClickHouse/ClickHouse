---
description: 'deltaLake 테이블 함수의 확장 기능입니다.'
sidebar_label: 'deltaLakeCluster'
sidebar_position: 46
slug: /sql-reference/table-functions/deltalakeCluster
title: 'deltaLakeCluster'
doc_type: 'reference'
---

[deltaLake](/ko/sql-reference/table-functions/deltalake.md) 테이블 함수의 확장 기능입니다.

지정된 클러스터의 여러 노드에서 Amazon S3의 [Delta Lake](https://github.com/delta-io/delta) 테이블 파일을 병렬로 처리할 수 있습니다. initiator 노드는 클러스터의 모든 노드에 연결(connection)을 생성하고 각 파일을 동적으로 분배합니다. worker 노드는 처리할 다음 작업을 initiator에 요청한 뒤 이를 처리합니다. 이 과정은 모든 작업이 완료될 때까지 반복됩니다.

<div id="syntax">
  ## 구문
</div>

```sql
deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeCluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeS3Cluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
deltaLakeAzureCluster(cluster_name, named_collection[, option=value [,..]])
```

`deltaLakeS3Cluster`는 `deltaLakeCluster`의 별칭이며, 둘 다 S3에서 사용됩니다.

<div id="arguments">
  ## 인수
</div>

* `cluster_name` — 원격 및 로컬 server의 주소 집합과 연결 매개변수를 생성하는 데 사용되는 클러스터의 이름입니다.
* 나머지 모든 인수에 대한 설명은 해당 [deltaLake](/ko/sql-reference/table-functions/deltalake.md) 테이블 함수의 인수 설명과 동일합니다.
* 선택적 `extra_credentials` 매개변수를 사용해 ClickHouse Cloud에서 역할 기반 접근을 위한 `role_arn`을 전달할 수 있습니다. 구성 단계는 [Secure S3](/ko/cloud/data-sources/secure-s3)를 참조하십시오.

<div id="returned_value">
  ## 반환 값
</div>

S3의 지정된 Delta Lake 테이블에서 클러스터 데이터를 읽기 위한, 지정된 구조의 테이블입니다.

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

* [deltaLake 엔진](/ko/engines/table-engines/integrations/deltalake.md)
* [deltaLake 테이블 함수](/ko/sql-reference/table-functions/deltalake.md)