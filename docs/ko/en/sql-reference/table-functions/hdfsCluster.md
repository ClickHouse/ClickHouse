---
description: '지정된 클러스터의 여러 노드에서 HDFS 파일을 병렬로 처리할 수 있습니다.'
sidebar_label: 'hdfsCluster'
sidebar_position: 81
slug: /sql-reference/table-functions/hdfsCluster
title: 'hdfsCluster'
doc_type: 'reference'
---

지정된 클러스터의 여러 노드에서 HDFS 파일을 병렬로 처리할 수 있습니다. initiator 노드에서는 클러스터의 모든 노드에 연결을 생성하고, HDFS 파일 경로의 애스터리스크를 확장한 다음 각 파일을 동적으로 분배합니다. worker 노드에서는 initiator에 다음에 처리할 작업을 요청한 뒤 이를 처리합니다. 이 과정은 모든 작업이 완료될 때까지 반복됩니다.

<div id="syntax">
  ## 구문
</div>

```sql
hdfsCluster(cluster_name, URI, format, structure)
```

<div id="arguments">
  ## 인수
</div>

| 인수             | 설명                                                                                                                                                                                                                                            |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | 원격 및 로컬 서버의 주소 집합과 연결 매개변수를 구성하는 데 사용되는 클러스터 이름입니다.                                                                                                                                                                                           |
| `URI`          | 파일 또는 여러 파일에 대한 URI입니다. 읽기 전용(readonly) 모드에서는 `*`, `**`, `?`, `{'abc','def'}`, `{N..M}` 와일드카드를 지원합니다. 여기서 `N`, `M`은 숫자이고 `abc`, `def`는 문자열입니다. 자세한 내용은 [경로의 와일드카드](../../engines/table-engines/integrations/s3.md#wildcards-in-path)를 참조하십시오. |
| `format`       | 파일의 [포맷](/ko/sql-reference/formats)입니다.                                                                                                                                                                                                          |
| `structure`    | 테이블의 구조입니다. 형식은 `'column1_name column1_type, column2_name column2_type, ...'`입니다.                                                                                                                                                             |

<div id="returned_value">
  ## 반환 값
</div>

지정된 파일의 데이터를 읽기 위한 지정된 구조의 테이블입니다.

<div id="examples">
  ## 예시
</div>

1. `cluster_simple`이라는 이름의 ClickHouse 클러스터가 있고, HDFS에 다음 URI의 여러 파일이 있다고 가정합니다:

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. 이 파일들의 행 수를 조회합니다:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. 다음 두 디렉터리의 모든 파일에 있는 행 수를 쿼리합니다:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
파일 목록에 앞자리에 0이 포함된 숫자 범위가 있으면 각 자릿수에 대해 중괄호를 사용하는 구문을 쓰거나 `?`를 사용하십시오.
:::

<div id="related">
  ## 관련
</div>

* [HDFS 엔진](../../engines/table-engines/integrations/hdfs.md)
* [HDFS 테이블 함수](../../sql-reference/table-functions/hdfs.md)