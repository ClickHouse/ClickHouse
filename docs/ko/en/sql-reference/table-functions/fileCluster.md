---
description: '클러스터 내 여러 노드에서 지정된 경로와 일치하는 파일을 동시에 처리할 수
  있도록 합니다. initiator는 worker 노드와 연결을 설정하고, 파일 경로의 글롭
  패턴을 확장한 다음, 파일 읽기 작업을 worker 노드에 할당합니다. 각 worker
  노드는 처리할 다음 파일을 받기 위해 initiator에 요청을 보내며, 모든 작업이
  완료될 때까지(즉, 모든 파일을 읽을 때까지) 이를 반복합니다.'
sidebar_label: 'fileCluster'
sidebar_position: 61
slug: /sql-reference/table-functions/fileCluster
title: 'fileCluster'
doc_type: 'reference'
---

클러스터 내 여러 노드에서 지정된 경로와 일치하는 파일을 동시에 처리할 수 있도록 합니다. initiator는 worker 노드와 연결을 설정하고, 파일 경로의 글롭 패턴을 확장한 다음, 파일 읽기 작업을 worker 노드에 할당합니다. 각 worker 노드는 처리할 다음 파일을 받기 위해 initiator에 요청을 보내며, 모든 작업이 완료될 때까지(즉, 모든 파일을 읽을 때까지) 이를 반복합니다.

:::note
이 함수는 처음 지정한 경로와 일치하는 파일 집합이 모든 노드에서 동일하고, 해당 파일의 내용도 노드 간에 일관된 경우에만 *올바르게* 동작합니다.
이 파일들이 노드마다 다르면 반환값은 미리 결정할 수 없으며, worker 노드가 initiator에 작업을 요청하는 순서에 따라 달라집니다.
:::

<div id="syntax">
  ## 구문
</div>

```sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

<div id="arguments">
  ## 인수
</div>

| 인수                   | 설명                                                                                                                                                              |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`       | 원격 및 로컬 server의 주소 집합과 connection 매개변수를 구성하는 데 사용되는 클러스터의 이름입니다.                                                                                             |
| `path`               | [user&#95;files&#95;path](/ko/operations/server-configuration-parameters/settings.md#user_files_path)를 기준으로 한 파일의 상대 경로입니다. 파일 경로는 [글롭 패턴](#globs-in-path)도 지원합니다. |
| `format`             | 파일의 [포맷](/ko/sql-reference/formats)입니다. 유형: [String](../../sql-reference/data-types/string.md).                                                                    |
| `structure`          | `'UserID UInt64, Name String'` 포맷의 table structure입니다. 컬럼 이름과 타입을 결정합니다. 유형: [String](../../sql-reference/data-types/string.md).                                |
| `compression_method` | 압축 방식입니다. 지원되는 압축 타입은 `gz`, `br`, `xz`, `zst`, `lz4`, `bz2`입니다.                                                                                                 |

<div id="returned_value">
  ## 반환 값
</div>

지정된 포맷과 구조를 가지며, 지정된 경로와 일치하는 파일의 데이터를 포함한 테이블입니다.

**예시**

`my_cluster`라는 이름의 클러스터가 있고, 설정 `user_files_path`의 값이 다음과 같다고 가정합니다:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

또한 각 클러스터 노드의 `user_files_path` 안에 `test1.csv` 및 `test2.csv` 파일이 있고, 이 파일들의 내용이 모든 노드에서 동일하다고 가정하면:

```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

예를 들어, 각 클러스터 노드에서 다음 두 쿼리를 실행하면 이러한 파일을 생성할 수 있습니다:

```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

이제 `fileCluster` 테이블 함수를 통해 `test1.csv`와 `test2.csv`의 데이터를 읽습니다:

```sql
SELECT * FROM fileCluster('my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY i, s
```

```response
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```

<div id="globs-in-path">
  ## 경로 내 글롭 패턴
</div>

[File](../../sql-reference/table-functions/file.md#globs-in-path) 테이블 함수에서 지원하는 모든 패턴은 FileCluster에서도 지원됩니다.

<div id="related">
  ## 관련 문서
</div>

* [File 테이블 함수](../../sql-reference/table-functions/file.md)