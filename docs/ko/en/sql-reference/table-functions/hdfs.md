---
description: 'HDFS의 파일에서 테이블을 생성합니다. 이 테이블 함수는
  `url` 및 `file` 테이블 함수와 유사합니다.'
sidebar_label: 'hdfs'
sidebar_position: 80
slug: /sql-reference/table-functions/hdfs
title: 'hdfs'
doc_type: '참고'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-function">
  # hdfs 테이블 함수
</div>

HDFS의 파일로부터 테이블을 생성합니다. 이 테이블 함수는 [url](../../sql-reference/table-functions/url.md) 및 [file](../../sql-reference/table-functions/file.md) 테이블 함수와 비슷합니다.

<div id="syntax">
  ## 구문
</div>

```sql
hdfs(URI, format, structure)
```

<div id="arguments">
  ## 인수
</div>

| 인수          | 설명                                                                                                                                       |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `URI`       | HDFS에 있는 파일의 상대 URI입니다. 파일 경로는 읽기 전용 모드에서 다음 글롭 패턴을 지원합니다: `*`, `?`, `{abc,def}`, `{N..M}`. 여기서 `N`, `M`은 숫자이고 `'abc'`, `'def'`는 문자열입니다. |
| `format`    | 파일의 [포맷](/ko/sql-reference/formats)입니다.                                                                                                     |
| `structure` | 테이블 구조입니다. 형식은 `'column1_name column1_type, column2_name column2_type, ...'`입니다.                                                         |

<div id="returned_value">
  ## 반환 값
</div>

지정된 파일에서 데이터를 읽거나 쓰기 위한, 지정된 구조의 테이블입니다.

**예시**

`hdfs://hdfs1:9000/test`에 있는 테이블과, 이 테이블에서 처음 두 행을 선택하는 예:

```sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="globs_in_path">
  ## 경로의 글롭 패턴
</div>

경로에서 글롭 패턴을 사용할 수 있습니다. 파일은 접미사나 접두사만이 아니라 전체 경로 패턴과 일치해야 합니다.

* `*` — 빈 문자열을 포함하며 `/`를 제외한 임의 개수의 문자를 나타냅니다.
* `**` — 폴더 내부의 모든 파일을 재귀적으로 나타냅니다.
* `?` — 임의의 단일 문자를 나타냅니다.
* `{some_string,another_string,yet_another_one}` — `'some_string'`, `'another_string'`, `'yet_another_one'` 중 임의의 문자열로 치환됩니다. 이 문자열에는 `/` 기호를 포함할 수 있습니다.
* `{N..M}` — `>= N` 이고 `<= M` 인 임의의 숫자를 나타냅니다.

`{}`를 사용하는 구문은 [remote](remote.md) 및 [file](file.md) 테이블 함수와 유사합니다.

**예시**

1. HDFS에 다음 URI를 가진 여러 파일이 있다고 가정합니다.

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. 이 파일들의 행 수를 조회합니다:

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. 이 두 디렉터리의 모든 파일에 있는 행 수를 조회합니다:

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
파일 목록에 앞자리가 0으로 채워진 숫자 범위가 포함되어 있으면, 각 자릿수별로 중괄호를 사용하는 구문이나 `?`를 사용하십시오.
:::

**예시**

`file000`, `file001`, ... , `file999`라는 이름의 파일에서 데이터를 쿼리합니다:

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — 파일 경로입니다. 유형: `LowCardinality(String)`.
* `_file` — 파일 이름입니다. 유형: `LowCardinality(String)`.
* `_size` — 파일 크기(바이트)입니다. 유형: `Nullable(UInt64)`. 크기를 알 수 없으면 값은 `NULL`입니다.
* `_time` — 파일의 마지막 수정 시간입니다. 유형: `Nullable(DateTime)`. 시간을 알 수 없으면 값은 `NULL`입니다.

<div id="hive-style-partitioning">
  ## use_hive_partitioning 설정
</div>

`use_hive_partitioning` 설정 값을 1로 지정하면 ClickHouse가 경로(`/name=value/`)에서 Hive 스타일 파티셔닝을 감지하고, 쿼리에서 파티션 컬럼을 가상 컬럼으로 사용할 수 있습니다. 이 가상 컬럼의 이름은 파티셔닝된 경로에 지정된 이름과 동일합니다.

**예시**

Hive 스타일 파티셔닝으로 생성된 가상 컬럼 사용

```sql
SELECT * FROM HDFS('hdfs://hdfs1:9000/data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="storage-settings">
  ## 스토리지 설정
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/ko/operations/settings/settings.md#hdfs_truncate_on_insert) - 파일에 데이터를 삽입하기 전에 파일 내용을 비울 수 있도록 합니다. 기본적으로 비활성화되어 있습니다.
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/ko/operations/settings/settings.md#hdfs_create_new_file_on_insert) - 포맷에 접미사가 있는 경우 삽입할 때마다 새 파일을 생성할 수 있도록 합니다. 기본적으로 비활성화되어 있습니다.
* [hdfs&#95;skip&#95;empty&#95;files](/ko/operations/settings/settings.md#hdfs_skip_empty_files) - 읽을 때 빈 파일을 건너뛸 수 있도록 합니다. 기본적으로 비활성화되어 있습니다.

<div id="related">
  ## 관련 항목
</div>

* [가상 컬럼](../../engines/table-engines/index.md#table_engines-virtual_columns)