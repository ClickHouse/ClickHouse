---
description: 's3 테이블 함수와 유사하게, 파일에 대해 SELECT 및 INSERT를 수행할 수 있는 테이블과 유사한 인터페이스를 제공하는 테이블 엔진입니다. 로컬 파일로 작업할 때는 `file`을 사용하고, S3, GCS, MinIO와 같은 객체 스토리지의 버킷으로 작업할 때는 `s3`를 사용합니다.'
sidebar_label: 'file'
sidebar_position: 60
slug: /sql-reference/table-functions/file
title: 'file'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="file-table-function">
  # file 테이블 함수
</div>

파일에서 `SELECT`하고 파일에 `INSERT`할 수 있도록 테이블처럼 사용할 수 있는 인터페이스를 제공하는 테이블 엔진이며, [s3](/ko/sql-reference/table-functions/s3.md) 테이블 함수와 유사합니다. 로컬 파일로 작업할 때는 `file`을 사용하고, S3, GCS, MinIO와 같은 객체 스토리지의 버킷으로 작업할 때는 `s3`를 사용합니다.

`file` 함수는 파일을 읽거나 파일에 쓰기 위해 `SELECT` 및 `INSERT` 쿼리에서 사용할 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
file([path_to_archive ::] path [,format] [,structure] [,compression])
```

`SELECT` 쿼리에서는 `path`가 `Array(String)`을 반환하는 표현식일 수도 있습니다:

```sql
file(['file1.csv', 'file2.csv'], 'CSV', 'column1 UInt32, column2 UInt32')
```

<div id="arguments">
  ## 인수
</div>

| 매개변수              | 설명                                                                                                                                                                                                                                                                                          |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`            | [user&#95;files&#95;path](/ko/operations/server-configuration-parameters/settings.md#user_files_path)를 기준으로 한 파일의 상대 경로 또는 `SELECT` 쿼리에서 사용하는 경로 `Array(String)`입니다. 읽기 전용 모드에서는 다음 [글롭 패턴](#globs-in-path)을 지원합니다: `*`, `?`, `{abc,def}` (`'abc'` 및 `'def'`는 문자열) 및 `{N..M}` (`N` 및 `M`은 숫자). |
| `path_to_archive` | zip/tar/7z 아카이브의 상대 경로입니다. `path`와 동일한 글롭 패턴을 지원합니다.                                                                                                                                                                                                                                        |
| `format`          | 파일의 [포맷](/ko/interfaces/formats)입니다.                                                                                                                                                                                                                                                           |
| `structure`       | 테이블의 구조입니다. 포맷: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                              |
| `compression`     | `SELECT` 쿼리에서 사용할 때는 기존 압축 유형을, `INSERT` 쿼리에서 사용할 때는 원하는 압축 유형을 지정합니다. 지원되는 압축 유형은 `gz`, `br`, `xz`, `zst`, `lz4`, `bz2`입니다.                                                                                                                                                                |

:::tip
`structure` 인수를 생략하면 ClickHouse가 포맷 자체에서 스키마를 추론합니다.
포맷마다 기본 컬럼 이름과 타입이 다르게 생성됩니다.
특정 포맷의 스키마를 확인하려면 [`DESC`](/ko/sql-reference/statements/describe-table)와 [`format`](/ko/sql-reference/table-functions/format) 테이블 함수를 사용하십시오.

예시:

```sql
DESC format(LineAsString, 'Hello\nWorld')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

:::

<div id="returned_value">
  ## 반환 값
</div>

파일에 있는 데이터를 읽거나 쓰는 테이블입니다.

<div id="examples-for-writing-to-a-file">
  ## 파일에 쓰는 예시
</div>

<div id="write-to-a-tsv-file">
  ### TSV 파일에 기록하기
</div>

```sql
INSERT INTO TABLE FUNCTION
file('test.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

그 결과 데이터가 `test.tsv` 파일에 기록됩니다:

```bash
# cat /var/lib/clickhouse/user_files/test.tsv
1    2    3
3    2    1
1    3    2
```

<div id="partitioned-write-to-multiple-tsv-files">
  ### 여러 TSV 파일에 파티션별로 쓰기
</div>

`file` 유형의 테이블 함수에 데이터를 삽입할 때 `PARTITION BY` 표현식을 지정하면 파티션별로 별도의 파일이 생성됩니다. 데이터를 별도 파일로 나누면 읽기 작업 성능을 높이는 데 도움이 됩니다.

```sql
INSERT INTO TABLE FUNCTION
file('test_{_partition_id}.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

그 결과, 데이터는 `test_1.tsv`, `test_2.tsv`, `test_3.tsv`의 세 개 파일에 기록됩니다.

```bash
# cat /var/lib/clickhouse/user_files/test_1.tsv
3    2    1

# cat /var/lib/clickhouse/user_files/test_2.tsv
1    3    2

# cat /var/lib/clickhouse/user_files/test_3.tsv
1    2    3
```

<div id="examples-for-reading-from-a-file">
  ## 파일에서 읽는 예시
</div>

<div id="select-from-a-csv-file">
  ### CSV 파일에서 SELECT
</div>

먼저 서버 구성에서 `user_files_path`를 설정하고 `test.csv` 파일을 준비합니다:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

그런 다음, `test.csv`의 데이터를 테이블에 읽어들인 후 처음 2개 행을 선택합니다:

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="inserting-data-from-a-file-into-a-table">
  ### 파일의 데이터를 테이블(table)에 삽입하기
</div>

```sql
INSERT INTO FUNCTION
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1);
```

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

`archive1.zip` 또는 `archive2.zip` 중 하나 또는 둘 다에 있는 `table.csv`에서 데이터를 읽습니다:

```sql
SELECT * FROM file('user_files/archives/archive{1..2}.zip :: table.csv');
```

<div id="globs-in-path">
  ## 경로의 글롭 패턴
</div>

경로에는 글로빙을 사용할 수 있습니다. 파일은 접미사나 접두사만이 아니라 전체 경로 패턴과 일치해야 합니다. 다만 한 가지 예외가 있습니다. 경로가 기존
디렉터리를 가리키고 글롭을 사용하지 않는 경우, `*`가 경로에 암묵적으로 추가되어
디렉터리 안의 모든 파일이 선택됩니다.

* `*` — 빈 문자열을 포함하되 `/`를 제외한 임의 개수의 문자를 나타냅니다.
* `?` — 임의의 단일 문자를 나타냅니다.
* `{some_string,another_string,yet_another_one}` — `'some_string', 'another_string', 'yet_another_one'` 문자열 중 하나로 대체합니다. 문자열에는 `/` 기호가 포함될 수 있습니다.
* `{N..M}` — `>= N` 이고 `<= M` 인 임의의 숫자를 나타냅니다.
* `**` - 폴더 내부의 모든 파일을 재귀적으로 나타냅니다.

`{}`를 사용하는 구문은 [remote](remote.md) 및 [hdfs](hdfs.md) 테이블 함수와 유사합니다.

<div id="examples">
  ## 예시
</div>

**예시**

다음과 같은 상대 경로를 가진 파일들이 있다고 가정합니다:

* `some_dir/some_file_1`
* `some_dir/some_file_2`
* `some_dir/some_file_3`
* `another_dir/some_file_1`
* `another_dir/some_file_2`
* `another_dir/some_file_3`

모든 파일의 총 행 수를 조회합니다:

```sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

같은 결과를 얻을 수 있는 대체 경로 표현식:

```sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

암시적 `*`를 사용해 `some_dir`의 전체 행 수를 쿼리합니다:

```sql
SELECT count(*) FROM file('some_dir', 'TSV', 'name String, value UInt32');
```

:::note
파일 목록에 앞자리가 0으로 채워진 숫자 범위가 포함된 경우, 각 자릿수마다 중괄호를 사용하는 구문이나 `?`를 사용하십시오.
:::

**예시**

`file000`, `file001`, ... , `file999`라는 이름의 파일에 있는 총 행 수를 쿼리합니다:

```sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

**예시**

`big_dir/` 디렉터리 내의 모든 파일에서 총 행 수를 재귀적으로 조회합니다:

```sql
SELECT count(*) FROM file('big_dir/**', 'CSV', 'name String, value UInt32');
```

**예시**

`big_dir/` 디렉터리 내 임의의 폴더에 있는 모든 `file002` 파일의 총 행 수를 하위 폴더까지 포함해 조회합니다:

```sql
SELECT count(*) FROM file('big_dir/**/file002', 'CSV', 'name String, value UInt32');
```

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — 파일 경로입니다. 유형: `LowCardinality(String)`.
* `_file` — 파일 이름입니다. 유형: `LowCardinality(String)`.
* `_size` — 파일 크기(바이트 단위)입니다. 유형: `Nullable(UInt64)`. 파일 크기를 알 수 없으면 값은 `NULL`입니다.
* `_time` — 파일의 최종 수정 시각입니다. 유형: `Nullable(DateTime)`. 시각을 알 수 없으면 값은 `NULL`입니다.

<div id="hive-style-partitioning">
  ## use_hive_partitioning 설정
</div>

`use_hive_partitioning`을 1로 설정하면 ClickHouse가 경로(`/name=value/`)에서 Hive 스타일 파티셔닝을 감지하고, 쿼리에서 파티션 컬럼을 가상 컬럼으로 사용할 수 있습니다. 이러한 가상 컬럼은 파티셔닝된 경로에 있는 이름과 동일한 이름을 가집니다.

**예시**

Hive 스타일 파티셔닝으로 생성된 가상 컬럼 사용

```sql
SELECT * FROM file('data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="settings">
  ## 설정
</div>

| Setting                                                                                                                                 | Description                                                                                                                                            |
| --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/ko/operations/settings/settings#engine_file_empty_if_not_exists)                    | 존재하지 않는 파일에서 빈 데이터를 읽을 수 있게 합니다. 기본적으로 비활성화되어 있습니다.                                                                                                    |
| [engine&#95;file&#95;truncate&#95;on&#95;insert](/ko/operations/settings/settings#engine_file_truncate_on_insert)                          | 파일에 삽입하기 전에 파일을 비울 수 있게 합니다. 기본적으로 비활성화되어 있습니다.                                                                                                        |
| [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/ko/operations/settings/settings.md#engine_file_allow_create_multiple_files) | 포맷에 접미사가 있으면 삽입할 때마다 새 파일을 생성할 수 있게 합니다. 기본적으로 비활성화되어 있습니다.                                                                                            |
| [engine&#95;file&#95;skip&#95;empty&#95;files](/ko/operations/settings/settings.md#engine_file_skip_empty_files)                           | 읽는 중에 빈 파일을 건너뛸 수 있게 합니다. 기본적으로 비활성화되어 있습니다.                                                                                                           |
| [storage&#95;file&#95;read&#95;method](/ko/operations/settings/settings#engine_file_empty_if_not_exists)                                   | 스토리지 파일에서 데이터를 읽는 메서드로, 다음 중 하나입니다: read, pread, mmap (clickhouse-local에서만 사용 가능). 기본값은 clickhouse-server의 경우 `pread`, clickhouse-local의 경우 `mmap`입니다. |

<div id="related">
  ## 관련
</div>

* [가상 컬럼](/ko/engines/table-engines/index.md#table_engines-virtual_columns)
* [처리 후 파일 이름 변경](/ko/operations/settings/settings.md#rename_files_after_processing)