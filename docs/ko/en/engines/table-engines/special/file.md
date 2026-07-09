---
description: 'File 테이블 엔진은 지원되는 파일 포맷(`TabSeparated`, `Native` 등) 중 하나를 사용하는 파일에 데이터를 저장합니다.'
sidebar_label: 'File'
sidebar_position: 40
slug: /engines/table-engines/special/file
title: 'File 테이블 엔진'
doc_type: '참고'
---

File 테이블 엔진은 지원되는 [파일 포맷](/ko/interfaces/formats#formats-overview) (`TabSeparated`, `Native` 등) 중 하나를 사용하는 파일에 데이터를 저장합니다.

사용 시나리오:

* ClickHouse의 데이터를 파일로 내보냅니다.
* 데이터를 한 포맷에서 다른 포맷으로 변환합니다.
* 디스크의 파일을 편집하여 ClickHouse 데이터를 업데이트합니다.

:::note
이 엔진은 현재 ClickHouse Cloud에서 사용할 수 없습니다. 대신 [S3 테이블 함수](/ko/sql-reference/table-functions/s3.md)를 사용하십시오.
:::

<div id="usage-in-clickhouse-server">
  ## ClickHouse 서버에서 사용
</div>

```sql
File(Format)
```

`Format` 매개변수는 사용 가능한 파일 포맷 중 하나를 지정합니다. `SELECT`
쿼리를 수행하려면 해당 포맷이 입력을 지원해야 하며, `INSERT`
쿼리를 수행하려면 출력을 지원해야 합니다. 사용 가능한 포맷은
[Formats](/ko/interfaces/formats#formats-overview) 섹션에 나와 있습니다.

ClickHouse에서는 `File`에 파일 시스템 경로를 지정할 수 없습니다. 대신 서버 구성의 [path](../../../operations/server-configuration-parameters/settings.md) 설정으로 정의된 폴더를 사용합니다.

`File(Format)`을 사용해 테이블을 생성하면 해당 폴더에 빈 하위 디렉터리가 생성됩니다. 데이터가 이 테이블에 기록되면 그 하위 디렉터리의 `data.Format` 파일에 저장됩니다.

서버 파일 시스템에서 이 하위 폴더와 파일을 수동으로 생성한 다음, 이름이 일치하는 테이블 정보에 [ATTACH](../../../sql-reference/statements/attach.md)하여 해당 파일의 데이터를 쿼리할 수 있습니다.

:::note
이 기능을 사용할 때는 주의하십시오. ClickHouse는 이러한 파일에 대한 외부 변경 사항을 추적하지 않습니다. ClickHouse를 통한 쓰기와 ClickHouse 외부에서의 쓰기가 동시에 발생할 경우 결과는 정의되지 않습니다.
:::

<div id="example">
  ## 예시
</div>

**1.** `file_engine_table` 테이블을 설정합니다:

```sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

기본적으로 ClickHouse는 `/var/lib/clickhouse/data/default/file_engine_table` 폴더를 생성합니다.

**2.** 다음 내용을 담은 `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` 파일을 수동으로 생성하십시오:

```bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** 데이터를 조회하세요:

```sql
SELECT * FROM file_engine_table
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="usage-in-clickhouse-local">
  ## ClickHouse-local에서의 사용
</div>

[clickhouse-local](../../../operations/utilities/clickhouse-local.md)에서 File 엔진은 `Format`뿐 아니라 파일 경로도 받을 수 있습니다. 기본 입력/출력 스트림은 `0` 또는 `stdin`, `1` 또는 `stdout`처럼 숫자 이름이나 사람이 읽기 쉬운 이름으로 지정할 수 있습니다. 추가 엔진 매개변수 또는 파일 확장자(`gz`, `br`, `xz`)에 따라 압축된 파일을 읽고 쓸 수 있습니다.

**예시:**

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

<div id="details-of-implementation">
  ## 구현 세부 정보
</div>

* 여러 `SELECT` 쿼리를 동시에 수행할 수 있지만, `INSERT` 쿼리는 서로 완료될 때까지 대기합니다.
* `INSERT` 쿼리로 새 파일을 생성할 수 있습니다.
* 파일이 이미 있으면 `INSERT`는 그 파일 끝에 새 데이터를 추가합니다.
* 다음은 지원되지 않습니다:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * 인덱스
  * 복제

<div id="partition-by">
  ## PARTITION BY
</div>

`PARTITION BY` — 선택 사항입니다. 파티션 키를 기준으로 데이터를 파티셔닝하면 별도의 파일을 만들 수 있습니다. 대부분의 경우 파티션 키는 필요하지 않으며, 필요하더라도 일반적으로 월 단위보다 더 세밀한 파티션 키는 필요하지 않습니다. 파티셔닝은 쿼리 속도를 높이지 않습니다(`ORDER BY` 표현식과는 다릅니다). 지나치게 세밀한 파티셔닝은 절대 사용하지 마십시오. 클라이언트 식별자나 이름을 기준으로 데이터를 파티셔닝하지 마십시오(대신 클라이언트 식별자 또는 이름을 `ORDER BY` 표현식의 첫 번째 컬럼으로 지정하십시오).

월 단위로 파티셔닝하려면 `toYYYYMM(date_column)` 표현식을 사용하십시오. 여기서 `date_column`은 [Date](/ko/sql-reference/data-types/date.md) 타입의 날짜를 저장하는 컬럼입니다. 이 경우 파티션 이름은 `"YYYYMM"` 포맷을 사용합니다.

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — 파일 경로입니다. 유형: `LowCardinality(String)`.
* `_file` — 파일 이름입니다. 유형: `LowCardinality(String)`.
* `_size` — 파일 크기(바이트)입니다. 유형: `Nullable(UInt64)`. 크기를 알 수 없으면 값은 `NULL`입니다.
* `_time` — 파일의 마지막 수정 시간입니다. 유형: `Nullable(DateTime)`. 시간을 알 수 없으면 값은 `NULL`입니다.

<div id="settings">
  ## 설정
</div>

* [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/ko/operations/settings/settings#engine_file_empty_if_not_exists) - 존재하지 않는 파일에서 빈 데이터를 선택할 수 있습니다. 기본적으로 비활성화되어 있습니다.
* [engine&#95;file&#95;truncate&#95;on&#95;insert](/ko/operations/settings/settings#engine_file_truncate_on_insert) - 파일에 데이터를 삽입하기 전에 파일을 TRUNCATE할 수 있습니다. 기본적으로 비활성화되어 있습니다.
* [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/ko/operations/settings/settings.md#engine_file_allow_create_multiple_files) - 포맷에 suffix가 있으면 데이터를 삽입할 때마다 새 파일을 생성할 수 있습니다. 기본적으로 비활성화되어 있습니다.
* [engine&#95;file&#95;skip&#95;empty&#95;files](/ko/operations/settings/settings.md#engine_file_skip_empty_files) - 읽는 중 빈 파일을 건너뛸 수 있습니다. 기본적으로 비활성화되어 있습니다.
* [storage&#95;file&#95;read&#95;method](/ko/operations/settings/settings#engine_file_empty_if_not_exists) - 스토리지 파일에서 데이터를 읽는 메서드로, `read`, `pread`, `mmap` 중 하나입니다. `mmap` 메서드는 clickhouse-server에는 적용되지 않으며(`clickhouse-local`용), 기본값은 clickhouse-server에서 `pread`, clickhouse-local에서 `mmap`입니다.