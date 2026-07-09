---
description: 'INSERT INTO SQL 문서'
sidebar_label: 'INSERT INTO'
sidebar_position: 33
slug: /sql-reference/statements/insert-into
title: 'INSERT INTO SQL 문'
doc_type: 'reference'
---

테이블에 데이터를 삽입합니다.

**구문**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

`(c1, c2, c3)`를 사용해 삽입할 컬럼 목록을 지정할 수 있습니다. `*`와 같은 컬럼 [매처](../../sql-reference/statements/select/index.md#asterisk) 및/또는 [APPLY](/ko/sql-reference/statements/select/apply-modifier), [EXCEPT](/ko/sql-reference/statements/select/except-modifier), [REPLACE](/ko/sql-reference/statements/select/replace-modifier)와 같은 [수정자](../../sql-reference/statements/select/index.md#select-modifiers)가 포함된 표현식도 사용할 수 있습니다.

예를 들어, 다음 테이블을 살펴보겠습니다:

```sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

```sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

`b` 컬럼을 제외한 모든 컬럼에 데이터를 삽입하려면 `EXCEPT` 키워드를 사용할 수 있습니다. 위의 구문에 따라, 지정한 컬럼 수(`(c1, c3)`)와 삽입하는 값의 개수(`VALUES (v11, v13)`)가 같아야 합니다:

```sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

```sql
SELECT * FROM insert_select_testtable;
```

```text
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

이 예시에서는 두 번째로 삽입된 행에서 `a` 및 `c` 컬럼은 전달된 값으로 채워지고, `b`는 기본값으로 채워지는 것을 볼 수 있습니다. 또한 기본값을 삽입할 때 `DEFAULT` 키워드를 사용할 수도 있습니다:

```sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

컬럼 목록에 기존 컬럼이 모두 포함되어 있지 않으면, 나머지 컬럼은 다음 값으로 채워집니다:

* 테이블 정의에 지정된 `DEFAULT` 표현식에서 계산된 값
* `DEFAULT` 표현식이 정의되어 있지 않으면 0과 빈 문자열

데이터는 ClickHouse가 지원하는 모든 [포맷](/ko/sql-reference/formats)으로 INSERT에 전달할 수 있습니다. 포맷은 쿼리에서 명시적으로 지정해야 합니다:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

예를 들어, 다음 쿼리 포맷은 `INSERT ... VALUES`의 기본 형식과 동일합니다:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse는 데이터 앞에 있는 모든 공백과 줄바꿈 문자 1개(있는 경우)를 제거합니다. 쿼리를 작성할 때는 데이터를 쿼리 연산자 다음 줄에 두는 것이 좋습니다. 데이터가 공백으로 시작하는 경우 특히 중요합니다.

예시:

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

[command-line client](/ko/operations/utilities/clickhouse-local) 또는 [HTTP 인터페이스](/ko/interfaces/http)를 사용하면 쿼리와 별개로 데이터를 삽입할 수 있습니다.

:::note
`INSERT` 쿼리에 `SETTINGS`를 지정하려면 `FORMAT` 절 *앞에* 지정해야 합니다. `FORMAT format_name` 뒤의 모든 내용은 데이터로 처리되기 때문입니다. 예시는 다음과 같습니다.

```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```

:::

<div id="constraints">
  ## 제약 조건
</div>

테이블에 [제약 조건](../../sql-reference/statements/create/table.md#constraints)이 있으면 삽입되는 데이터의 각 행에 대해 해당 표현식을 검사합니다. 이 제약 조건 중 하나라도 만족하지 않으면 서버에서 제약 조건 이름과 표현식이 포함된 예외를 발생시키며, 쿼리는 중단됩니다.

<div id="data-type-validation">
  ## 데이터 타입 검증
</div>

ClickHouse는 허용된 데이터 타입(`enable_time_time64_type`, `allow_suspicious_low_cardinality_types`, `allow_suspicious_fixed_string_types` 등의 설정으로 제어됨)을 `INSERT` 시에는 검증하지 않고, 테이블 생성(`CREATE TABLE`)과 스키마 수정(`ALTER TABLE`) 시에만 검증합니다.

즉, 허용되지 않는 데이터 타입을 가진 테이블이 이미 존재하는 경우, 서버에서 해당 설정이 비활성화되어 있더라도 그 테이블에는 데이터를 삽입할 수 있습니다. 이는 의도된 동작입니다. 테이블이 한 번 생성되면 타입 생성을 제어하는 설정 때문에 삽입이 차단되어서는 안 됩니다.

예시는 다음과 같습니다.

```sql
SET enable_time_time64_type = 1;

CREATE TABLE events
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id;

SET enable_time_time64_type = 0;

-- This works even though the setting is now disabled.
-- The table already exists, so inserts are not blocked.
INSERT INTO events VALUES (1, '14:30:25');

-- But creating a new table with the Time type will fail.
CREATE TABLE events_new
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id; -- ERR: TYPE_TIME_TIME64_IS_NOT_ENABLED
```

:::note
그 결과, 대상 테이블에 해당 컬럼 타입이 이미 있으면 더 최신 버전의 클라이언트(설정이 기본적으로 활성화된 상태)에서 더 오래된 버전의 서버(설정이 비활성화된 상태)로 허용되지 않는 데이터 타입의 데이터를 삽입할 수 있습니다. 검증은 DML 수준이 아니라 DDL 수준에서 수행됩니다.
:::

<div id="inserting-the-results-of-select">
  ## SELECT 결과 삽입
</div>

**구문**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

컬럼은 `SELECT` 절에서의 위치에 따라 매핑됩니다. 다만 `SELECT` 표현식에서의 이름과 `INSERT` 대상 테이블에서의 이름은 서로 다를 수 있습니다. 필요한 경우 타입 캐스팅이 수행됩니다.

Values 형식을 제외한 모든 데이터 포맷에서는 `now()`, `1 + 2` 등과 같은 표현식으로 값을 설정할 수 없습니다. Values 형식에서는 표현식을 제한적으로 사용할 수 있지만, 이 경우 비효율적인 코드로 실행되므로 권장되지 않습니다.

데이터 파트를 수정하는 다른 쿼리는 지원되지 않습니다: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
하지만 `ALTER TABLE ... DROP PARTITION`을 사용하여 오래된 데이터를 삭제할 수 있습니다.

`SELECT` 절에 테이블 함수 [input()](../../sql-reference/table-functions/input.md)가 포함된 경우, `FORMAT` 절은 쿼리의 끝에 지정해야 합니다.

널을 허용하지 않는 데이터 타입의 컬럼에 `NULL` 대신 기본값을 삽입하려면 [insert&#95;null&#95;as&#95;default](../../operations/settings/settings.md#insert_null_as_default) 설정을 활성화하십시오.

`INSERT`는 CTE(common table expression)도 지원합니다. 예를 들어, 다음 두 SQL 문은 동일합니다:

```sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

<div id="inserting-data-from-a-file">
  ## 파일에서 데이터 삽입
</div>

**구문**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

위 구문을 사용하면 **클라이언트** 측에 저장된 하나 이상의 파일에서 데이터를 삽입할 수 있습니다. `file_name` 및 `type`은 문자열 리터럴입니다. 입력 파일 [포맷](../../interfaces/formats.md)은 `FORMAT` 절에서 지정해야 합니다.

압축 파일도 지원합니다. 압축 유형은 파일 이름의 확장자를 통해 감지됩니다. 또는 `COMPRESSION` 절에서 명시적으로 지정할 수도 있습니다. 지원되는 유형은 다음과 같습니다: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

이 기능은 [command-line client](../../interfaces/client.md) 및 [clickhouse-local](../../operations/utilities/clickhouse-local.md)에서 사용할 수 있습니다.

**예시**

<div id="single-file-with-from-infile">
  ### FROM INFILE로 단일 파일 사용
</div>

[command-line client](../../interfaces/client.md)를 사용하여 다음 쿼리를 실행하십시오:

```bash title="Query"
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

```text title="Response"
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

<div id="multiple-files-with-from-infile-using-globs">
  ### 글롭 패턴을 사용해 FROM INFILE로 여러 파일 처리하기
</div>

이 예시는 이전 예시와 매우 유사하지만, `FROM INFILE 'input_*.csv'`를 사용해 여러 파일에서 삽입합니다.

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
`*`를 사용해 여러 파일을 선택하는 것 외에도, 범위(`{1,2}` 또는 `{1..9}`)와 기타 [글롭 치환](/ko/sql-reference/table-functions/file.md/#globs-in-path)을 사용할 수 있습니다. 아래 3가지 모두 위 예시에 사용할 수 있습니다:

```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```

:::

<div id="inserting-using-a-table-function">
  ## 테이블 함수를 사용한 삽입
</div>

[테이블 함수](../../sql-reference/table-functions/index.md)가 참조하는 테이블에 데이터를 삽입할 수 있습니다.

**구문**

```sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**예시**

다음 쿼리에서 [remote](/ko/sql-reference/table-functions/remote) 테이블 함수를 사용합니다:

```sql title="Query"
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

```text title="Response"
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

<div id="inserting-into-clickhouse-cloud">
  ## ClickHouse Cloud에 삽입하기
</div>

기본적으로 ClickHouse Cloud의 서비스는 고가용성을 위해 여러 개의 레플리카를 제공합니다. 서비스에 연결하면 이러한 레플리카 중 하나에 연결(connection)이 설정됩니다.

`INSERT`가 성공하면 데이터가 기반 스토리지에 기록됩니다. 하지만 레플리카가 이러한 업데이트를 수신하기까지는 다소 시간이 걸릴 수 있습니다. 따라서 다른 연결을 사용해 이들 다른 레플리카 중 하나에서 `SELECT` 쿼리를 실행하면, 업데이트된 데이터가 아직 반영되지 않았을 수 있습니다.

`select_sequential_consistency`를 사용하면 레플리카가 최신 업데이트를 수신하도록 강제할 수 있습니다. 다음은 이 설정을 사용하는 `SELECT` 쿼리의 예시입니다.

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

`select_sequential_consistency`를 사용하면 ClickHouse Keeper(ClickHouse Cloud에서 내부적으로 사용)가 받는 부하가 증가하며, 서비스 부하 수준에 따라 성능이 저하될 수 있습니다. 꼭 필요한 경우가 아니라면 이 설정을 활성화하지 않는 것이 좋습니다. 권장되는 방법은 동일한 세션에서 읽기/쓰기를 수행하거나, 네이티브 프로토콜을 사용하는 클라이언트 드라이버(즉, 스티키 연결을 지원하는 드라이버)를 사용하는 것입니다.

<div id="inserting-into-a-replicated-setup">
  ## 복제 구성에 삽입하기
</div>

복제 구성에서는 데이터가 복제된 후 다른 레플리카에도 표시됩니다. 데이터 복제는 `INSERT` 직후 즉시 시작되며(다른 레플리카에서 다운로드됨), 이는 데이터가 즉시 공유 스토리지에 기록되고 레플리카가 메타데이터 변경을 구독하는 ClickHouse Cloud와는 다릅니다.

복제 구성에서는 분산 합의를 위해 ClickHouse Keeper에 커밋해야 하므로 `INSERTs`에 때때로 상당한 시간(약 1초)이 걸릴 수 있습니다. 또한 스토리지로 S3를 사용하면 지연 시간이 더 늘어납니다.

<div id="performance-considerations">
  ## 성능 고려 사항
</div>

`INSERT`는 입력 데이터를 기본 키(primary key)로 정렬하고, 파티션 키(partition key)를 기준으로 파티션을 나눕니다. 여러 파티션에 데이터를 한 번에 삽입하면 `INSERT` 쿼리 성능이 크게 저하될 수 있습니다. 이를 방지하려면 다음을 따르십시오.

* 데이터는 한 번에 100,000행 정도의 비교적 큰 배치로 추가하십시오.
* ClickHouse에 업로드하기 전에 파티션 키를 기준으로 데이터를 그룹화하십시오.

다음과 같은 경우에는 성능이 저하되지 않습니다.

* 데이터를 실시간으로 추가하는 경우
* 일반적으로 시간순으로 정렬된 데이터를 업로드하는 경우

<div id="asynchronous-inserts">
  ### 비동기 삽입
</div>

작지만 빈번한 삽입 작업에서는 데이터를 비동기적으로 삽입할 수 있습니다. 이러한 삽입의 데이터는 배치로 묶인 다음 테이블에 안전하게 삽입됩니다. 비동기 삽입을 사용하려면 [`async_insert`](/ko/operations/settings/settings#async_insert) 설정을 활성화하십시오.

`async_insert` 또는 [`Buffer` 테이블 엔진](/ko/engines/table-engines/special/buffer)을 사용하면 추가 버퍼링이 발생합니다.

<div id="large-or-long-running-inserts">
  ### 대규모 또는 장시간 수행되는 삽입
</div>

대량의 데이터를 삽입할 때 ClickHouse는 &quot;squashing&quot;이라는 과정을 통해 쓰기 성능을 최적화합니다. 메모리에 있는 작은 삽입 데이터 블록은 디스크에 기록되기 전에 머지되어 더 큰 블록으로 합쳐집니다. 스쿼싱은 각 쓰기 작업에 수반되는 오버헤드를 줄여 줍니다. 이 과정에서 삽입된 데이터는 ClickHouse가 각 [`max_insert_block_size`](/ko/operations/settings/settings#max_insert_block_size)행에 대한 쓰기를 완료할 때마다 쿼리할 수 있게 됩니다.

**관련 항목**

* [async&#95;insert](/ko/operations/settings/settings#async_insert)
* [wait&#95;for&#95;async&#95;insert](/ko/operations/settings/settings#wait_for_async_insert)
* [wait&#95;for&#95;async&#95;insert&#95;timeout](/ko/operations/settings/settings#wait_for_async_insert_timeout)
* [async&#95;insert&#95;max&#95;data&#95;size](/ko/operations/settings/settings#async_insert_max_data_size)
* [async&#95;insert&#95;busy&#95;timeout&#95;ms](/ko/operations/settings/settings#async_insert_busy_timeout_max_ms)
* [async&#95;insert&#95;stale&#95;timeout&#95;ms](/ko/operations/settings/settings#async_insert_max_data_size)