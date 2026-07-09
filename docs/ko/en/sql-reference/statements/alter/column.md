---
description: '컬럼에 대한 문서'
sidebar_label: 'COLUMN'
sidebar_position: 37
slug: /sql-reference/statements/alter/column
title: '컬럼 조작'
doc_type: 'reference'
---

테이블 구조를 변경할 수 있는 일련의 쿼리입니다.

구문:

```sql
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

쿼리에서 쉼표로 구분된 하나 이상의 작업 목록을 지정합니다.
각 작업은 컬럼에 수행하는 연산입니다.

지원되는 작업은 다음과 같습니다.

* [ADD COLUMN](#add-column) — 테이블에 새 컬럼을 추가합니다.
* [DROP COLUMN](#drop-column) — 컬럼을 삭제합니다.
* [RENAME COLUMN](#rename-column) — 기존 컬럼의 이름을 변경합니다.
* [CLEAR COLUMN](#clear-column) — 컬럼 값을 재설정합니다.
* [COMMENT COLUMN](#comment-column) — 컬럼에 텍스트 주석을 추가합니다.
* [MODIFY COLUMN](#modify-column) — 컬럼의 유형, 기본 표현식, TTL 및 컬럼 설정을 변경합니다.
* [MODIFY COLUMN REMOVE](#modify-column-remove) — 컬럼 속성 중 하나를 제거합니다.
* [MODIFY COLUMN MODIFY SETTING](#modify-column-modify-setting) - 컬럼 설정을 변경합니다.
* [MODIFY COLUMN RESET SETTING](#modify-column-reset-setting) - 컬럼 설정을 재설정합니다.
* [MODIFY COLUMN ADD ENUM VALUES](#modify-column-add-enum-values) - Enum에 새 값을 추가합니다.
* [MATERIALIZE COLUMN](#materialize-column) — 컬럼이 없는 파트에서 해당 컬럼을 구체화합니다.
  이러한 작업은 아래에서 자세히 설명합니다.

<div id="add-column">
  ## ADD COLUMN
</div>

```sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

지정된 `name`, `type`, [`codec`](../create/table.md/#column_compression_codec), `default_expr`를 사용해 테이블에 새 컬럼을 추가합니다([기본 표현식](/ko/sql-reference/statements/create/table#default_values) 섹션 참고).

`IF NOT EXISTS` 절이 포함되면 컬럼이 이미 존재하더라도 쿼리는 오류를 반환하지 않습니다. `AFTER name_after`(다른 컬럼의 이름)를 지정하면 해당 컬럼은 테이블 컬럼 목록에서 지정한 컬럼 뒤에 추가됩니다. 컬럼을 테이블의 맨 앞에 추가하려면 `FIRST` 절을 사용하세요. 그렇지 않으면 컬럼은 테이블 끝에 추가됩니다. 일련의 작업에서는 `name_after`로 이전 작업 중 하나에서 추가된 컬럼의 이름을 사용할 수 있습니다.

컬럼을 추가해도 데이터에는 아무 작업도 수행되지 않고 테이블 구조만 변경됩니다. `ALTER` 후에도 데이터는 디스크에 나타나지 않습니다. 테이블을 읽을 때 컬럼 데이터가 없으면 기본값으로 채워집니다(기본 표현식이 있으면 이를 계산하고, 없으면 0 또는 빈 문자열을 사용함). 이 컬럼은 데이터 파트가 머지된 후 디스크에 나타납니다([MergeTree](/ko/engines/table-engines/mergetree-family/mergetree.md) 참고).

이 방식은 기존 데이터의 양을 늘리지 않고 `ALTER` 쿼리를 즉시 완료할 수 있게 해줍니다.

예시:

```sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

```text
Added1  UInt32
CounterID       UInt32
StartDate       Date
UserID  UInt32
VisitID UInt32
NestedColumn.A  Array(UInt8)
NestedColumn.S  Array(String)
Added2  UInt32
ToDrop  UInt32
Added3  UInt32
```

<div id="drop-column">
  ## DROP COLUMN
</div>

```sql
DROP COLUMN [IF EXISTS] name
```

이름이 `name`인 컬럼을 삭제합니다. `IF EXISTS` 절이 지정된 경우, 해당 컬럼이 존재하지 않더라도 쿼리에서 오류를 반환하지 않습니다.

파일 시스템에서 데이터를 삭제합니다. 전체 파일을 삭제하므로 쿼리가 거의 즉시 완료됩니다.

:::tip
컬럼이 [materialized view](/ko/sql-reference/statements/create/view)에서 참조되고 있으면 삭제할 수 없습니다. 그렇지 않으면 오류가 반환됩니다.
:::

예시:

```sql
ALTER TABLE visits DROP COLUMN browser
```

<div id="rename-column">
  ## RENAME COLUMN
</div>

```sql
RENAME COLUMN [IF EXISTS] name to new_name
```

컬럼 `name`의 이름을 `new_name`으로 변경합니다. `IF EXISTS` 절을 지정하면 해당 컬럼이 존재하지 않아도 쿼리에서 오류를 반환하지 않습니다. 이름 변경은 실제 데이터를 건드리지 않으므로 쿼리는 거의 즉시 완료됩니다.

**참고**: 테이블의 키 표현식(`ORDER BY` 또는 `PRIMARY KEY`)에 지정된 컬럼은 이름을 변경할 수 없습니다. 이러한 컬럼의 이름을 변경하려고 하면 `SQL Error [524]`가 발생합니다.

예시:

```sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

<div id="clear-column">
  ## CLEAR COLUMN
</div>

```sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

지정된 파티션의 컬럼에 있는 모든 데이터를 재설정합니다. 파티션 이름 설정에 대한 자세한 내용은 [How to set the partition expression](../alter/partition.md/#how-to-set-partition-expression) 섹션을 참조하십시오.

`IF EXISTS` 절을 지정하면 컬럼이 존재하지 않더라도 쿼리에서 오류를 반환하지 않습니다.

예시:

```sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

<div id="comment-column">
  ## COMMENT COLUMN
</div>

```sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

컬럼에 주석을 추가합니다. `IF EXISTS` 절을 지정하면 컬럼이 존재하지 않더라도 쿼리에서 오류가 반환되지 않습니다.

각 컬럼에는 하나의 주석만 지정할 수 있습니다. 컬럼에 이미 주석이 있으면 새 주석이 기존 주석을 덮어씁니다.

주석은 [DESCRIBE TABLE](/ko/sql-reference/statements/describe-table.md) 쿼리에서 반환되는 `comment_expression` 컬럼에 저장됩니다.

예시:

```sql
ALTER TABLE visits COMMENT COLUMN browser 'This column shows the browser used for accessing the site.'
```

<div id="modify-column">
  ## MODIFY COLUMN
</div>

```sql
MODIFY COLUMN [IF EXISTS] name
    [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
ALTER COLUMN [IF EXISTS] name
    TYPE [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
```

이 쿼리는 `name` 컬럼의 다음 속성을 변경합니다:

* 유형

* 기본 표현식

* 압축 코덱

* TTL

* 컬럼 수준 설정

* Enum/Enum8/Enum16 타입의 Enum 값

컬럼 압축 코덱 변경 예시는 [Column Compression Codecs](../create/table.md/#column_compression_codec)를 참조하십시오.

컬럼 TTL 변경 예시는 [Column TTL](/ko/engines/table-engines/mergetree-family/mergetree.md/#mergetree-column-ttl)을 참조하십시오.

컬럼 수준 설정 변경 예시는 [Column-level Settings](/ko/engines/table-engines/mergetree-family/mergetree.md/#column-level-settings)를 참조하십시오.

`IF EXISTS` 절이 지정되면 해당 컬럼이 존재하지 않더라도 쿼리는 오류를 반환하지 않습니다.

타입을 변경할 때 값은 [toType](/ko/sql-reference/functions/type-conversion-functions.md) 함수가 적용된 것처럼 변환됩니다. 기본 표현식만 변경하는 경우 쿼리는 복잡한 작업을 수행하지 않으며 거의 즉시 완료됩니다.

예시:

```sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

복잡한 작업은 컬럼 타입 변경뿐입니다. 이 작업은 데이터가 저장된 파일의 내용을 변경합니다. 테이블이 크면 시간이 오래 걸릴 수 있습니다.

이 쿼리는 `FIRST | AFTER` 절을 사용해 컬럼 순서도 변경할 수 있습니다. 자세한 내용은 [ADD COLUMN](#add-column) 설명을 참조하십시오. 다만 이 경우에는 컬럼 타입을 반드시 지정해야 합니다.

예시:

```sql
CREATE TABLE users (
    c1 Int16,
    c2 String
) ENGINE = MergeTree
ORDER BY c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴

ALTER TABLE users MODIFY COLUMN c2 String FIRST;

DESCRIBE users;
┌─name─┬─type───┬
│ c2   │ String │
│ c1   │ Int16  │
└──────┴────────┴

ALTER TABLE users ALTER COLUMN c2 TYPE String AFTER c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴
```

`ALTER` 쿼리는 atomic합니다. MergeTree 테이블에서는 잠금 없이도 수행됩니다.

컬럼을 변경하는 `ALTER` 쿼리는 복제됩니다. 명령은 ZooKeeper에 저장된 후 각 레플리카에 적용됩니다. 모든 `ALTER` 쿼리는 동일한 순서로 실행됩니다. 이 쿼리는 다른 레플리카에서 필요한 작업이 완료될 때까지 기다립니다. 하지만 복제된 테이블의 컬럼을 변경하는 쿼리는 중단될 수 있으며, 모든 작업은 비동기로 수행됩니다.

:::note
널 허용 컬럼을 Non-널 허용로 변경할 때는 각별히 주의하십시오. NULL 값이 없는지 반드시 확인해야 합니다. 그렇지 않으면 해당 컬럼을 읽을 때 문제가 발생할 수 있습니다. 이런 경우에는 뮤테이션을 중단하고 컬럼을 다시 널 허용 유형으로 되돌려야 합니다.
:::

<div id="modify-column-remove">
  ## MODIFY COLUMN REMOVE
</div>

컬럼 속성 `DEFAULT`, `ALIAS`, `MATERIALIZED`, `CODEC`, `COMMENT`, `TTL`, `SETTINGS` 중 하나를 제거합니다.

구문:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name REMOVE property;
```

**예시**

TTL 제거:

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**관련 항목**

* [REMOVE TTL](ttl.md).

<div id="modify-column-modify-setting">
  ## MODIFY COLUMN MODIFY SETTING
</div>

컬럼 설정을 수정합니다.

구문:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING name=value,...;
```

**예시**

컬럼의 `max_compress_block_size`를 `1MB`로 변경합니다:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING max_compress_block_size = 1048576;
```

<div id="modify-column-reset-setting">
  ## MODIFY COLUMN RESET SETTING
</div>

컬럼 설정을 초기화하며, 테이블의 CREATE 쿼리 내 컬럼 표현식에서 해당 설정 선언도 제거합니다.

구문:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING name,...;
```

**예시**

컬럼 설정 `max_compress_block_size`를 기본값으로 되돌립니다:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING max_compress_block_size;
```

<div id="modify-column-add-enum-values">
  ## MODIFY COLUMN ADD ENUM VALUES
</div>

`Enum`, `Enum8`, `Enum16`, `Nullable(Enum)`, `Nullable(Enum8)` 또는 `Nullable(Enum16)` 유형의 컬럼에 새 값을 추가합니다

구문:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('EnumName' [= number], ...);
```

**예시**

컬럼 `enum_column_name`에 두 개의 값을 추가합니다:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('Hundred' = 100, 'HundredOne');
```

<div id="materialize-column">
  ## MATERIALIZE COLUMN
</div>

`DEFAULT` 또는 `MATERIALIZED` 값 표현식이 있는 컬럼을 구체화합니다. `ALTER TABLE table_name ADD COLUMN column_name MATERIALIZED`를 사용해 구체화된 컬럼을 추가할 때, 구체화된 값이 없는 기존 행은 자동으로 채워지지 않습니다. `MATERIALIZE COLUMN` SQL 문은 `DEFAULT` 또는 `MATERIALIZED` 표현식을 추가하거나 업데이트한 후 기존 컬럼 데이터를 재작성하는 데 사용할 수 있습니다(이 경우 메타데이터만 업데이트되고 기존 데이터는 변경되지 않음). 정렬 키에 포함된 컬럼을 구체화하는 작업은 정렬 순서를 깨뜨릴 수 있으므로 유효하지 않습니다.
이는 [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)으로 구현됩니다.

새로 추가되었거나 업데이트된 `MATERIALIZED` 값 표현식이 있는 컬럼의 경우, 모든 기존 행이 재작성됩니다.

새로 추가되었거나 업데이트된 `DEFAULT` 값 표현식이 있는 컬럼의 경우, 동작은 ClickHouse 버전에 따라 달라집니다:

* ClickHouse &lt; v24.2에서는 모든 기존 행이 재작성됩니다.
* ClickHouse &gt;= v24.2에서는 `DEFAULT` 값 표현식이 있는 컬럼의 행 값이 삽입 시 명시적으로 지정되었는지, 아니면 `DEFAULT` 값 표현식으로 계산되었는지를 구분합니다. 값이 명시적으로 지정된 경우 ClickHouse는 해당 값을 그대로 유지합니다. 값이 계산된 경우 ClickHouse는 그 값을 새로 추가되었거나 업데이트된 `MATERIALIZED` 값 표현식으로 변경합니다.

구문:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE COLUMN col [IN PARTITION partition | IN PARTITION ID 'partition_id'];
```

* PARTITION을 지정하면 컬럼은 지정된 파티션에 대해서만 구체화됩니다.

**예시**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM (select x,s from tmp order by x);

┌─groupArray(x)─┬─groupArray(s)─────────┐
│ [0,1,2,3,4]   │ ['0','1','2','3','4'] │
└───────────────┴───────────────────────┘

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(round(100/x));

INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5,5;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)──────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','20','17','14','12','11'] │
└───────────────────────┴────────────────────────────────────────────────┘

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['inf','100','50','33','25','20','17','14','12','11'] │
└───────────────────────┴───────────────────────────────────────────────────────┘
```

**관련 항목**

* [MATERIALIZED](/ko/sql-reference/statements/create/view#materialized-view).

<div id="limitations">
  ## 제한 사항
</div>

`ALTER` 쿼리를 사용하면 중첩 데이터 구조에서 개별 요소(컬럼)는 생성하거나 삭제할 수 있지만, 중첩 데이터 구조 전체는 생성하거나 삭제할 수 없습니다. 중첩 데이터 구조를 추가하려면 `name.nested_name`과 같은 이름과 `Array(T)` 유형을 가진 컬럼을 추가하면 됩니다. 중첩 데이터 구조는 점 앞의 접두사가 같은 이름을 가진 여러 배열 컬럼과 동일합니다.

이름에 점이 포함된 컬럼의 이름 변경은 부분적으로 지원됩니다. 점은 [Nested](/ko/sql-reference/data-types/nested-data-structures/nested) 하위 컬럼 접근용으로 예약되어 있으므로 접두사(상위 이름)는 그대로 유지되어야 합니다. 변경할 수 있는 것은 접미사(하위 컬럼 이름)뿐입니다. 예를 들어 `a.b`는 `a.c`로 이름을 변경할 수 있지만, `a.b`를 `b.d`로 변경하는 것은 Nested 상위 접두사가 바뀌므로 허용되지 않습니다.

프라이머리 키 또는 샘플링 키(`ENGINE` 표현식에서 사용되는 컬럼)의 컬럼 삭제는 지원되지 않습니다. 프라이머리 키에 포함된 컬럼의 유형 변경은, 이 변경으로 인해 데이터가 수정되지 않는 경우에만 가능합니다(예를 들어 Enum에 값을 추가하거나 `DateTime` 유형을 `UInt32`로 변경하는 것은 허용됩니다).

필요한 테이블 변경을 `ALTER` 쿼리만으로 처리할 수 없다면, 새 테이블을 생성하고 [INSERT SELECT](/ko/sql-reference/statements/insert-into.md/#inserting-the-results-of-select) 쿼리를 사용해 데이터를 복사한 다음, [RENAME](/ko/sql-reference/statements/rename.md/#rename-table) 쿼리로 테이블을 전환하고 기존 테이블을 삭제할 수 있습니다.

`ALTER` 쿼리는 해당 테이블의 모든 읽기와 쓰기를 차단합니다. 다시 말해, `ALTER` 쿼리가 실행될 때 오래 걸리는 `SELECT`가 실행 중이면 `ALTER` 쿼리는 그것이 완료될 때까지 기다립니다. 동시에 같은 테이블에 대한 모든 새 쿼리도 이 `ALTER`가 실행되는 동안 대기합니다.

직접 데이터를 저장하지 않는 테이블([Merge](/ko/sql-reference/statements/alter/index.md) 및 [Distributed](/ko/sql-reference/statements/alter/index.md) 등)의 경우, `ALTER`는 테이블 구조만 변경하며 하위 테이블의 구조는 변경하지 않습니다. 예를 들어 `Distributed` 테이블에 대해 ALTER를 실행하는 경우, 모든 remote server의 테이블에 대해서도 `ALTER`를 실행해야 합니다.