---
description: '이름이 지정된 상수 값들의 집합을 나타내는 ClickHouse의 Enum 데이터 타입에 대한 문서'
sidebar_label: 'Enum'
sidebar_position: 20
slug: /sql-reference/data-types/enum
title: 'Enum'
doc_type: 'reference'
---

이름이 지정된 값들로 이루어진 열거형 타입입니다.

이름이 지정된 값은 `'string' = integer` 쌍 또는 `'string'` 이름으로 선언할 수 있습니다. ClickHouse는 실제로 숫자만 저장하지만, 이름을 사용해 해당 값으로 연산할 수 있습니다.

ClickHouse는 다음을 지원합니다.

* 8비트 `Enum`. `[-128, 127]` 범위의 값으로 최대 256개까지 정의할 수 있습니다.
* 16비트 `Enum`. `[-32768, 32767]` 범위의 값으로 최대 65536개까지 정의할 수 있습니다.

ClickHouse는 데이터가 삽입될 때 `Enum` 타입을 자동으로 선택합니다. 저장 크기를 명확히 지정하려면 `Enum8` 또는 `Enum16` 타입을 사용할 수도 있습니다.

<div id="usage-examples">
  ## 사용 예시
</div>

다음은 `Enum8('hello' = 1, 'world' = 2)` 유형의 컬럼이 있는 테이블을 생성하는 예시입니다:

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

마찬가지로 숫자는 생략할 수 있습니다. ClickHouse가 연속된 번호를 자동으로 할당합니다. 번호는 기본적으로 1부터 할당됩니다.

```sql
CREATE TABLE t_enum
(
    x Enum('hello', 'world')
)
ENGINE = TinyLog
```

첫 번째 name의 유효한 시작 번호를 지정할 수도 있습니다.

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world')
)
ENGINE = TinyLog
```

```sql
CREATE TABLE t_enum
(
    x Enum8('hello' = -129, 'world')
)
ENGINE = TinyLog
```

```text
Exception on server:
Code: 69. DB::Exception: Value -129 for element 'hello' exceeds range of Enum8.
```

컬럼 `x`에는 형식 정의에 나열된 값, 즉 `'hello'` 또는 `'world'`만 저장할 수 있습니다. 그 외의 값을 저장하려고 하면 ClickHouse에서 예외가 발생합니다. 이 `Enum`에는 8비트 크기가 자동으로 선택됩니다.

```sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

```text
Ok.
```

```sql
INSERT INTO t_enum VALUES('a')
```

```text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

테이블의 데이터를 쿼리하면 ClickHouse는 `Enum`의 문자열 값을 출력합니다.

```sql
SELECT * FROM t_enum
```

```text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

행의 숫자형 값을 확인하려면 `Enum` 값을 정수 유형으로 형변환해야 합니다.

```sql
SELECT CAST(x, 'Int8') FROM t_enum
```

```text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

쿼리에서 Enum 값을 생성하려면 `CAST`도 함께 사용해야 합니다.

```sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

```text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

<div id="general-rules-and-usage">
  ## 일반 규칙 및 사용법
</div>

각 값에는 `Enum8`의 경우 `-128 ... 127` 범위의 숫자, `Enum16`의 경우 `-32768 ... 32767` 범위의 숫자가 할당됩니다. 모든 문자열과 숫자 값은 서로 달라야 합니다. 빈 문자열은 허용됩니다. 이 타입이 지정된 경우(테이블 정의에서) 숫자는 임의의 순서로 지정할 수 있습니다. 하지만 순서는 중요하지 않습니다.

`Enum`의 문자열 값이나 숫자 값은 [NULL](../../sql-reference/syntax.md)일 수 없습니다.

`Enum`은 [Nullable](../../sql-reference/data-types/nullable.md) 타입에 포함될 수 있습니다. 따라서 쿼리를 사용해 테이블을 생성하면

```sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

`'hello'`와 `'world'`뿐 아니라 `NULL`도 저장할 수 있습니다.

```sql
INSERT INTO t_enum_nullable VALUES('hello'),('world'),(NULL)
```

RAM에서는 `Enum` 컬럼이 해당 숫자값의 `Int8` 또는 `Int16`과 같은 방식으로 저장됩니다.

텍스트 형식으로 읽을 때 ClickHouse는 값을 문자열로 파싱하고 Enum 값 집합에서 해당 문자열을 찾습니다. 찾지 못하면 예외가 발생합니다. 텍스트 포맷으로 읽을 때는 문자열을 읽은 뒤 해당 숫자값을 조회합니다. 찾지 못하면 예외가 발생합니다.
텍스트 형식으로 쓸 때는 값을 해당 문자열로 기록합니다. 컬럼 데이터에 잘못된 값(유효한 집합에 속하지 않는 숫자)이 포함되어 있으면 예외가 발생합니다. 바이너리 형식으로 읽고 쓸 때는 `Int8` 및 `Int16` 데이터 타입과 동일하게 동작합니다.
암시적 기본값은 숫자가 가장 작은 값입니다.

`ORDER BY`, `GROUP BY`, `IN`, `DISTINCT` 등에서 Enum은 해당 숫자와 동일하게 동작합니다. 예를 들어 `ORDER BY`는 숫자 기준으로 정렬합니다. 같음 및 비교 연산자도 Enum에서 내부 숫자값과 동일하게 동작합니다.

Enum 값은 숫자와 비교할 수 없습니다. Enum은 상수 문자열과 비교할 수 있습니다. 비교 대상 문자열이 해당 Enum의 유효한 값이 아니면 예외가 발생합니다. IN 연산자는 왼쪽에 Enum이 있고 오른쪽에 문자열 집합이 있는 경우 지원됩니다. 이 문자열들은 해당 Enum의 값입니다.

대부분의 숫자 및 문자열 연산은 Enum 값에 대해 정의되어 있지 않습니다. 예를 들어 Enum에 숫자를 더하거나 Enum에 문자열을 연결하는 연산은 지원되지 않습니다.
하지만 Enum에는 문자열 값을 반환하는 기본 `toString` 함수가 있습니다.

Enum 값은 `toT` 함수를 사용해 숫자 타입으로도 변환할 수 있으며, 여기서 T는 숫자 타입입니다. T가 enum의 기반 숫자 타입과 일치하면 이 변환에는 추가 비용이 들지 않습니다.
값 집합만 변경되는 경우 Enum 타입은 ALTER를 사용해 비용 없이 변경할 수 있습니다. ALTER를 사용하면 Enum의 멤버를 추가하거나 제거할 수 있습니다(제거는 해당 값이 테이블에서 한 번도 사용된 적이 없는 경우에만 안전합니다). 안전장치로서, 이전에 정의된 Enum 멤버의 숫자값을 변경하면 예외가 발생합니다.

ALTER를 사용하면 Int8을 Int16으로 변경하는 것처럼 Enum8을 Enum16으로 또는 그 반대로 변경할 수 있습니다.

<div id="add-enum-values">
  ## ENUM 값 추가
</div>

ALTER [MODIFY COLUMN ADD ENUM VALUES](../../sql-reference/statements/alter/column.md#modify-column-add-enum-values)를 사용하면 enum에 새 값을 간편하게 추가할 수 있습니다.

```sql
CREATE TABLE enum
(
    x Enum('One' = 1, 'Two', 'Three')
) ENGINE = Memory;
ALTER TABLE enum MODIFY COLUMN x ADD ENUM VALUES ('Zero' = 0, 'Four' = 4);
SHOW CREATE TABLE enum;
```

```text
┌─statement────────────────────────────────────────────────────────────────┐
│CREATE TABLE default.enum                                                 │
│(                                                                         │
│    `x` Enum8('Zero' = 0, 'One' = 1, 'Two' = 2, 'Three' = 3, 'Four' = 4)  │
│)                                                                         │
│ENGINE = Memory                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```