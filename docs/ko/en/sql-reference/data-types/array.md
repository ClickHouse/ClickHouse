---
description: 'ClickHouse의 배열 데이터 타입 문서'
sidebar_label: 'Array(T)'
sidebar_position: 32
slug: /sql-reference/data-types/array
title: 'Array(T)'
doc_type: 'reference'
---

배열 인덱스가 1부터 시작하는 `T` 타입 항목의 배열입니다. `T`에는 배열을 포함해 모든 데이터 타입을 사용할 수 있습니다.

<div id="creating-an-array">
  ## 배열 생성하기
</div>

함수를 사용해 배열을 생성할 수 있습니다.

```sql
array(T)
```

또한 `[]`를 사용할 수 있습니다.

```sql
[]
```

배열 생성 예시:

```sql
SELECT array(1, 2) AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

```sql
SELECT [1, 2] AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

<div id="working-with-data-types">
  ## 데이터 타입 사용하기
</div>

즉석에서 배열을 생성할 때 ClickHouse는 나열된 모든 인수를 저장할 수 있는 가장 좁은 데이터 타입으로 인수 타입을 자동 결정합니다. [널 허용](/ko/sql-reference/data-types/nullable) 또는 리터럴 [NULL](/ko/operations/settings/formats#input_format_null_as_default) 값이 하나라도 있으면 배열 요소의 타입도 [널 허용](../../sql-reference/data-types/nullable.md)이 됩니다.

ClickHouse가 데이터 타입을 결정할 수 없으면 예외를 발생시킵니다. 예를 들어 문자열과 숫자를 동시에 포함하는 배열을 생성하려고 할 때(`SELECT array(1, 'a')`) 이런 일이 발생합니다.

자동 데이터 타입 감지 예시:

```sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

```text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

서로 호환되지 않는 데이터 타입으로 배열을 생성하려고 하면 ClickHouse가 예외를 발생시킵니다:

```sql
SELECT array(1, 'a')
```

```text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

<div id="array-size">
  ## 배열 크기
</div>

전체 컬럼을 읽지 않고도 `size0` 서브컬럼을 사용해 배열의 크기를 확인할 수 있습니다. 다차원 배열에서는 `sizeN-1`을 사용할 수 있으며, 여기서 `N`은 원하는 차원입니다.

**예시**

```sql title="Query"
CREATE TABLE t_arr (`arr` Array(Array(Array(UInt32)))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_arr VALUES ([[[12, 13, 0, 1],[12]]]);

SELECT arr.size0, arr.size1, arr.size2 FROM t_arr;
```

```text title="Response"
┌─arr.size0─┬─arr.size1─┬─arr.size2─┐
│         1 │ [2]       │ [[4,1]]   │
└───────────┴───────────┴───────────┘
```

<div id="reading-nested-subcolumns-from-array">
  ## 배열에서 중첩된 서브컬럼 읽기
</div>

`Array` 내부의 중첩 유형 `T`에 서브컬럼이 있는 경우(예를 들어 [named tuple](./tuple.md)인 경우), 동일한 서브컬럼 이름으로 `Array(T)` 타입에서 해당 서브컬럼을 읽을 수 있습니다. 서브컬럼의 타입은 원래 서브컬럼 타입의 `Array`입니다.

**예시**

```sql
CREATE TABLE t_arr (arr Array(Tuple(field1 UInt32, field2 String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arr VALUES ([(1, 'Hello'), (2, 'World')]), ([(3, 'This'), (4, 'is'), (5, 'subcolumn')]);
SELECT arr.field1, toTypeName(arr.field1), arr.field2, toTypeName(arr.field2) from t_arr;
```

```test
┌─arr.field1─┬─toTypeName(arr.field1)─┬─arr.field2────────────────┬─toTypeName(arr.field2)─┐
│ [1,2]      │ Array(UInt32)          │ ['Hello','World']         │ Array(String)          │
│ [3,4,5]    │ Array(UInt32)          │ ['This','is','subcolumn'] │ Array(String)          │
└────────────┴────────────────────────┴───────────────────────────┴────────────────────────┘
```