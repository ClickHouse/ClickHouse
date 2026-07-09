---
description: 'ClickHouse의 널 허용 데이터 타입 수정자에 대한 문서'
sidebar_label: 'Nullable(T)'
sidebar_position: 44
slug: /sql-reference/data-types/nullable
title: 'Nullable(T)'
doc_type: 'reference'
---

`T`가 허용하는 일반 값과 함께 &quot;누락된 값&quot;을 나타내는 특수 마커([NULL](../../sql-reference/syntax.md))를 저장할 수 있습니다. 예를 들어 `Nullable(Int8)` 타입의 컬럼은 `Int8` 타입 값을 저장할 수 있으며, 값이 없는 행에는 `NULL`이 저장됩니다.

`T`는 다음 복합 데이터 타입 중 어느 것도 될 수 없습니다.

* [Array](../../sql-reference/data-types/array.md) — 지원되지 않음
* [Map](../../sql-reference/data-types/map.md) — 지원되지 않음
* [Tuple](../../sql-reference/data-types/tuple.md) — 베타 지원 제공*

하지만 복합 데이터 타입은 `Nullable` 타입 값을 **포함할 수 있습니다**. 예를 들어 `Array(Nullable(Int8))` 또는 `Tuple(Nullable(String), Nullable(Int64))`가 가능합니다.

:::note 베타: 널 허용 튜플

* [Nullable(Tuple(...))](../../sql-reference/data-types/tuple.md#nullable-tuple)은 `enable_nullable_tuple_type = 1`이 활성화된 경우 지원됩니다.
  :::

`Nullable` 타입 필드는 테이블 인덱스에 포함할 수 없습니다.

ClickHouse 서버 구성에서 달리 지정하지 않는 한, 모든 `Nullable` 타입의 기본값은 `NULL`입니다.

<div id="storage-features">
  ## 스토리지 기능
</div>

테이블 컬럼에 `Nullable` 타입의 값을 저장할 때 ClickHouse는 값이 들어 있는 일반 파일과 함께 `NULL` 마스크가 저장된 별도의 파일을 사용합니다. 마스크 파일의 항목을 통해 ClickHouse는 각 테이블 행에서 `NULL`과 해당 데이터 타입의 기본값을 구분할 수 있습니다. 별도 파일이 추가로 필요하므로 `Nullable` 컬럼은 유사한 일반 컬럼보다 더 많은 스토리지 공간을 사용합니다.

:::note
`Nullable`을 사용하면 거의 항상 성능에 부정적인 영향을 미칩니다. 데이터베이스를 설계할 때 이 점을 염두에 두십시오.
:::

<div id="finding-null">
  ## NULL 찾기
</div>

전체 컬럼을 읽지 않고도 `null` 하위 컬럼을 사용해 컬럼에서 `NULL` 값을 찾을 수 있습니다. 해당 값이 `NULL`이면 `1`을 반환하고, 그렇지 않으면 `0`을 반환합니다.

**예시**

```sql title="Query"
CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nullable VALUES (1) (NULL) (2) (NULL);

SELECT n.null FROM nullable;
```

```text title="Response"
┌─n.null─┐
│      0 │
│      1 │
│      0 │
│      1 │
└────────┘
```

<div id="usage-example">
  ## 사용 예시
</div>

```sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

```sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

```sql
SELECT x + y FROM t_null
```

```text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```