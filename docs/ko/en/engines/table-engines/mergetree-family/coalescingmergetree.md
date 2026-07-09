---
description: 'CoalescingMergeTree는 MergeTree 엔진을 상속합니다. 핵심 기능은 파트 병합 중 각 컬럼의 마지막 NULL이 아닌 값을 자동으로 저장하는 것입니다.'
sidebar_label: 'CoalescingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/coalescingmergetree
title: 'CoalescingMergeTree 테이블 엔진'
keywords: ['CoalescingMergeTree']
show_related_blogs: true
doc_type: 'reference'
---

:::note 25.6 버전부터 사용 가능
이 테이블 엔진은 OSS와 Cloud 모두에서 25.6 이상 버전부터 사용할 수 있습니다.
:::

이 엔진은 [MergeTree](/ko/engines/table-engines/mergetree-family/mergetree)를 상속합니다. 핵심적인 차이는 데이터 파트가 병합되는 방식에 있습니다. `CoalescingMergeTree` 테이블에서 ClickHouse는 동일한 프라이머리 키(더 정확히는 동일한 [정렬 키(sorting key)](../../../engines/table-engines/mergetree-family/mergetree.md))를 가진 모든 행을, 각 컬럼의 가장 최근 NULL이 아닌 값을 담은 단일 행으로 대체합니다.

이를 통해 컬럼 수준 업서트가 가능하므로 전체 행이 아니라 특정 컬럼만 업데이트할 수 있습니다.

`CoalescingMergeTree`는 키가 아닌 컬럼에서 널 허용(Nullable) 타입과 함께 사용하도록 설계되었습니다. 컬럼이 Nullable이 아니면 동작은 [ReplacingMergeTree](/ko/engines/table-engines/mergetree-family/replacingmergetree)와 동일합니다.

<div id="creating-a-table">
  ## 테이블 생성하기
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CoalescingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

요청 매개변수에 대한 자세한 설명은 [요청 설명](../../../sql-reference/statements/create/table.md)을 참조하십시오.

<div id="parameters-of-coalescingmergetree">
  ### CoalescingMergeTree 매개변수
</div>

<div id="columns">
  #### 컬럼
</div>

`columns` - 선택 사항입니다. 값이 병합될 컬럼 이름으로 이루어진 튜플입니다. 지정된 컬럼은 파티션 또는 정렬 키(sorting key)에 포함되면 안 됩니다. `columns`를 지정하지 않으면 ClickHouse는 정렬 키에 포함되지 않은 모든 컬럼의 값을 병합합니다.

<div id="query-clauses">
  ### 쿼리 절
</div>

`CoalescingMergeTree` 테이블을 생성할 때 필요한 [절](../../../engines/table-engines/mergetree-family/mergetree.md)은 `MergeTree` 테이블을 생성할 때와 동일합니다.

<details markdown="1">
  <summary>더 이상 권장되지 않는 테이블 생성 방법</summary>

  :::note
  새 프로젝트에서는 이 메서드를 사용하지 말고, 가능하다면 기존 프로젝트도 위에서 설명한 메서드로 전환하십시오.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] CoalescingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  `columns`를 제외한 모든 매개변수는 `MergeTree`와 동일한 의미를 가집니다.

  * `columns` — 값을 합산할 컬럼 이름의 튜플입니다. 선택적 매개변수입니다. 자세한 설명은 위 내용을 참조하십시오.
</details>

<div id="usage-example">
  ## 사용 예시
</div>

다음 테이블을 예로 들어보겠습니다:

```sql
CREATE TABLE test_table
(
    key UInt64,
    value_int Nullable(UInt32),
    value_string Nullable(String),
    value_date Nullable(Date)
)
ENGINE = CoalescingMergeTree()
ORDER BY key
```

여기에 데이터를 삽입하세요:

```sql
INSERT INTO test_table VALUES(1, NULL, NULL, '2025-01-01'), (2, 10, 'test', NULL);
INSERT INTO test_table VALUES(1, 42, 'win', '2025-02-01');
INSERT INTO test_table(key, value_date) VALUES(2, '2025-02-01');
```

결과는 다음과 같습니다:

```sql
SELECT * FROM test_table ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   1 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-01-01 │
│   2 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-02-01 │
│   2 │        10 │ test         │       ᴺᵁᴸᴸ │
└─────┴───────────┴──────────────┴────────────┘
```

정확한 최종 결과를 얻기 위한 권장 쿼리:

```sql
SELECT * FROM test_table FINAL ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   2 │        10 │ test         │ 2025-02-01 │
└─────┴───────────┴──────────────┴────────────┘
```

`FINAL` 수정자를 사용하면 ClickHouse가 쿼리 시점에 머지 로직을 적용하므로 각 컬럼의 올바르게 합쳐진 &quot;최신&quot; 값을 얻을 수 있습니다. 이는 CoalescingMergeTree 테이블을 쿼리할 때 가장 안전하고 정확한 방법입니다.

:::note

기반이 되는 파트가 아직 완전히 머지되지 않았다면 `GROUP BY`를 사용하는 방식은 잘못된 결과를 반환할 수 있습니다.

```sql
SELECT key, last_value(value_int), last_value(value_string), last_value(value_date)  FROM test_table GROUP BY key; -- Not recommended.
```

:::

<div id="tuple-element-aggregation">
  ## Tuple 요소 집계
</div>

`allow_tuple_element_aggregation` 설정이 활성화되면 `Tuple` 컬럼은 각 리프 요소가 coalescing에 독립적으로 참여할 수 있도록 재귀적으로 평탄화됩니다. 이렇게 하면 여러 필드를 하나의 `Tuple` 컬럼에 저장하면서, 머지 중에 각 요소별로 coalescing되도록 할 수 있습니다. 각 `Nullable` 서브컬럼은 각각 최신 non-NULL 값을 유지합니다.

평탄화된 서브컬럼에도 일반 컬럼과 동일한 규칙이 적용됩니다:

* 정렬 키 또는 partition key의 `Tuple`에 속하는 서브컬럼은 coalescing 대상에서 제외됩니다.
* `columns`를 지정한 경우, 나열된 `Tuple` 컬럼의 서브컬럼만 coalescing됩니다.

:::note
이 설정은 변경할 수 없으므로 테이블 생성 시에 지정해야 합니다.
:::

```sql
CREATE TABLE coalescing_tuples
(
    key UInt64,
    data Tuple(
        value_a Nullable(UInt64),
        value_b Nullable(String),
        nested Tuple(
            value_c Nullable(UInt64)
        )
    )
) ENGINE = CoalescingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO coalescing_tuples VALUES (1, (100, NULL, (NULL)));
INSERT INTO coalescing_tuples VALUES (1, (NULL, 'hello', (42)));

SELECT key, data.value_a, data.value_b, data.nested.value_c FROM coalescing_tuples FINAL;
```

```text
┌─key─┬─data.value_a─┬─data.value_b─┬─data.nested.value_c─┐
│   1 │          100 │ hello        │                  42 │
└─────┴──────────────┴──────────────┴─────────────────────┘
```