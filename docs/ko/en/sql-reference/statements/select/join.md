---
description: 'JOIN 절 문서'
sidebar_label: 'JOIN'
slug: /sql-reference/statements/select/join
title: 'JOIN 절'
keywords: ['INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', 'RIGHT JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN', 'LEFT SEMI JOIN', 'RIGHT SEMI JOIN', 'LEFT ANTI JOIN', 'RIGHT ANTI JOIN', 'LEFT ANY JOIN', 'RIGHT ANY JOIN', 'INNER ANY JOIN', 'ASOF JOIN', 'LEFT ASOF JOIN', 'PASTE JOIN', 'NATURAL JOIN']
doc_type: 'reference'
---

`JOIN` 절은 공통된 값을 기준으로 하나 이상의 테이블에서 컬럼을 결합해 새 테이블을 생성합니다. 이는 SQL을 지원하는 데이터베이스에서 흔히 사용되는 연산으로, [관계 대수](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators)의 조인에 해당합니다. 하나의 테이블을 자기 자신과 조인하는 특수한 경우는 흔히 &quot;self-join&quot;이라고 합니다.

**구문**

```sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

`ON` 절의 표현식과 `USING` 절의 컬럼을 &quot;조인 키&quot;라고 합니다. 별도로 명시하지 않는 한, `JOIN`은 &quot;조인 키&quot;가 일치하는 행들에 대해 [카테시안 곱](https://en.wikipedia.org/wiki/Cartesian_product)을 생성하므로, 원본 테이블보다 훨씬 많은 행을 포함하는 결과가 나올 수 있습니다.

<div id="supported-types-of-join">
  ## 지원되는 JOIN 유형
</div>

모든 표준 [SQL JOIN](https://en.wikipedia.org/wiki/Join_\(SQL\)) 유형을 지원합니다.

| 유형                 | 설명                                                                                                                                                                                     |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `INNER JOIN`       | 일치하는 행만 반환됩니다.                                                                                                                                                                         |
| `LEFT OUTER JOIN`  | 일치하는 행에 더해 왼쪽 테이블의 불일치 행도 반환됩니다.                                                                                                                                                       |
| `RIGHT OUTER JOIN` | 일치하는 행에 더해 오른쪽 테이블의 불일치 행도 반환됩니다.                                                                                                                                                      |
| `FULL OUTER JOIN`  | 일치하는 행에 더해 양쪽 테이블의 불일치 행도 반환됩니다.                                                                                                                                                       |
| `CROSS JOIN`       | 전체 테이블의 카테시안 곱을 생성하며, &quot;조인 키&quot;는 지정되지 **않습니다**.                                                                                                                                 |
| `NATURAL JOIN`     | 양쪽 테이블에서 이름이 같은 모든 컬럼을 기준으로 자동으로 조인합니다. 각 공통 컬럼은 결과에 한 번만 나타납니다. `INNER`(기본값), `LEFT`, `RIGHT`, `FULL` 변형을 지원합니다. 컬럼 목록이 자동으로 도출된다는 점을 제외하면 `JOIN ... USING (col1, col2, ...)`와 동일합니다. |

* 유형을 지정하지 않은 `JOIN`은 `INNER`를 의미합니다.
* `OUTER` 키워드는 생략해도 됩니다.
* `CROSS JOIN`의 대체 구문으로는 [`FROM` 절](../../../sql-reference/statements/select/from.md)에서 여러 테이블을 쉼표로 구분해 지정하는 방식이 있습니다.
* `NATURAL JOIN`에서 일치하는 컬럼이 없으면 `CROSS JOIN`처럼 동작합니다.

ClickHouse에서 사용할 수 있는 추가 조인 유형은 다음과 같습니다.

| 유형                                                  | 설명                                                                                            |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`                 | 카테시안 곱을 생성하지 않고 &quot;조인 키&quot;에 대한 허용 목록으로 동작합니다.                                           |
| `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`                 | 카테시안 곱을 생성하지 않고 &quot;조인 키&quot;에 대한 차단 목록으로 동작합니다.                                           |
| `LEFT ANY JOIN`, `RIGHT ANY JOIN`, `INNER ANY JOIN` | 표준 `JOIN` 유형에서 카테시안 곱을 부분적으로(`LEFT` 및 `RIGHT`의 반대편에 대해) 또는 완전히(`INNER` 및 `FULL`에 대해) 비활성화합니다. |
| `ASOF JOIN`, `LEFT ASOF JOIN`                       | 정확히 일치하지 않는 조건으로 시퀀스를 조인합니다. `ASOF JOIN` 사용법은 아래에 설명되어 있습니다.                                  |
| `PASTE JOIN`                                        | 두 테이블을 가로로 연결합니다.                                                                             |

:::note
[join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm)이 `partial_merge`로 설정된 경우, `RIGHT JOIN`과 `FULL JOIN`은 `ALL` 엄격성에서만 지원됩니다(`SEMI`, `ANTI`, `ANY`, `ASOF`는 지원되지 않음).
:::

<div id="settings">
  ## 설정
</div>

기본 조인 유형은 [`join_default_strictness`](../../../operations/settings/settings.md#join_default_strictness) 설정으로 재정의할 수 있습니다.

`ANY JOIN` 작업에서 ClickHouse 서버의 동작은 [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys) 설정에 따라 달라집니다.

**관련 항목**

* [`join_algorithm`](../../../operations/settings/settings.md#join_algorithm)
* [`join_any_take_last_row`](../../../operations/settings/settings.md#join_any_take_last_row)
* [`join_use_nulls`](../../../operations/settings/settings.md#join_use_nulls)
* [`partial_merge_join_rows_in_right_blocks`](../../../operations/settings/settings.md#partial_merge_join_rows_in_right_blocks)
* [`join_on_disk_max_files_to_merge`](../../../operations/settings/settings.md#join_on_disk_max_files_to_merge)
* [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)

ClickHouse가 `CROSS JOIN`을 `INNER JOIN`으로 재작성하지 못할 때의 동작은 `cross_to_inner_join_rewrite` 설정으로 지정할 수 있습니다. 기본값은 `1`이며, 이 경우 조인은 계속 수행되지만 더 느리게 실행됩니다. 오류를 발생시키려면 `cross_to_inner_join_rewrite`를 `0`으로 설정하고, 크로스 조인을 실행하지 않고 대신 모든 쉼표 조인/크로스 조인을 강제로 재작성하려면 `2`로 설정하십시오. 값이 `2`일 때 재작성이 실패하면 &quot;Please, try to simplify `WHERE` section&quot;라는 오류 메시지가 표시됩니다.

<div id="on-section-conditions">
  ## ON 절 조건
</div>

`ON` 절에는 `AND` 및 `OR` 연산자로 결합된 여러 조건이 포함될 수 있습니다. 조인 키를 지정하는 조건은 다음을 충족해야 합니다.

* 왼쪽 테이블과 오른쪽 테이블을 모두 참조해야 합니다
* 등호 연산자를 사용해야 합니다

그 밖의 조건에는 다른 논리 연산자를 사용할 수 있지만, 쿼리의 왼쪽 테이블 또는 오른쪽 테이블 중 하나만 참조해야 합니다.

전체 복합 조건이 충족되면 행이 조인됩니다. 조건이 충족되지 않더라도 `JOIN` 유형에 따라 행이 결과에 포함될 수 있습니다. 동일한 조건을 `WHERE` 절에 두었는데 조건이 충족되지 않으면, 해당 행은 항상 결과에서 필터링된다는 점에 유의하십시오.

`ON` 절 내부의 `OR` 연산자는 해시 조인 알고리즘을 사용해 동작합니다. 즉, `JOIN`의 조인 키를 포함하는 각 `OR` 인수마다 별도의 해시 테이블이 생성되므로, `ON` 절에서 `OR` 표현식 수가 증가할수록 메모리 활용과 쿼리 실행 시간도 선형적으로 증가합니다.

:::note
조건이 서로 다른 테이블의 컬럼을 참조하는 경우, 현재는 등호 연산자(`=`)만 지원됩니다.
:::

**예시**

`table_1`과 `table_2`를 살펴보겠습니다.

```response
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1 │ A    │     │  1 │ Text A         │     10 │
│  2 │ B    │     │  1 │ Another text A │     12 │
│  3 │ C    │     │  2 │ Text B         │     15 │
└────┴──────┘     └────┴────────────────┴────────┘
```

조인 키 조건이 하나이고 `table_2`에 대한 추가 조건이 있는 쿼리:

```sql title="Query"
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

결과에는 이름이 `C`이고 텍스트 컬럼이 비어 있는 행이 포함되어 있다는 점에 유의하십시오. 이는 `OUTER` 유형의 join을 사용했기 때문에 결과에 포함된 것입니다.

```response title="Response"
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

`INNER` 유형의 join과 여러 조건을 사용한 쿼리:

```sql title="Query"
SELECT name, text, scores FROM table_1 INNER JOIN table_2
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

```sql title="Response"
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │     15 │
└──────┴────────┴────────┘
```

조인 유형이 `INNER`이고 조건에 `OR`가 있는 쿼리:

```sql title="Query"
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

조인 유형이 `INNER`이고 조건에 `OR` 및 `AND`가 포함된 쿼리:

:::note

기본적으로 부등호 조건은 동일한 테이블의 컬럼을 사용하는 경우에만 지원됩니다.
예를 들어 `t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c`는 `t1.b > 0`이 `t1`의 컬럼만 사용하고 `t2.b > t2.c`가 `t2`의 컬럼만 사용하므로 지원됩니다.
하지만 `t1.a = t2.key AND t1.b > t2.key`와 같은 조건에 대해서는 실험적 지원을 사용해 볼 수 있습니다. 자세한 내용은 아래 섹션을 참조하십시오.

:::

```sql title="Query"
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

<div id="join-with-inequality-conditions-for-columns-from-different-tables">
  ## 서로 다른 테이블의 컬럼에 대한 부등식 조건이 있는 JOIN
</div>

ClickHouse는 현재 동등 조건뿐 아니라 부등식 조건이 있는 `ALL/ANY/SEMI/ANTI INNER/LEFT/RIGHT/FULL JOIN`도 지원합니다. 부등식 조건은 `hash` 및 `grace_hash` 조인 알고리즘에서만 지원됩니다. `join_use_nulls`를 사용하는 경우에는 부등식 조건이 지원되지 않습니다.

**예시**

테이블 `t1`:

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ a    │ 1 │ 1 │ 2 │
│ key1 │ b    │ 2 │ 3 │ 2 │
│ key1 │ c    │ 3 │ 2 │ 1 │
│ key1 │ d    │ 4 │ 7 │ 2 │
│ key1 │ e    │ 5 │ 5 │ 5 │
│ key2 │ a2   │ 1 │ 1 │ 1 │
│ key4 │ f    │ 2 │ 3 │ 4 │
└──────┴──────┴───┴───┴───┘
```

테이블 `t2`

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ A    │ 1 │ 2 │ 1 │
│ key1 │ B    │ 2 │ 1 │ 2 │
│ key1 │ C    │ 3 │ 4 │ 5 │
│ key1 │ D    │ 4 │ 1 │ 6 │
│ key3 │ a3   │ 1 │ 1 │ 1 │
│ key4 │ F    │ 1 │ 1 │ 1 │
└──────┴──────┴───┴───┴───┘
```

```sql
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.a < t2.a) ORDER BY (t1.key, t1.attr, t2.key, t2.attr);
```

```response
key1    a    1    1    2    key1    B    2    1    2
key1    a    1    1    2    key1    C    3    4    5
key1    a    1    1    2    key1    D    4    1    6
key1    b    2    3    2    key1    C    3    4    5
key1    b    2    3    2    key1    D    4    1    6
key1    c    3    2    1    key1    D    4    1    6
key1    d    4    7    2            0    0    \N
key1    e    5    5    5            0    0    \N
key2    a2    1    1    1            0    0    \N
key4    f    2    3    4            0    0    \N
```

<div id="null-values-in-join-keys">
  ## JOIN 키의 NULL 값
</div>

`NULL`은 자기 자신을 포함해 어떤 값과도 같지 않습니다. 즉, 한 테이블에서 `JOIN` 키가 `NULL`이면 다른 테이블의 `NULL` 값과는 일치하지 않습니다.

**예시**

테이블 `A`:

```response
┌───id─┬─name────┐
│    1 │ Alice   │
│    2 │ Bob     │
│ ᴺᵁᴸᴸ │ Charlie │
└──────┴─────────┘
```

테이블 `B`:

```response
┌───id─┬─score─┐
│    1 │    90 │
│    3 │    85 │
│ ᴺᵁᴸᴸ │    88 │
└──────┴───────┘
```

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON A.id = B.id
```

```response
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │     0 │
└─────────┴───────┘
```

테이블 `A`에서 `Charlie`가 있는 행과 테이블 `B`에서 점수가 88인 행은 `JOIN` 키에 `NULL` 값이 있으므로 결과에 포함되지 않습니다.

`NULL` 값을 서로 일치시키려면 `JOIN` 키를 비교할 때 `isNotDistinctFrom` 함수를 사용하십시오.

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```markdown
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

<div id="asof-join-usage">
  ## ASOF JOIN 사용
</div>

`ASOF JOIN`은 정확히 일치하는 항목이 없는 레코드를 조인해야 할 때 유용합니다.

이 JOIN 알고리즘을 사용하려면 테이블에 특수한 컬럼이 필요합니다. 이 컬럼은 다음 조건을 충족해야 합니다.

* 정렬된 시퀀스를 포함해야 합니다.
* 다음 타입 중 하나여야 합니다: [Int, UInt](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md), [Decimal](../../../sql-reference/data-types/decimal.md).
* `hash` 조인 알고리즘에서는 `JOIN` 절의 유일한 컬럼이 될 수 없습니다.

구문 `ASOF JOIN ... ON`:

```sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

동등 조건은 여러 개 사용할 수 있으며, 가장 가까운 일치 조건은 정확히 1개만 사용할 수 있습니다. 예를 들어, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

가장 가까운 일치에 사용할 수 있는 조건은 다음과 같습니다: `>`, `>=`, `<`, `<=`.

구문 `ASOF JOIN ... USING`:

```sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN`은 동등 조건으로 조인할 때 `equi_columnX`를 사용하고, `table_1.asof_column >= table_2.asof_column` 조건으로 가장 가까운 값을 매칭할 때 `asof_column`을 사용합니다. `asof_column` 컬럼은 `USING` 절에서 항상 마지막에 위치합니다.

예시로, 다음 테이블을 살펴보겠습니다:

```text
         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...
```

`ASOF JOIN`은 `table_1`의 사용자 이벤트 타임스탬프를 기준으로, 가장 가까운 일치 조건을 만족하는 `table_2`의 이벤트 중 해당 `table_1` 이벤트의 타임스탬프와 가장 가까운 타임스탬프를 가진 이벤트를 찾을 수 있습니다. 동일한 타임스탬프 값이 있으면 그 값이 가장 가까운 값으로 사용됩니다. 여기서는 `user_id` 컬럼을 동등 조건으로 조인하는 데 사용할 수 있고, `ev_time` 컬럼을 가장 가까운 값 기준으로 조인하는 데 사용할 수 있습니다. 이 예시에서는 `event_1_1`을 `event_2_1`과 조인할 수 있고 `event_1_2`를 `event_2_3`과 조인할 수 있지만, `event_2_2`는 조인할 수 없습니다.

:::note
`ASOF JOIN`은 `hash` 및 `full_sorting_merge` 조인 알고리즘에서만 지원됩니다.
[Join](../../../engines/table-engines/special/join.md) 테이블 엔진에서는 **지원되지 않습니다**.
:::

<div id="paste-join-usage">
  ## PASTE JOIN 사용법
</div>

`PASTE JOIN`의 결과는 왼쪽 서브쿼리의 모든 컬럼에 이어 오른쪽 서브쿼리의 모든 컬럼이 포함된 테이블입니다.
행은 원본 테이블에서의 위치를 기준으로 매칭됩니다(행 순서는 정의되어 있어야 합니다).
서브쿼리가 반환하는 행 수가 서로 다르면 초과 행은 잘립니다.

예시:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers(2)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(2)
    ORDER BY a DESC
) AS t2

┌─a─┬─t2.a─┐
│ 0 │    1 │
│ 1 │    0 │
└───┴──────┘
```

참고: 이 경우 읽기 작업이 병렬로 수행되면 결과가 비결정적일 수 있습니다. 예시:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers_mt(5)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(10)
    ORDER BY a DESC
) AS t2
SETTINGS max_block_size = 2;

┌─a─┬─t2.a─┐
│ 2 │    9 │
│ 3 │    8 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 0 │    7 │
│ 1 │    6 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 4 │    5 │
└───┴──────┘
```

<div id="distributed-join">
  ## 분산 JOIN
</div>

분산 테이블이 포함된 JOIN을 실행하는 방법은 2가지입니다.

* 일반 `JOIN`을 사용하면 쿼리가 원격 서버로 전송됩니다. 오른쪽 테이블을 만들기 위해 각 서버에서 서브쿼리가 실행되고, 그 테이블과 조인이 수행됩니다. 즉, 오른쪽 테이블은 각 서버에서 각각 별도로 구성됩니다.
* `GLOBAL ... JOIN`을 사용하면 먼저 요청을 보낸 서버가 서브쿼리를 실행해 조인의 한쪽을 계산하고, 그 결과를 임시 테이블에 수집합니다. 그런 다음 이 임시 테이블이 각 원격 서버로 전달되고, 전송된 임시 데이터를 사용해 해당 서버들에서 쿼리가 실행됩니다. `LEFT` 및 `INNER` 조인의 경우에는 오른쪽 테이블이 서브쿼리로 계산됩니다. `RIGHT` 조인의 경우에는 대신 왼쪽 테이블이 계산되는데, 유지되는 쪽은 오른쪽 테이블이므로 세그먼트에서 읽어야 하기 때문입니다.

`GLOBAL`을 사용할 때는 주의하십시오. 자세한 내용은 [분산 서브쿼리](/ko/sql-reference/operators/in#distributed-subqueries) 섹션을 참조하십시오.

<div id="implicit-type-conversion">
  ## 암시적 타입 변환
</div>

`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN` 쿼리는 &quot;조인 키&quot;에 대해 암시적 타입 변환을 지원합니다. 그러나 왼쪽 table과 오른쪽 table의 join key를 하나의 타입으로 변환할 수 없으면 쿼리를 실행할 수 없습니다(예를 들어, `UInt64`와 `Int64`의 모든 값을 모두 담을 수 있는 데이터 타입이 없거나 `String`과 `Int32`를 함께 표현할 수 있는 단일 타입이 없는 경우).

**예시**

table `t_1`을 살펴보겠습니다:

```response
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```

그리고 `t_2` 테이블:

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int16         │ Nullable(Int64) │
│  1 │   -1 │ Int16         │ Nullable(Int64) │
│  1 │    1 │ Int16         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

쿼리

```sql
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b);
```

Set을 반환합니다:

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

<div id="usage-recommendations">
  ## 사용 권장사항
</div>

<div id="processing-of-empty-or-null-cells">
  ### 빈 셀 또는 NULL 셀 처리
</div>

테이블을 조인하는 동안 빈 셀이 생길 수 있습니다. [join&#95;use&#95;nulls](../../../operations/settings/settings.md#join_use_nulls) 설정은 ClickHouse가 이러한 셀을 어떻게 채울지 정의합니다.

`JOIN` 키가 [널 허용](../../../sql-reference/data-types/nullable.md) 필드인 경우, 키 중 하나 이상이 [NULL](/ko/sql-reference/syntax#null) 값인 행은 조인되지 않습니다.

<div id="syntax">
  ### 구문
</div>

`USING`에 지정된 컬럼은 두 서브쿼리에서 이름이 같아야 하며, 나머지 컬럼은 서로 다른 이름이어야 합니다. 서브쿼리의 컬럼 이름은 alias를 사용해 변경할 수 있습니다.

`USING` 절은 join할 하나 이상의 컬럼을 지정하며, 이를 통해 해당 컬럼들이 서로 같다고 간주합니다. 컬럼 목록은 괄호 없이 지정합니다. 더 복잡한 join 조건은 지원하지 않습니다.

<div id="syntax-limitations">
  ### 구문 제한 사항
</div>

하나의 `SELECT` 쿼리에서 여러 `JOIN` 절을 사용하는 경우:

* `*`로 모든 컬럼을 가져오는 기능은 테이블을 조인한 경우에만 사용할 수 있으며, 서브쿼리에서는 사용할 수 없습니다.
* `PREWHERE` 절은 사용할 수 없습니다.
* `USING` 절은 사용할 수 없습니다.

`ON`, `WHERE`, `GROUP BY` 절에 대해서는 다음과 같습니다.

* `ON`, `WHERE`, `GROUP BY` 절에서는 임의의 표현식을 사용할 수 없습니다. 다만 `SELECT` 절에서 표현식을 정의한 뒤 별칭으로 이러한 절에서 사용할 수 있습니다.

<div id="performance">
  ### 성능
</div>

`JOIN`을 실행할 때는 쿼리의 다른 단계와 관련해 실행 순서가 최적화되지 않습니다. join(오른쪽 테이블에서의 검색)은 `WHERE`의 필터링과 집계보다 먼저 실행됩니다.

동일한 `JOIN`으로 쿼리를 실행할 때마다 결과가 캐시되지 않으므로 서브쿼리가 매번 다시 실행됩니다. 이를 방지하려면 조인용으로 미리 준비된 배열이며 항상 RAM에 상주하는 특수한 [Join](../../../engines/table-engines/special/join.md) 테이블 엔진을 사용하십시오.

경우에 따라 `JOIN` 대신 [IN](../../../sql-reference/operators/in.md)을 사용하는 것이 더 효율적입니다.

차원 테이블과 조인하기 위해 `JOIN`이 필요한 경우(차원 테이블은 광고 캠페인 이름과 같은 차원 속성을 포함하는 비교적 작은 테이블입니다), 쿼리마다 오른쪽 테이블에 다시 접근해야 하므로 `JOIN`이 그다지 편리하지 않을 수 있습니다. 이런 경우에는 `JOIN` 대신 사용해야 하는 &quot;딕셔너리&quot; 기능이 있습니다. 자세한 내용은 [Dictionaries](/ko/sql-reference/statements/create/dictionary/overview.md) 섹션을 참조하십시오.

<div id="memory-limitations">
  ### 메모리 제한
</div>

기본적으로 ClickHouse는 [해시 조인](https://en.wikipedia.org/wiki/Hash_join) 알고리즘을 사용합니다. ClickHouse는 `right_table`을 가져와 이를 위한 해시 테이블을 RAM에 생성합니다. `join_algorithm = 'auto'`가 활성화되어 있으면 메모리 사용량이 일정 임계값을 넘은 뒤 ClickHouse는 [머지](https://en.wikipedia.org/wiki/Sort-merge_join) 조인 알고리즘으로 전환합니다. `JOIN` 알고리즘에 대한 설명은 [join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm) 설정을 참조하십시오.

`JOIN` 작업의 메모리 사용량을 제한해야 하는 경우 다음 설정을 사용하십시오:

* [max&#95;rows&#95;in&#95;join](/ko/operations/settings/settings#max_rows_in_join) — 해시 테이블의 행 수를 제한합니다.
* [max&#95;bytes&#95;in&#95;join](/ko/operations/settings/settings#max_bytes_in_join) — 해시 테이블의 크기를 제한합니다.

이 제한 중 하나에 도달하면 ClickHouse는 [join&#95;overflow&#95;mode](/ko/operations/settings/settings#join_overflow_mode)
설정에 지정된 방식으로 동작합니다.

<div id="examples">
  ## 예시
</div>

예시:

```sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

```text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse: 완전한 SQL 조인을 지원하는 초고속 DBMS - Part 1](https://clickhouse.com/blog/clickhouse-fully-supports-joins)
* 블로그: [ClickHouse: 완전한 SQL 조인을 지원하는 초고속 DBMS - 내부 동작 원리 - Part 2](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)
* 블로그: [ClickHouse: 완전한 SQL 조인을 지원하는 초고속 DBMS - 내부 동작 원리 - Part 3](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3)
* 블로그: [ClickHouse: 완전한 SQL 조인을 지원하는 초고속 DBMS - 내부 동작 원리 - Part 4](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)