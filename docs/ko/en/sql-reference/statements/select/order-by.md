---
description: 'ORDER BY 절 문서'
sidebar_label: 'ORDER BY'
slug: /sql-reference/statements/select/order-by
title: 'ORDER BY 절'
doc_type: 'reference'
---

`ORDER BY` 절에는 다음이 포함될 수 있습니다.

* 표현식 목록(예: `ORDER BY visits, search_phrase`),
* `SELECT` 절의 컬럼을 가리키는 숫자 목록(예: `ORDER BY 2, 1`), 또는
* `SELECT` 절의 모든 컬럼을 의미하는 `ALL`(예: `ORDER BY ALL`)

컬럼 번호 기준 정렬을 비활성화하려면 설정 [enable&#95;positional&#95;arguments](/ko/operations/settings/settings#enable_positional_arguments) = 0으로 지정하십시오.
`ALL` 기준 정렬을 비활성화하려면 설정 [enable&#95;order&#95;by&#95;all](/ko/operations/settings/settings#enable_order_by_all) = 0으로 지정하십시오.

`ORDER BY` 절에는 정렬 방향을 결정하는 `DESC`(내림차순) 또는 `ASC`(오름차순) 수정자를 지정할 수 있습니다.
명시적으로 정렬 순서를 지정하지 않으면 기본적으로 `ASC`가 사용됩니다.
정렬 방향은 전체 목록이 아니라 개별 표현식에 적용됩니다. 예: `ORDER BY Visits DESC, SearchPhrase`.
또한 정렬은 대소문자를 구분하여 수행됩니다.

정렬 표현식 값이 동일한 행은 임의의 비결정적 순서로 반환됩니다.
`SELECT` SQL 문에서 `ORDER BY` 절을 생략한 경우에도 행 순서는 임의의 비결정적 순서가 됩니다.

<div id="sorting-of-special-values">
  ## 특수 값 정렬
</div>

`NaN` 및 `NULL`의 정렬 순서는 두 가지 방식으로 처리됩니다:

* 기본값 또는 `NULLS LAST` 수정자를 사용하는 경우: 값이 먼저 오고, 그다음 `NaN`, 마지막으로 `NULL`이 옵니다.
* `NULLS FIRST` 수정자를 사용하는 경우: `NULL`이 먼저 오고, 그다음 `NaN`, 마지막으로 다른 값이 옵니다.

<div id="example">
  ### 예시
</div>

테이블(table)에 대해

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

쿼리 `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST`를 실행하면 다음과 같은 결과를 얻습니다:

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

부동소수점 수를 정렬하면 NaN은 다른 값과 구분됩니다. 정렬 순서와 관계없이 NaN은 항상 맨 끝에 위치합니다. 즉, 오름차순으로 정렬할 때는 다른 모든 수보다 큰 것처럼 배치되고, 내림차순으로 정렬할 때는 나머지 수보다 작은 것처럼 배치됩니다.

<div id="collation-support">
  ## Collation 지원
</div>

[String](../../../sql-reference/data-types/string.md) 값으로 정렬할 때는 collation(비교 규칙)을 지정할 수 있습니다. 예시: `ORDER BY SearchPhrase COLLATE 'tr'` - 문자열이 UTF-8로 인코딩되어 있다고 가정할 때, 터키어 알파벳을 사용해 키워드를 오름차순으로 정렬하며 대소문자는 구분하지 않습니다. `COLLATE`는 ORDER BY의 각 표현식마다 독립적으로 지정할 수도 있고 생략할 수도 있습니다. `ASC` 또는 `DESC`를 지정하는 경우 `COLLATE`는 그 뒤에 지정합니다. `COLLATE`를 사용하면 정렬은 항상 대소문자를 구분하지 않습니다.

Collate는 [LowCardinality](../../../sql-reference/data-types/lowcardinality.md), [널 허용](../../../sql-reference/data-types/nullable.md), [배열](../../../sql-reference/data-types/array.md), [Tuple](../../../sql-reference/data-types/tuple.md)에서 지원됩니다.

`COLLATE`를 사용한 정렬은 바이트 기준의 일반 정렬보다 효율이 낮으므로, 최종적으로 소수의 행만 정렬할 때 `COLLATE`를 사용하는 것이 좋습니다.

<div id="collation-examples">
  ## Collation 예시
</div>

[String](../../../sql-reference/data-types/string.md) 값만 사용하는 예시:

입력 테이블:

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ABC  │
│ 3 │ 123a │
│ 4 │ abc  │
│ 5 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 3 │ 123a │
│ 4 │ abc  │
│ 2 │ ABC  │
│ 1 │ bca  │
│ 5 │ BCA  │
└───┴──────┘
```

[널 허용](../../../sql-reference/data-types/nullable.md) 예시:

입력 테이블:

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │ ABC  │
│ 4 │ 123a │
│ 5 │ abc  │
│ 6 │ ᴺᵁᴸᴸ │
│ 7 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 4 │ 123a │
│ 5 │ abc  │
│ 3 │ ABC  │
│ 1 │ bca  │
│ 7 │ BCA  │
│ 6 │ ᴺᵁᴸᴸ │
│ 2 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

[배열](../../../sql-reference/data-types/array.md)을 사용한 예시:

입력 테이블:

```text
┌─x─┬─s─────────────┐
│ 1 │ ['Z']         │
│ 2 │ ['z']         │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 7 │ ['']          │
└───┴───────────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s─────────────┐
│ 7 │ ['']          │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 2 │ ['z']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 1 │ ['Z']         │
└───┴───────────────┘
```

[LowCardinality](../../../sql-reference/data-types/lowcardinality.md) 문자열 예시:

입력 테이블:

```response
┌─x─┬─s───┐
│ 1 │ Z   │
│ 2 │ z   │
│ 3 │ a   │
│ 4 │ A   │
│ 5 │ za  │
│ 6 │ zaa │
│ 7 │     │
└───┴─────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───┐
│ 7 │     │
│ 3 │ a   │
│ 4 │ A   │
│ 2 │ z   │
│ 1 │ Z   │
│ 5 │ za  │
│ 6 │ zaa │
└───┴─────┘
```

[Tuple](../../../sql-reference/data-types/tuple.md) 사용 예시:

```response title="Response"
┌─x─┬─s───────┐
│ 1 │ (1,'Z') │
│ 2 │ (1,'z') │
│ 3 │ (1,'a') │
│ 4 │ (2,'z') │
│ 5 │ (1,'A') │
│ 6 │ (2,'Z') │
│ 7 │ (2,'A') │
└───┴─────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───────┐
│ 3 │ (1,'a') │
│ 5 │ (1,'A') │
│ 2 │ (1,'z') │
│ 1 │ (1,'Z') │
│ 7 │ (2,'A') │
│ 4 │ (2,'z') │
│ 6 │ (2,'Z') │
└───┴─────────┘
```

<div id="implementation-details">
  ## 구현 세부 사항
</div>

`ORDER BY`와 함께 충분히 작은 [LIMIT](../../../sql-reference/statements/select/limit.md)를 지정하면 RAM 사용량을 줄일 수 있습니다. 그렇지 않으면 메모리 사용량은 정렬할 데이터 양에 비례합니다. 분산 쿼리 처리에서는 [GROUP BY](/ko/sql-reference/statements/select/group-by)를 생략할 경우 원격 서버에서 정렬을 일부 수행하고, 그 결과를 요청 서버에서 머지합니다. 즉, 분산 정렬에서는 정렬해야 할 데이터 양이 단일 서버의 메모리 용량보다 클 수 있습니다.

RAM이 충분하지 않으면 외부 메모리를 사용해 정렬할 수 있습니다(디스크에 임시 파일 생성). 이 경우 `max_bytes_before_external_sort` 설정을 사용합니다. 이 값이 0(기본값)이면 외부 정렬은 비활성화됩니다. 이 기능을 활성화하면 정렬할 데이터 양이 지정된 바이트 수에 도달했을 때 수집된 데이터를 정렬하여 임시 파일에 덤프합니다. 모든 데이터를 읽은 후에는 정렬된 파일을 모두 머지하고 결과를 출력합니다. 파일은 구성의 `/var/lib/clickhouse/tmp/` 디렉터리에 기록됩니다(기본값이며, `tmp_path` 매개변수를 사용해 이 설정을 변경할 수 있습니다). 또한 쿼리가 메모리 한도를 초과하는 경우에만 디스크로 스필하도록 설정할 수도 있습니다. 예를 들어 `max_bytes_ratio_before_external_sort=0.6`으로 설정하면 쿼리가 메모리 한도(사용자/서버)의 `60%`에 도달한 경우에만 디스크 스필이 활성화됩니다.

쿼리를 실행할 때는 `max_bytes_before_external_sort`보다 더 많은 메모리를 사용할 수 있습니다. 따라서 이 설정값은 `max_memory_usage`보다 상당히 작아야 합니다. 예를 들어 서버에 128 GB RAM이 있고 단일 쿼리 하나를 실행해야 한다면 `max_memory_usage`는 100 GB로, `max_bytes_before_external_sort`는 80 GB로 설정하십시오.

외부 정렬은 RAM에서 수행하는 정렬보다 훨씬 비효율적입니다.

<div id="optimization-of-data-reading">
  ## 데이터 읽기 최적화
</div>

`ORDER BY` 표현식에 테이블 정렬 키와 일치하는 접두사가 있으면 [optimize&#95;read&#95;in&#95;order](../../../operations/settings/settings.md#optimize_read_in_order) 설정을 사용해 쿼리를 최적화할 수 있습니다.

`optimize_read_in_order` 설정이 활성화되면 ClickHouse 서버는 테이블 인덱스를 사용해 `ORDER BY` 키 순서대로 데이터를 읽습니다. 이렇게 하면 [LIMIT](../../../sql-reference/statements/select/limit.md)가 지정된 경우 모든 데이터를 읽지 않아도 됩니다. 따라서 LIMIT가 작은 대용량 데이터 쿼리를 더 빠르게 처리할 수 있습니다.

이 최적화는 `ASC`와 `DESC` 모두에서 작동하지만, [GROUP BY](/ko/sql-reference/statements/select/group-by) 절 및 [FINAL](/ko/sql-reference/statements/select/from#final-modifier) 수정자와 함께는 사용할 수 없습니다.

`optimize_read_in_order` 설정이 비활성화되면 ClickHouse 서버는 `SELECT` 쿼리를 처리할 때 테이블 인덱스를 사용하지 않습니다.

`ORDER BY` 절이 있고 `LIMIT`가 크며, 조회 대상 데이터를 찾기 전에 매우 많은 레코드를 읽어야 하는 [WHERE](../../../sql-reference/statements/select/where.md) 조건이 있는 쿼리를 실행할 때는 `optimize_read_in_order`를 수동으로 비활성화하는 것이 좋습니다.

이 최적화는 다음 테이블 엔진에서 지원됩니다:

* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) ([구체화된 뷰(Materialized View)](/ko/sql-reference/statements/create/view#materialized-view) 포함),
* [Merge](../../../engines/table-engines/special/merge.md),
* [Buffer](../../../engines/table-engines/special/buffer.md)

`MaterializedView` 엔진 테이블에서는 `SELECT ... FROM merge_tree_table ORDER BY pk`와 같은 뷰에서 이 최적화가 작동합니다. 하지만 뷰 쿼리에 `ORDER BY` 절이 없으면 `SELECT ... FROM view ORDER BY pk`와 같은 쿼리에서는 지원되지 않습니다.

<div id="order-by-expr-with-fill-modifier">
  ## ORDER BY Expr WITH FILL 수정자
</div>

이 수정자는 [LIMIT ... WITH TIES 수정자](/ko/sql-reference/statements/select/limit#limit--with-ties-modifier)와 함께 사용할 수도 있습니다.

`WITH FILL` 수정자는 선택적 매개변수 `FROM expr`, `TO expr`, `STEP expr`와 함께 `ORDER BY expr` 뒤에 지정할 수 있습니다.
`expr` 컬럼에서 누락된 모든 값은 순차적으로 채워지며, 다른 컬럼은 기본값으로 채워집니다.

여러 컬럼을 채우려면 `ORDER BY` 절에서 각 컬럼 이름 뒤에 선택적 매개변수와 함께 `WITH FILL` 수정자를 추가하십시오.

```sql title="Query"
ORDER BY expr [WITH FILL] [FROM const_expr] [TO const_expr] [STEP const_numeric_expr] [STALENESS const_numeric_expr], ... exprN [WITH FILL] [FROM expr] [TO expr] [STEP numeric_expr] [STALENESS numeric_expr]
[INTERPOLATE [(col [AS expr], ... colN [AS exprN])]]
```

`WITH FILL`은 Numeric(모든 종류의 float, decimal, int) 또는 Date/DateTime 타입의 필드에 적용할 수 있습니다. `String` 필드에 적용하면 누락된 값은 빈 문자열로 채워집니다.
`FROM const_expr`가 정의되지 않으면 채우기 시퀀스에 `ORDER BY`의 `expr` 필드 최솟값이 사용됩니다.
`TO const_expr`가 정의되지 않으면 채우기 시퀀스에 `ORDER BY`의 `expr` 필드 최댓값이 사용됩니다.
`STEP const_numeric_expr`가 정의되면 `const_numeric_expr`는 numeric 타입에서는 `as is` 그대로, Date 타입에서는 `days`로, DateTime 타입에서는 `seconds`로 해석됩니다. 또한 시간 및 날짜 인터벌을 나타내는 [INTERVAL](/ko/sql-reference/data-types/special-data-types/interval/) 데이터 타입도 지원합니다.
`STEP const_numeric_expr`가 생략되면 채우기 시퀀스에는 numeric 타입의 경우 `1.0`, Date 타입의 경우 `1 day`, DateTime 타입의 경우 `1 second`가 사용됩니다.
`STALENESS const_numeric_expr`가 정의되면 쿼리는 원본 데이터에서 이전 행과의 차이가 `const_numeric_expr`를 초과할 때까지 행을 생성합니다.
`INTERPOLATE`는 `ORDER BY WITH FILL`에 포함되지 않는 컬럼에 적용할 수 있습니다. 이러한 컬럼은 `expr`를 적용해 이전 필드 값을 기준으로 채워집니다. `expr`가 없으면 이전 값이 반복됩니다. 목록을 생략하면 허용되는 모든 컬럼이 포함됩니다.

`WITH FILL`이 없는 쿼리의 예시:

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n;
```

```text title="Response"
┌─n─┬─source───┐
│ 1 │ original │
│ 4 │ original │
│ 7 │ original │
└───┴──────────┘
```

`WITH FILL` 수정자를 적용한 동일한 쿘리는 다음과 같습니다:

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┐
│   0 │          │
│ 0.5 │          │
│   1 │ original │
│ 1.5 │          │
│   2 │          │
│ 2.5 │          │
│   3 │          │
│ 3.5 │          │
│   4 │ original │
│ 4.5 │          │
│   5 │          │
│ 5.5 │          │
│   7 │ original │
└─────┴──────────┘
```

여러 필드가 있는 `ORDER BY field2 WITH FILL, field1 WITH FILL`의 경우, 채우기 순서는 `ORDER BY` 절에 나열된 필드 순서를 따릅니다.

예시:

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d2 WITH FILL,
    d1 WITH FILL STEP 5;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-01 │ 1970-01-03 │          │
│ 1970-01-01 │ 1970-01-04 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-01-01 │ 1970-01-06 │          │
│ 1970-01-01 │ 1970-01-07 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

필드 `d1`은 `d2` 값에 반복되는 값이 없기 때문에 채워지지 않고 기본값도 사용되지 않습니다. 또한 `d1`의 시퀀스도 올바르게 계산할 수 없습니다.

다음은 `ORDER BY`에서 필드를 변경한 쿼리입니다:

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP 5,
    d2 WITH FILL;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

다음 쿼리는 `d1` 컬럼에서 채워지는 각 값에 대해 1일짜리 `INTERVAL` 데이터 타입을 사용합니다:

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP INTERVAL 1 DAY,
    d2 WITH FILL;
```

```response title="Response"
┌─────────d1─┬─────────d2─┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-12 │ 1970-01-01 │          │
│ 1970-01-13 │ 1970-01-01 │          │
│ 1970-01-14 │ 1970-01-01 │          │
│ 1970-01-15 │ 1970-01-01 │          │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-17 │ 1970-01-01 │          │
│ 1970-01-18 │ 1970-01-01 │          │
│ 1970-01-19 │ 1970-01-01 │          │
│ 1970-01-20 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-22 │ 1970-01-01 │          │
│ 1970-01-23 │ 1970-01-01 │          │
│ 1970-01-24 │ 1970-01-01 │          │
│ 1970-01-25 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-27 │ 1970-01-01 │          │
│ 1970-01-28 │ 1970-01-01 │          │
│ 1970-01-29 │ 1970-01-01 │          │
│ 1970-01-30 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-01 │ 1970-01-01 │          │
│ 1970-02-02 │ 1970-01-01 │          │
│ 1970-02-03 │ 1970-01-01 │          │
│ 1970-02-04 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-06 │ 1970-01-01 │          │
│ 1970-02-07 │ 1970-01-01 │          │
│ 1970-02-08 │ 1970-01-01 │          │
│ 1970-02-09 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-11 │ 1970-01-01 │          │
│ 1970-02-12 │ 1970-01-01 │          │
│ 1970-02-13 │ 1970-01-01 │          │
│ 1970-02-14 │ 1970-01-01 │          │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-16 │ 1970-01-01 │          │
│ 1970-02-17 │ 1970-01-01 │          │
│ 1970-02-18 │ 1970-01-01 │          │
│ 1970-02-19 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-21 │ 1970-01-01 │          │
│ 1970-02-22 │ 1970-01-01 │          │
│ 1970-02-23 │ 1970-01-01 │          │
│ 1970-02-24 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-02-26 │ 1970-01-01 │          │
│ 1970-02-27 │ 1970-01-01 │          │
│ 1970-02-28 │ 1970-01-01 │          │
│ 1970-03-01 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-03 │ 1970-01-01 │          │
│ 1970-03-04 │ 1970-01-01 │          │
│ 1970-03-05 │ 1970-01-01 │          │
│ 1970-03-06 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-08 │ 1970-01-01 │          │
│ 1970-03-09 │ 1970-01-01 │          │
│ 1970-03-10 │ 1970-01-01 │          │
│ 1970-03-11 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

`STALENESS` 없이 작성한 쿼리 예시:

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   3 │     0 │          │
 5. │   4 │     0 │          │
 6. │   5 │    25 │ original │
 7. │   6 │     0 │          │
 8. │   7 │     0 │          │
 9. │   8 │     0 │          │
10. │   9 │     0 │          │
11. │  10 │    50 │ original │
12. │  11 │     0 │          │
13. │  12 │     0 │          │
14. │  13 │     0 │          │
15. │  14 │     0 │          │
16. │  15 │    75 │ original │
    └─────┴───────┴──────────┘
```

`STALENESS 3` 적용 후 동일한 쿼리:

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL STALENESS 3;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   5 │    25 │ original │
 5. │   6 │     0 │          │
 6. │   7 │     0 │          │
 7. │  10 │    50 │ original │
 8. │  11 │     0 │          │
 9. │  12 │     0 │          │
10. │  15 │    75 │ original │
11. │  16 │     0 │          │
12. │  17 │     0 │          │
    └─────┴───────┴──────────┘
```

`INTERPOLATE` 없이 작성한 쿼리의 예시입니다:

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     0 │
│   2 │          │     0 │
│ 2.5 │          │     0 │
│   3 │          │     0 │
│ 3.5 │          │     0 │
│   4 │ original │     4 │
│ 4.5 │          │     0 │
│   5 │          │     0 │
│ 5.5 │          │     0 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

`INTERPOLATE`를 적용한 동일한 쿼리:

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS inter + 1);
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     2 │
│   2 │          │     3 │
│ 2.5 │          │     4 │
│   3 │          │     5 │
│ 3.5 │          │     6 │
│   4 │ original │     4 │
│ 4.5 │          │     5 │
│   5 │          │     6 │
│ 5.5 │          │     7 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

<div id="filling-grouped-by-sorting-prefix">
  ## 정렬 접두사 기준 그룹별 채우기
</div>

특정 컬럼의 값이 같은 행들을 서로 독립적으로 채우는 기능이 유용한 경우가 있습니다. 대표적인 예시는 시계열에서 누락된 값을 채우는 것입니다.
다음과 같은 시계열 테이블이 있다고 가정합니다.

```sql
CREATE TABLE timeseries
(
    `sensor_id` UInt64,
    `timestamp` DateTime64(3, 'UTC'),
    `value` Float64
)
ENGINE = Memory;

SELECT * FROM timeseries;

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

또한 각 센서별로 누락된 값을 1초 인터벌로 서로 독립적으로 채우려고 합니다.
이를 위해 채우기를 적용할 `timestamp` 컬럼의 정렬 접두사로 `sensor_id` 컬럼을 사용합니다:

```sql
SELECT *
FROM timeseries
ORDER BY
    sensor_id,
    timestamp WITH FILL
INTERPOLATE ( value AS 9999 )

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       234 │ 2021-12-01 00:00:04.000 │  9999 │
│       234 │ 2021-12-01 00:00:05.000 │  9999 │
│       234 │ 2021-12-01 00:00:06.000 │  9999 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       432 │ 2021-12-01 00:00:02.000 │  9999 │
│       432 │ 2021-12-01 00:00:03.000 │  9999 │
│       432 │ 2021-12-01 00:00:04.000 │  9999 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

여기서는 채워진 행이 더 잘 눈에 띄도록 `value` 컬럼 값을 `9999`로 보간했습니다.
이 동작은 `use_with_fill_by_sorting_prefix` 설정으로 제어되며, 기본적으로 활성화되어 있습니다.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse에서 시계열 데이터 다루기](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)