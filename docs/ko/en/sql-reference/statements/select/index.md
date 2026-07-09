---
description: 'SELECT 쿼리 문서'
sidebar_label: 'SELECT'
sidebar_position: 32
slug: /sql-reference/statements/select/
title: 'SELECT 쿼리'
doc_type: 'reference'
---

`SELECT` 쿼리는 데이터를 조회합니다. 기본적으로는 요청한 데이터가 클라이언트에 반환되며, [INSERT INTO](../../../sql-reference/statements/insert-into.md)와 함께 사용하면 다른 테이블로 전달될 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

모든 절은 선택 사항입니다. 단, `SELECT` 바로 뒤에 오는 필수 표현식 목록은 예외이며, 이에 대해서는 [아래](#select-clause)에서 더 자세히 설명합니다.

각 선택적 절의 세부 사항은 별도의 섹션에서 다루며, 실제 실행 순서와 동일한 순서로 나열되어 있습니다:

* [WITH 절](../../../sql-reference/statements/select/with.md)
* [SELECT 절](#select-clause)
* [DISTINCT 절](../../../sql-reference/statements/select/distinct.md)
* [FROM 절](../../../sql-reference/statements/select/from.md)
* [SAMPLE 절](../../../sql-reference/statements/select/sample.md)
* [JOIN 절](../../../sql-reference/statements/select/join.md)
* [PREWHERE 절](../../../sql-reference/statements/select/prewhere.md)
* [WHERE 절](../../../sql-reference/statements/select/where.md)
* [WINDOW 절](../../../sql-reference/window-functions/index.md)
* [GROUP BY 절](/ko/sql-reference/statements/select/group-by)
* [LIMIT BY 절](../../../sql-reference/statements/select/limit-by.md)
* [HAVING 절](../../../sql-reference/statements/select/having.md)
* [QUALIFY 절](../../../sql-reference/statements/select/qualify.md)
* [LIMIT 절](../../../sql-reference/statements/select/limit.md)
* [OFFSET 절](../../../sql-reference/statements/select/offset.md)
* [UNION 절](../../../sql-reference/statements/select/union.md)
* [INTERSECT 절](../../../sql-reference/statements/select/intersect.md)
* [EXCEPT 절](../../../sql-reference/statements/select/except.md)
* [INTO OUTFILE 절](../../../sql-reference/statements/select/into-outfile.md)
* [FORMAT 절](../../../sql-reference/statements/select/format.md)

<div id="select-clause">
  ## SELECT 절
</div>

`SELECT` 절에 지정된 [표현식](/ko/sql-reference/syntax#expressions)은 위에서 설명한 각 절의 모든 작업이 끝난 후에 계산됩니다. 이러한 표현식은 결과의 각 개별 행에 적용되는 것처럼 동작합니다. `SELECT` 절의 표현식에 집계 함수가 포함되어 있으면, ClickHouse는 [GROUP BY](/ko/sql-reference/statements/select/group-by) 집계 과정에서 집계 함수와 그 인수로 사용되는 표현식을 처리합니다.

결과에 모든 컬럼을 포함하려면 애스터리스크(`*`) 기호를 사용하십시오. 예를 들어 `SELECT * FROM ...`와 같이 사용합니다.

<div id="dynamic-column-selection">
  ### 동적 컬럼 선택
</div>

동적 컬럼 선택(또는 COLUMNS 표현식이라고도 함)을 사용하면 결과에서 [re2](https://en.wikipedia.org/wiki/RE2_\(software\)) 정규식과 일치하는 일부 컬럼을 선택할 수 있습니다.

```sql
COLUMNS('regexp')
```

예를 들어, 다음 테이블을 예로 들어 보겠습니다:

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

다음 쿼리는 이름에 `a` 기호가 포함된 모든 컬럼의 데이터를 선택합니다.

```sql
SELECT COLUMNS('a') FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

선택한 컬럼은 알파벳순으로 반환되지 않습니다.

쿼리에서 여러 `COLUMNS` 표현식을 사용할 수 있으며, 여기에 함수를 적용할 수도 있습니다.

예시:

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

`COLUMNS` 표현식이 반환한 각 컬럼은 함수에 각각 별도의 인수로 전달됩니다. 또한 함수가 지원하는 경우 다른 인수도 함께 전달할 수 있습니다. 함수를 사용할 때는 주의해야 합니다. 함수가 전달된 인수 개수를 지원하지 않으면 ClickHouse에서 예외가 발생합니다.

예시:

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

이 예시에서 `COLUMNS('a')`는 `aa`와 `ab`의 두 컬럼을 반환합니다. `COLUMNS('c')`는 `bc` 컬럼을 반환합니다. `+` 연산자는 3개의 인수에 적용할 수 없으므로, ClickHouse는 관련 메시지와 함께 예외를 발생시킵니다.

`COLUMNS` 표현식과 일치하는 컬럼은 서로 다른 데이터 타입일 수 있습니다. `COLUMNS`가 어떤 컬럼과도 일치하지 않고 `SELECT`의 유일한 표현식인 경우, ClickHouse는 예외를 발생시킵니다.

<div id="select-columns-with-like-or-ilike">
  #### `LIKE` 또는 `ILIKE`를 사용해 컬럼 선택
</div>

`*` 뒤에 대소문자를 구분하는 `LIKE` 또는 대소문자를 구분하지 않는 `ILIKE`를 사용해 컬럼 이름을 패턴과 일치시키는 방식으로 컬럼을 선택할 수도 있습니다:

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

`LIKE` 및 `ILIKE` 패턴은 정규식 의미가 아니라 `LIKE` 의미 체계를 따릅니다. `%` 문자는 임의의 문자 시퀀스와 일치하고, `_` 문자는 임의의 단일 문자와 일치하며, `\`는 `%`, `_`, `\`를 이스케이프합니다. 두 패턴의 유일한 차이점은 `LIKE`는 컬럼 이름을 대소문자를 구분하여 일치시키는 반면 `ILIKE`는 대소문자를 구분하지 않는다는 점입니다. 예시는 다음과 같습니다:

```sql
SELECT * ILIKE 'a_' FROM col_names
```

쿼리는 `a`로 시작하는, 이름이 두 글자인 컬럼(예: `aa`, `ab`)을 선택합니다.

`* LIKE` 및 `* ILIKE`는 한정자가 붙은 애스터리스크와 컬럼 변환자도 지원합니다:

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

<div id="asterisk">
  ### 애스터리스크
</div>

쿼리의 어느 부분에서든 표현식 대신 애스터리스크를 사용할 수 있습니다. 쿼리를 분석할 때 애스터리스크는 모든 테이블 컬럼의 목록으로 확장됩니다(`MATERIALIZED` 및 `ALIAS` 컬럼 제외). 애스터리스크 사용이 타당한 경우는 몇 가지뿐입니다.

* 테이블 덤프를 생성할 때
* system tables처럼 컬럼 수가 적은 테이블인 경우
* 테이블에 어떤 컬럼이 있는지 확인할 때. 이 경우 `LIMIT 1`을 설정하십시오. 하지만 `DESC TABLE` 쿼리를 사용하는 편이 더 좋습니다.
* `PREWHERE`를 사용해 적은 수의 컬럼에 강한 필터링을 적용할 때
* 서브쿼리에서(외부 쿼리에 필요하지 않은 컬럼은 서브쿼리에서 제외되므로)

그 밖의 모든 경우에는 애스터리스크 사용을 권장하지 않습니다. 열 지향 DBMS의 장점은 살리지 못하고 단점만 드러내기 때문입니다. 다시 말해, 애스터리스크 사용은 권장되지 않습니다.

<div id="extreme-values">
  ### 극값
</div>

결과와 함께 결과 컬럼의 최솟값과 최댓값도 가져올 수 있습니다. 이렇게 하려면 **extremes** 설정을 1로 지정하십시오. 최솟값과 최댓값은 숫자 타입, 날짜, 시간 정보가 포함된 날짜에 대해 계산됩니다. 다른 컬럼에는 기본값이 출력됩니다.

최솟값과 최댓값에 해당하는 2개의 추가 행도 계산됩니다. 이 2개의 추가 행은 다른 행과 별도로 `XML`, `JSON*`, `TabSeparated*`, `CSV*`, `Vertical`, `Template`, `Pretty*` [포맷](../../../interfaces/formats.md)에서 출력됩니다. 다른 포맷에서는 출력되지 않습니다.

`JSON*` 및 `XML` 포맷에서는 극값이 별도의 &#39;extremes&#39; 필드에 출력됩니다. `TabSeparated*`, `CSV*`, `Vertical` 포맷에서는 이 행이 기본 결과 뒤에 출력되며, &#39;합계&#39;가 있으면 그 뒤에 출력됩니다. 또한 이 행 앞에는 빈 행이 하나 추가됩니다(다른 데이터 뒤). `Pretty*` 포맷에서는 이 행이 기본 결과 뒤에 별도의 테이블로 출력되며, `totals`가 있으면 그 뒤에 출력됩니다. `Template` 포맷에서는 지정된 템플릿에 따라 극값이 출력됩니다.

극값은 `LIMIT` 적용 전의 행을 기준으로 계산되지만, `LIMIT BY` 적용 후에 계산됩니다. 하지만 `LIMIT offset, size`를 사용하는 경우에는 `offset` 이전의 행도 `extremes`에 포함됩니다. 스트림 request에서는 `LIMIT`를 통과한 소수의 행이 결과에 포함될 수도 있습니다.

<div id="notes">
  ### 참고 사항
</div>

쿼리의 모든 부분에서 동의어(`AS` 별칭)를 사용할 수 있습니다.

`GROUP BY`, `ORDER BY`, `LIMIT BY` 절은 위치 인수를 지원합니다. 이 기능을 사용하려면 [enable&#95;positional&#95;arguments](/ko/operations/settings/settings#enable_positional_arguments) 설정을 활성화하십시오. 그러면 예를 들어 `ORDER BY 1,2`는 테이블의 첫 번째 컬럼과 두 번째 컬럼을 기준으로 행을 정렬합니다.

<div id="implementation-details">
  ## 구현 세부 정보
</div>

쿼리에서 `DISTINCT`, `GROUP BY`, `ORDER BY` 절과 `IN`, `JOIN` 서브쿼리를 생략하면, 해당 쿼리는 O(1) 수준의 RAM만 사용하여 전적으로 스트림 방식으로 처리됩니다. 그렇지 않으면 적절한 제한을 지정하지 않은 경우 쿼리가 많은 RAM을 사용할 수 있습니다:

* `max_memory_usage`
* `max_rows_to_group_by`
* `max_rows_to_sort`
* `max_rows_in_distinct`
* `max_bytes_in_distinct`
* `max_rows_in_set`
* `max_bytes_in_set`
* `max_rows_in_join`
* `max_bytes_in_join`
* `max_bytes_before_external_sort`
* `max_bytes_ratio_before_external_sort`
* `max_bytes_before_external_group_by`
* `max_bytes_ratio_before_external_group_by`

자세한 내용은 &quot;설정&quot; 섹션을 참조하십시오. 외부 정렬(임시 테이블을 디스크에 저장)을 사용하고 외부 집계도 수행할 수 있습니다.

<div id="select-modifiers">
  ## SELECT 수정자
</div>

`SELECT` 쿼리에서 다음 수정자를 사용할 수 있습니다.

| 수정자                                | 설명                                                                                                                                                                                                           |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`APPLY`](./apply_modifier.md)     | 쿼리의 외부 테이블 표현식이 반환하는 각 행에 대해 함수를 호출할 수 있습니다.                                                                                                                                                                 |
| [`EXCEPT`](./except_modifier.md)   | 결과에서 제외할 하나 이상의 컬럼 이름을 지정합니다. 일치하는 모든 컬럼 이름은 출력에서 생략됩니다.                                                                                                                                                     |
| [`REPLACE`](./replace_modifier.md) | 하나 이상의 [표현식 별칭](/ko/sql-reference/syntax#expression-aliases)을 지정합니다. 각 별칭은 `SELECT *` 문의 컬럼 이름과 일치해야 합니다. 출력 컬럼 목록에서 별칭과 일치하는 컬럼은 해당 `REPLACE`의 표현식으로 대체됩니다. 이 수정자는 컬럼의 이름이나 순서는 바꾸지 않지만, 값과 값의 유형은 변경할 수 있습니다. |

<div id="modifier-combinations">
  ### 수정자 조합
</div>

각 수정자는 개별적으로 사용할 수도 있고 조합해서 사용할 수도 있습니다.

**예시:**

같은 수정자를 여러 번 사용하는 경우

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

하나의 쿼리에서 여러 수정자를 사용합니다.

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

<div id="settings-in-select-query">
  ## SELECT 쿼리의 SETTINGS
</div>

필요한 설정은 `SELECT` 쿼리 내에서 바로 지정할 수 있습니다. 설정 값은 이 쿼리에만 적용되며, 쿼리 실행이 끝나면 `default` 또는 이전 값으로 재설정됩니다.

설정을 지정하는 다른 방법은 [여기](/ko/operations/settings/overview)를 참조하십시오.

불리언 설정이 true인 경우에는 값 할당을 생략하는 축약 구문을 사용할 수 있습니다. 설정 이름만 지정하면 자동으로 `1`(true)로 설정됩니다.

**예시**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```