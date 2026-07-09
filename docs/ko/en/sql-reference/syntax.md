---
description: '구문에 대한 문서'
sidebar_label: '구문'
sidebar_position: 2
slug: /sql-reference/syntax
title: '구문'
doc_type: 'reference'
---

이 섹션에서는 ClickHouse의 SQL 구문을 살펴보겠습니다.
ClickHouse는 SQL을 기반으로 한 구문을 사용하지만 다양한 확장 기능과 최적화를 제공합니다.

<div id="query-parsing">
  ## 쿼리 파싱
</div>

ClickHouse에는 두 가지 유형의 파서가 있습니다:

* *전체 SQL 파서* (재귀 하강 파서)
* *데이터 포맷 파서* (고속 스트림 파서)

전체 SQL 파서는 `INSERT` 쿼리를 제외한 모든 경우에 사용되며, `INSERT` 쿼리에는 두 파서가 모두 사용됩니다.

아래 쿼리를 살펴보겠습니다:

```sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

이미 언급했듯이 `INSERT` 쿼리는 두 가지 파서를 모두 사용합니다.
`INSERT INTO t VALUES` 부분은 전체 파서가 파싱하고,
데이터 `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` 는 데이터 포맷 파서 또는 고속 스트림 파서가 파싱합니다.

<details>
  <summary>전체 파서 활성화</summary>

  [`input_format_values_interpret_expressions`](../operations/settings/settings-formats.md#input_format_values_interpret_expressions) 설정을 사용하면
  데이터에도 전체 파서를 사용할 수 있습니다.

  앞서 언급한 설정을 `1`로 지정하면,
  ClickHouse는 먼저 고속 스트림 파서로 값을 파싱하려고 시도합니다.
  이 작업에 실패하면 ClickHouse는 데이터를 SQL [표현식](#expressions)으로 간주하고 전체 파서로 파싱을 시도합니다.
</details>

데이터는 어떤 포맷이든 될 수 있습니다.
쿼리를 받으면 서버는 요청에서 최대 [max&#95;query&#95;size](../operations/settings/settings.md#max_query_size)바이트만 RAM에 로드하여 처리하고
(기본값은 1 MB), 나머지는 스트림 파싱합니다.
이렇게 하면 ClickHouse에서 데이터를 삽입하는 권장 방식인 대용량 `INSERT` 쿼리에서 발생할 수 있는 문제를 방지할 수 있습니다.

`INSERT` 쿼리에서 [`Values`](/ko/interfaces/formats/Values) 포맷을 사용할 때,
데이터가 `SELECT` 쿼리의 표현식과 동일한 방식으로 파싱되는 것처럼 보일 수 있지만 실제로는 그렇지 않습니다.
`Values` 포맷은 훨씬 더 제한적입니다.

이 섹션의 나머지 부분에서는 전체 파서를 다룹니다.

:::note
포맷 파서에 대한 자세한 내용은 [포맷](../interfaces/formats.md) 섹션을 참조하십시오.
:::

<div id="spaces">
  ## 공백
</div>

* 구문 구성 요소 사이에는 공백 문자가 임의 개수만큼 들어갈 수 있습니다(쿼리의 시작과 끝 포함).
* 공백 문자에는 스페이스, 탭, 줄 바꿈(line feed), CR, 폼 피드가 포함됩니다.

<div id="comments">
  ## 주석
</div>

ClickHouse는 SQL 스타일 주석과 C 스타일 주석을 모두 지원합니다.

* SQL 스타일 주석은 `--`, `#!` 또는 `# `로 시작하며 줄 끝까지 계속됩니다. `--`와 `#!` 뒤의 공백은 생략할 수 있습니다.
* C 스타일 주석:
  * `//`(또는 `/`가 2개보다 많이 연속된 경우) 뒤에 텍스트가 오며 줄 끝까지 계속됩니다. `/` 뒤에 공백은 필요하지 않습니다.
  * 여러 줄 주석은 `/*`부터 `*/`까지 사용할 수 있습니다. 이 경우에도 공백은 필요하지 않습니다.
  * C 스타일 주석은 중첩할 수 있습니다.

예시:

```sql
/*
 * Compute the number of days between two dates.
 * /* Returns NULL if either argument is NULL */
 */
SELECT
    dateDiff('day', toDate('2024-01-01'), toDate('2024-12-31')) AS days_in_year, -- 365
    dateDiff('day', toDate('2020-01-01'), today()) AS days_since  #! since 2020
    ///////////////////////////////////////////////////////////////////
    # TODO: add hour/minute variants
```

<div id="keywords">
  ## 키워드
</div>

ClickHouse의 키워드는 문맥에 따라 *case-sensitive*일 수도 있고 *case-insensitive*일 수도 있습니다.

키워드는 다음에 해당하면 **case-insensitive**입니다.

* SQL 표준. 예를 들어 `SELECT`, `select`, `SeLeCt`는 모두 유효합니다.
* 널리 사용되는 일부 DBMS(MySQL 또는 Postgres)의 구현. 예를 들어 `DateTime`은 `datetime`과 동일합니다.

:::note
데이터 타입 이름이 case-sensitive인지 여부는 [system.data&#95;type&#95;families](/ko/operations/system-tables/data_type_families) 테이블에서 확인할 수 있습니다.
:::

반면 표준 SQL과 달리, 그 밖의 모든 키워드(함수 이름 포함)는 **case-sensitive**입니다.

또한 키워드는 예약어가 아닙니다.
키워드는 해당 문맥에서만 그렇게 처리됩니다.
키워드와 같은 이름의 [식별자](#identifiers)를 사용하는 경우 큰따옴표 또는 백틱으로 감싸십시오.

예를 들어 다음 쿼리는 `table_name` 테이블에 `"FROM"`이라는 이름의 컬럼이 있으면 유효합니다.

```sql
SELECT "FROM" FROM table_name
```

<div id="identifiers">
  ## 식별자
</div>

식별자는 다음과 같습니다.

* 클러스터, 데이터베이스, 테이블, 파티션, 컬럼 이름
* [함수](#functions)
* [데이터 타입](../sql-reference/data-types/index.md)
* [표현식 별칭](#expression-aliases)

식별자는 따옴표로 감싸거나 감싸지 않을 수 있지만, 일반적으로는 따옴표 없이 사용하는 방식을 권장합니다.

따옴표로 감싸지 않은 식별자는 정규식 `^[a-zA-Z_][0-9a-zA-Z_]*$`와 일치해야 하며, [키워드](#keywords)와 같아서는 안 됩니다.
유효한 식별자와 유효하지 않은 식별자의 예시는 아래 표를 참조하십시오.

| 유효한 식별자                                        | 유효하지 않은 식별자                            |
| ---------------------------------------------- | -------------------------------------- |
| `xyz`, `_internal`, `Id_with_underscores_123_` | `1x`, `tom@gmail.com`, `äußerst_schön` |

키워드와 동일한 식별자를 사용하거나 식별자에 다른 기호를 사용하려면 `"id"`, `` `id` ``와 같이 큰따옴표 또는 백틱으로 감싸십시오.

:::note
따옴표로 감싼 식별자에 적용되는 이스케이프 규칙은 문자열 리터럴에도 동일하게 적용됩니다. 자세한 내용은 [String](#string)을 참조하십시오.
:::

:::tip[컬럼 이름에 점 사용 피하기]
점이 포함된 컬럼 이름, 공통된 점 접두사를 공유하는 컬럼, 그리고 `Array` 타입 컬럼은 `flatten_nested = 1`(기본값)일 때 각각 평탄화된 Nested 구조의 일부로 해석될 수 있습니다. 이로 인해 삽입 시 예기치 않은 배열 길이 검증과 이름 변경 제한이 발생할 수 있습니다.

가능하면 컬럼 이름에 점을 사용하지 마십시오.
의도적으로 `Nested` 의미 체계가 필요한 경우가 아니라면, 컬럼 이름에서는 점 대신 밑줄(`_`) 또는 다른 구분자를 사용하십시오.
:::

<div id="literals">
  ## 리터럴
</div>

ClickHouse에서 리터럴은 쿼리에 직접 명시되는 값입니다.
즉, 쿼리 실행 중에는 변하지 않는 고정값입니다.

리터럴에는 다음이 있습니다.

* [String](#string)
* [숫자형](#numeric)
* [복합형](#compound)
* [`NULL`](#null)
* [Heredoc](#heredoc) (사용자 정의 문자열 리터럴)

아래 섹션에서 각각을 자세히 살펴보겠습니다.

<div id="string">
  ### String
</div>

String 리터럴은 작은따옴표로 묶어야 합니다. 큰따옴표는 지원되지 않습니다.

이스케이프는 다음 두 가지 방식으로 처리할 수 있습니다.

* 작은따옴표 문자 `'`만 앞에 작은따옴표를 붙여 `''`로 이스케이프하거나,
* 아래 표에 나열된 지원되는 이스케이프 시퀀스와 함께 앞에 백슬래시를 사용합니다.

:::note
백슬래시는 아래에 나열된 문자 이외의 문자 앞에 오면 특별한 의미를 잃고, 문자 그대로 해석됩니다.
:::

| 지원되는 이스케이프                             | 설명                                               |
| -------------------------------------- | ------------------------------------------------ |
| `\xHH`                                 | 뒤에 임의 개수의 16진수 숫자(H)가 오는 8비트 문자 지정입니다.           |
| `\N`                                   | 예약됨, 아무 동작도 하지 않음(예: `SELECT 'a\Nb'`는 `ab`를 반환함) |
| `\a`                                   | 알림                                               |
| `\b`                                   | 백스페이스                                            |
| `\e`                                   | 이스케이프 문자                                         |
| `\f`                                   | 폼 피드                                             |
| `\n`                                   | 줄 바꿈                                             |
| `\r`                                   | 캐리지 리턴                                           |
| `\t`                                   | 가로 탭                                             |
| `\v`                                   | 세로 탭                                             |
| `\0`                                   | 널 문자                                             |
| `\\`                                   | 백슬래시                                             |
| `\'` (or `''`)                         | 작은따옴표                                            |
| `\"`                                   | 큰따옴표                                             |
| `` ` ``                                | 백틱                                               |
| `\/`                                   | 슬래시                                              |
| `\=`                                   | 등호                                               |
| ASCII control characters (c &lt;= 31). |                                                  |

:::note
String 리터럴에서는 최소한 `'`와 `\`를 이스케이프 코드 `\'`(또는 `''`) 및 `\\`를 사용해 이스케이프해야 합니다.
:::

<div id="numeric">
  ### 숫자
</div>

숫자 리터럴은 다음과 같이 파싱됩니다.

* 리터럴 앞에 마이너스 기호 `-`가 있으면 해당 토큰은 건너뛰고, 파싱 후 결과에 음수를 적용합니다.
* 숫자 리터럴은 먼저 [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) 함수를 사용해 64비트 부호 없는 정수로 파싱됩니다.
  * 값 앞에 `0b` 또는 `0x`/`0X`가 있으면 숫자는 각각 2진수 또는 16진수로 파싱됩니다.
  * 값이 음수이고 절댓값이 2<sup>63</sup>보다 크면 오류가 반환됩니다.
* 이 단계가 실패하면, 다음으로 [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) 함수를 사용해 값을 floating-point 숫자로 파싱합니다.
* 그 외의 경우에는 오류가 반환됩니다.

리터럴 값은 해당 값이 들어갈 수 있는 가장 작은 타입으로 CAST됩니다.
예시:

* `1`은 `UInt8`로 파싱됩니다.
* `256`은 `UInt16`으로 파싱됩니다.

:::note 중요
64비트보다 큰 정수 값(`UInt128`, `Int128`, `UInt256`, `Int256`)은 올바르게 파싱하려면 더 큰 타입으로 CAST해야 합니다.

```sql
-170141183460469231731687303715884105728::Int128
340282366920938463463374607431768211455::UInt128
-57896044618658097711785492504343953926634992332820282019728792003956564819968::Int256
115792089237316195423570985008687907853269984665640564039457584007913129639935::UInt256
```

이는 위 알고리즘을 우회하여, 임의 정밀도를 지원하는 루틴으로 정수를 구문 분석합니다.

그렇지 않으면 리터럴이 부동소수점 수로 구문 분석되므로, 잘림으로 인해 정밀도가 손실될 수 있습니다.
:::

자세한 내용은 [데이터 타입](../sql-reference/data-types/index.md)을 참조하십시오.

숫자 리터럴 내부의 밑줄 `_`은 무시되며, 가독성을 높이기 위해 사용할 수 있습니다.

다음 숫자 리터럴이 지원됩니다.

| Numeric Literal              | 예시                                              |
| ---------------------------- | ----------------------------------------------- |
| **정수**                       | `1`, `10_000_000`, `18446744073709551615`, `01` |
| **소수**                       | `0.1`                                           |
| **지수 표기법**                   | `1e100`, `-1e-100`                              |
| **부동소수점 수**                  | `123.456`, `inf`, `nan`                         |
| **16진수**                     | `0xc0fe`                                        |
| **SQL Standard 호환 16진수 문자열** | `x'c0fe'`                                       |
| **이진수**                      | `0b1101`                                        |
| **SQL Standard 호환 바이너리 문자열** | `b'1101'`                                       |

:::note
해석상 발생할 수 있는 오류를 방지하기 위해 8진수 리터럴은 지원되지 않습니다.
:::

<div id="compound">
  ### 복합형
</div>

배열은 `[]`로 만듭니다: `[1, 2, 3]`. 튜플은 `()`로 만듭니다: `(1, 'Hello, world!', 2)`.
엄밀히 말하면 이것들은 리터럴이 아니라, 각각 배열 생성 연산자와 튜플 생성 연산자를 사용하는 표현식입니다.
배열은 항목이 최소 1개 있어야 하며, 튜플은 항목이 최소 2개 있어야 합니다.

:::note
튜플이 `SELECT` 쿼리의 `IN` 절에 나타나는 별도의 경우도 있습니다.
쿼리 결과에는 튜플이 포함될 수 있지만, 튜플은 데이터베이스에 저장할 수 없습니다([Memory](../engines/table-engines/special/memory.md) 엔진을 사용하는 테이블은 예외).
:::

<div id="null">
  ### NULL
</div>

`NULL`은 값이 없음을 나타내는 데 사용됩니다.
테이블 필드에 `NULL`을 저장하려면 해당 필드의 타입이 [널 허용](../sql-reference/data-types/nullable.md)이어야 합니다.

:::note
`NULL`에 대해서는 다음 사항에 유의해야 합니다.

* 데이터 포맷(입력 또는 출력)에 따라 `NULL`이 다르게 표현될 수 있습니다. 자세한 내용은 [데이터 포맷](/ko/interfaces/formats)을 참조하십시오.
* `NULL` 처리는 다소 복잡합니다. 예를 들어, 비교 연산의 인수 중 하나 이상이 `NULL`이면 이 연산의 결과도 `NULL`이 됩니다. 이는 곱셈, 덧셈 및 기타 연산에도 동일하게 적용됩니다. 각 연산의 문서를 확인할 것을 권장합니다.
* 쿼리에서는 [`IS NULL`](/ko/sql-reference/functions/functions-for-nulls#isNull) 및 [`IS NOT NULL`](/ko/sql-reference/functions/functions-for-nulls#isNotNull) 연산자와 관련 함수인 `isNull` 및 `isNotNull`을 사용해 `NULL` 여부를 확인할 수 있습니다.
  :::

<div id="heredoc">
  ### Heredoc
</div>

[heredoc](https://en.wikipedia.org/wiki/Here_document)은 원래 포맷을 유지하면서 문자열(대개 여러 줄)을 정의하는 방법입니다.
heredoc은 두 개의 `$` 기호 사이에 넣는 사용자 정의 문자열 리터럴입니다.

예시:

```sql
SELECT $heredoc$SHOW CREATE VIEW my_view$heredoc$;

┌─'SHOW CREATE VIEW my_view'─┐
│ SHOW CREATE VIEW my_view   │
└────────────────────────────┘
```

:::note

* 두 heredoc 사이의 값은 &quot;있는 그대로&quot; 처리됩니다.
  :::

:::tip

* heredoc을 사용하면 SQL, HTML, XML 등의 코드 조각을 포함할 수 있습니다.
  :::

<div id="defining-and-using-query-parameters">
  ## 쿼리 매개변수 정의 및 사용
</div>

쿼리 매개변수를 사용하면 구체적인 식별자 대신 추상적인 플레이스홀더가 포함된 범용 쿼리를 작성할 수 있습니다.
쿼리 매개변수가 포함된 쿼리를 실행하면
모든 플레이스홀더가 해석되어 실제 쿼리 매개변수 값으로 대체됩니다.

쿼리 매개변수는 여러 방법으로 정의할 수 있습니다:

* `SET param_<name>=<value>` — 쿼리에서 `SET` 명령을 사용합니다.
* `--param_<name>='<value>'` — 명령줄에서 `clickhouse-client` 인수로 사용합니다.
* `param_<name>=<value>` — HTTP 인터페이스의 URL 쿼리 문자열 매개변수로 사용합니다.

쿼리 매개변수는 쿼리에서 `{<name>: <datatype>}` 형식으로 참조할 수 있습니다. 여기서 `<name>`은 쿼리 매개변수 이름이고 `<datatype>`은 변환할 데이터 타입입니다.

<details>
  <summary>SET 명령 사용 예시</summary>

  예를 들어, 다음 SQL은 `a`, `b`, `c`, `d`라는 이름의 매개변수를 정의하며, 각각 서로 다른 데이터 타입을 가집니다:

  ```sql
  SET param_a = 13;
  SET param_b = 'str';
  SET param_c = '2022-08-04 18:30:53';
  SET param_d = {'10': [11, 12], '13': [14, 15]};

  SELECT
     {a: UInt32},
     {b: String},
     {c: DateTime},
     {d: Map(String, Array(UInt8))};

  13    str    2022-08-04 18:30:53    {'10':[11,12],'13':[14,15]}
  ```
</details>

<details>
  <summary>clickhouse-client 사용 예시</summary>

  `clickhouse-client`를 사용하는 경우 매개변수는 `--param_name=value` 형식으로 지정합니다. 예를 들어, 다음 매개변수의 이름은 `message`이며 `String`으로 가져옵니다:

  ```bash
  clickhouse-client --param_message='hello' --query="SELECT {message: String}"

  hello
  ```

  쿼리 매개변수가 데이터베이스, 테이블, 함수 또는 기타 식별자의 이름을 나타내는 경우 타입으로 `Identifier`를 사용하십시오. 예를 들어, 다음 쿼리는 `uk_price_paid`라는 이름의 테이블에서 행을 반환합니다:

  ```sql
  SET param_mytablename = "uk_price_paid";
  SELECT * FROM {mytablename:Identifier};
  ```
</details>

<details>
  <summary>HTTP 인터페이스 사용 예시</summary>

  쿼리 매개변수는 `param_` 접두사가 붙은 URL 쿼리 문자열 매개변수로 전달할 수 있습니다. 예를 들면 다음과 같습니다:

  ```bash
  curl -s "http://localhost:8123/?param_message=hello" --data-binary "SELECT {message: String}"

  hello
  ```
</details>

<details>
  <summary>Web UI 사용 예시</summary>

  기본 제공되는 Web UI(`play.html`)는 쿼리에서 `{name:Type}` 매개변수 플레이스홀더를 자동으로 감지하고, 각 매개변수에 대해 레이블이 지정된 입력 필드를 표시합니다. 매개변수 값은 HTTP 요청에 포함되며, 북마크와 공유를 위해 페이지 URL에도 유지됩니다.
</details>

:::note
쿼리 매개변수는 임의의 SQL 쿼리에서 아무 위치에나 사용할 수 있는 범용 텍스트 치환이 아닙니다.
주로 식별자나 리터럴 대신 `SELECT` SQL 문에서 사용하도록 설계되었습니다.
:::

<div id="functions">
  ## 함수
</div>

함수 호출은 `()` 안에 인수 목록(비어 있을 수도 있음)을 포함한 식별자 형태로 작성합니다.
표준 SQL과 달리, 인수 목록이 비어 있어도 괄호를 반드시 사용해야 합니다.
예시는 다음과 같습니다:

```sql
now()
```

다음도 있습니다:

* [일반 함수](/ko/sql-reference/functions/overview).
* [집계 함수](/ko/sql-reference/aggregate-functions).

일부 집계 함수는 괄호 안에 2개의 인수 목록을 가질 수 있습니다. 예시는 다음과 같습니다:

```sql
quantile (0.9)(x) 
```

이러한 집계 함수는 &quot;매개변수형&quot; 함수라고 하며,
첫 번째 목록의 인수를 &quot;매개변수&quot;라고 합니다.

:::note
매개변수가 없는 집계 함수의 구문은 일반 함수와 동일합니다.
:::

<div id="operators">
  ## 연산자
</div>

연산자는 쿼리를 구문 분석할 때 우선순위와 결합 방향을 고려하여 대응하는 함수로 변환됩니다.

예를 들어, 다음 표현식은

```text
1 + 2 * 3 + 4
```

로 변환됩니다

```text
plus(plus(1, multiply(2, 3)), 4)`
```

<div id="data-types-and-database-table-engines">
  ## 데이터 타입 및 데이터베이스 테이블 엔진
</div>

`CREATE` 쿼리에서 데이터 타입과 테이블 엔진은 식별자나 함수와 동일한 방식으로 작성됩니다.
즉, 괄호로 묶인 인수 목록을 포함할 수도 있고 포함하지 않을 수도 있습니다.

자세한 내용은 다음 섹션을 참조하십시오.

* [데이터 타입](/ko/sql-reference/data-types/index.md)
* [테이블 엔진](/ko/engines/table-engines/index.md)
* [CREATE](/ko/sql-reference/statements/create/index.md).

<div id="expressions">
  ## 표현식
</div>

표현식은 다음 중 하나일 수 있습니다.

* 함수
* 식별자
* 리터럴
* 연산자가 적용된 식
* 괄호로 묶인 표현식
* 서브쿼리
* 애스터리스크

또한 [별칭](#expression-aliases)을 포함할 수도 있습니다.

표현식 목록은 쉼표로 구분된 하나 이상의 표현식으로 이루어집니다.
함수와 연산자도 표현식을 인수로 받을 수 있습니다.

상수 표현식은 쿼리 분석 중, 즉 실행 전에 결과를 알 수 있는 표현식입니다.
예를 들어, 리터럴만으로 이루어진 표현식은 상수 표현식입니다.

<div id="expression-aliases">
  ## 표현식 별칭
</div>

별칭은 쿼리에서 [표현식](#expressions)에 사용자가 지정한 이름입니다.

```sql
expr AS alias
```

위 구문의 각 부분은 아래에서 설명합니다.

| Part of syntax | Description                                                                      | Example                                                                 | Notes                                                                                                       |
| -------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `AS`           | 별칭을 정의하는 키워드입니다. `AS` 키워드를 사용하지 않아도 `SELECT` 절에서 테이블 이름이나 컬럼 이름의 별칭을 정의할 수 있습니다. | `SELECT table_name_alias.column_name FROM table_name table_name_alias`. | [CAST](/ko/sql-reference/functions/type-conversion-functions#CAST) 함수에서 `AS` 키워드는 다른 의미로 사용됩니다. 함수 설명을 참조하십시오. |
| `expr`         | ClickHouse에서 지원하는 모든 표현식입니다.                                                     | `SELECT column_name * 2 AS double FROM some_table`                      |                                                                                                             |
| `alias`        | `expr`의 이름입니다. 별칭은 [identifiers](#identifiers) 구문을 따라야 합니다.                      | `SELECT "table t".column_name FROM table_name AS "table t"`.            |                                                                                                             |

<div id="notes-on-usage">
  ### 사용 시 참고 사항
</div>

* 별칭은 쿼리 또는 서브쿼리 전체에 전역으로 적용되며, 어떤 표현식이든 쿼리의 어느 부분에서나 별칭으로 정의할 수 있습니다. 예를 들어, 다음과 같습니다:

```sql
SELECT (1 AS n) + 2, n`.
```

* 별칭은 서브쿼리 내부나 서브쿼리 사이에서는 인식되지 않습니다. 예를 들어, 다음 쿼리를 실행하면 ClickHouse에서 `Unknown identifier: num` 예외가 발생합니다:

```sql
`SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a`
```

* 서브쿼리의 `SELECT` 절에서 결과 컬럼에 별칭이 정의되어 있으면, 해당 컬럼은 외부 쿼리에서 사용할 수 있습니다. 예시는 다음과 같습니다:

```sql
SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.
```

* 컬럼 또는 테이블 이름과 동일한 별칭은 주의해서 사용하십시오. 다음 예시를 살펴보겠습니다:

```sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog();

SELECT
    argMax(a, b),
    sum(b) AS b
FROM t;

Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

앞선 예시에서는 컬럼 `b`가 있는 테이블 `t`를 선언했습니다.
그런 다음 데이터를 조회할 때 `sum(b) AS b` 별칭을 정의했습니다.
별칭은 전역으로 적용되므로
ClickHouse는 표현식 `argMax(a, b)`의 리터럴 `b`를 표현식 `sum(b)`로 치환했습니다.
이 치환으로 인해 예외가 발생했습니다.

:::note
[prefer&#95;column&#95;name&#95;to&#95;alias](/ko/operations/settings/settings#prefer_column_name_to_alias)를 `1`로 설정하면 이 기본 동작을 변경할 수 있습니다.
:::

<div id="asterisk">
  ## 애스터리스크
</div>

`SELECT` 쿼리에서 애스터리스크는 표현식 대신 사용할 수 있습니다.
자세한 내용은 [SELECT](/ko/sql-reference/statements/select/index.md#asterisk) 섹션을 참고하십시오.