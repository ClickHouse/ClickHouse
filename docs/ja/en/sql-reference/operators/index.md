---
description: '演算子のドキュメント'
sidebar_label: '演算子'
sidebar_position: 38
slug: /sql-reference/operators/
title: '演算子'
doc_type: 'reference'
---

ClickHouse は、クエリのパース段階で、優先度、優先順位、結合性に基づいて演算子を対応する関数に変換します。

<div id="access-operators">
  ## アクセス演算子
</div>

`a[N]` – 配列要素へのアクセス。`arrayElement(a, N)` 関数。

`a.N` – タプル 要素へのアクセス。`tupleElement(a, N)` 関数。

<div id="numeric-negation-operator">
  ## 数値の符号反転演算子
</div>

`-a` – `negate (a)` 関数。

タプルの符号反転については、[tupleNegate](../../sql-reference/functions/tuple-functions.md#tupleNegate) を参照してください。

<div id="multiplication-and-division-operators">
  ## 乗算および除算の演算子
</div>

`a * b` – `multiply(a, b)` 関数です。

数値によるタプルの乗算については [tupleMultiplyByNumber](../../sql-reference/functions/tuple-functions.md#tupleMultiplyByNumber)、スカラー積については [dotProduct](/ja/sql-reference/functions/array-functions#arrayDotProduct) を参照してください。

`a / b` – `divide(a, b)` 関数です。

数値によるタプルの除算については [tupleDivideByNumber](../../sql-reference/functions/tuple-functions.md#tupleDivideByNumber) を参照してください。

`a % b` – `modulo(a, b)` 関数です。

<div id="addition-and-subtraction-operators">
  ## 加算演算子と減算演算子
</div>

`a + b` – `plus(a, b)` 関数です。

タプル の加算については、[tuplePlus](../../sql-reference/functions/tuple-functions.md#tuplePlus) を参照してください。

`a - b` – `minus(a, b)` 関数です。

タプル の減算については、[tupleMinus](../../sql-reference/functions/tuple-functions.md#tupleMinus) を参照してください。

<div id="comparison-operators">
  ## 比較演算子
</div>

<div id="equals-function">
  ### equals 関数
</div>

`a = b` – `equals(a, b)` 関数を表します。

`a == b` – `equals(a, b)` 関数を表します。

<div id="notequals-function">
  ### notEquals 関数
</div>

`a != b` – `notEquals(a, b)` 関数。

`a <> b` – `notEquals(a, b)` 関数。

<div id="lessorequals-function">
  ### lessOrEquals 関数
</div>

`a <= b` — `lessOrEquals(a, b)` 関数。

<div id="greaterorequals-function">
  ### greaterOrEquals 関数
</div>

`a >= b` – `greaterOrEquals(a, b)` 関数を表します。

<div id="less-function">
  ### less 関数
</div>

`a < b` – `less(a, b)` 関数です。

<div id="greater-function">
  ### greater 関数
</div>

`a > b` – `greater(a, b)` 関数。

<div id="like-function">
  ### like 関数
</div>

`a LIKE b` – `like(a, b)` 関数。

<div id="notlike-function">
  ### notLike 関数
</div>

`a NOT LIKE b` – `notLike(a, b)` 関数。

<div id="ilike-function">
  ### ilike 関数
</div>

`a ILIKE b` – `ilike(a, b)` 関数です。

<div id="between-function">
  ### BETWEEN 関数
</div>

`a BETWEEN b AND c` – `a >= b AND a <= c` と同じです。

`a NOT BETWEEN b AND c` – `a < b OR a > c` と同じです。

<div id="is-not-distinct-from">
  ### is not distinct from 演算子 (`<=>`)
</div>

:::note
25.10 以降では、`<=>` を他の演算子と同様に使用できます。
25.10 より前は、JOIN 式でしか使用できませんでした。たとえば次のとおりです。

```sql
CREATE TABLE a (x String) ENGINE = Memory;
INSERT INTO a VALUES ('ClickHouse');

SELECT * FROM a AS a1 JOIN a AS a2 ON a1.x <=> a2.x;

┌─x──────────┬─a2.x───────┐
│ ClickHouse │ ClickHouse │
└────────────┴────────────┘
```

:::

`<=>` 演算子は、`NULL` セーフな等価演算子で、`IS NOT DISTINCT FROM` と同等です。
通常の等価演算子 (`=`) と同様に動作しますが、`NULL` 値も比較対象として扱います。
2 つの `NULL` 値は等しいものと見なされ、`NULL` を `NULL` 以外の任意の値と比較した場合は、`NULL` ではなく 0 (false) を返します。

```sql
SELECT
  'ClickHouse' <=> NULL,
  NULL <=> NULL
```

```response
┌─isNotDistinc⋯use', NULL)─┬─isNotDistinc⋯NULL, NULL)─┐
│                        0 │                        1 │
└──────────────────────────┴──────────────────────────┘
```

<div id="operators-for-working-with-strings">
  ## Stringを操作する演算子
</div>

<div id="overlay">
  ### OVERLAY
</div>

* `OVERLAY(string PLACING replacement FROM offset)` - `overlay(string, replacement, offset)` 関数
* `OVERLAY(string PLACING replacement FROM offset FOR length)` - `overlay(string, replacement, offset, length)` 関数
* `OVERLAYUTF8(string PLACING replacement FROM offset)` - `overlayUTF8(string, replacement, offset)` 関数
* `OVERLAYUTF8(string PLACING replacement FROM offset FOR length)` - `overlayUTF8(string, replacement, offset, length)` 関数

<div id="operators-for-working-with-data-sets">
  ## データセットを扱う演算子
</div>

[IN 演算子](../../sql-reference/operators/in.md)および[EXISTS](../../sql-reference/operators/exists.md)演算子を参照してください。

<div id="in-function">
  ### in 関数
</div>

`a IN ...` – `in(a, b)` 関数です。

<div id="notin-function">
  ### notIn 関数
</div>

`a NOT IN ...` — `notIn(a, b)` 関数です。

<div id="globalin-function">
  ### globalIn 関数
</div>

`a GLOBAL IN ...` – `globalIn(a, b)` 関数。

<div id="globalnotin-function">
  ### globalNotIn 関数
</div>

`a GLOBAL NOT IN ...` — `globalNotIn(a, b)` 関数です。

<div id="in-subquery-function">
  ### in サブクエリ 関数
</div>

`a = ANY (subquery)` – `in(a, subquery)` 関数です。

<div id="notin-subquery-function">
  ### notIn サブクエリ function
</div>

`a != ANY (subquery)` – `a NOT IN (SELECT singleValueOrNull(*) FROM subquery)` と同等です。

<div id="in-subquery-function-1">
  ### in サブクエリ関数
</div>

`a = ALL (subquery)` – `a IN (SELECT singleValueOrNull(*) FROM subquery)` と同じです。

<div id="notin-subquery-function">
  ### notIn サブクエリ function
</div>

`a != ALL (subquery)` – `notIn(a, subquery)` 関数です。

**例**

ALL を使用するクエリ:

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ALL (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

ANY を使ったクエリ：

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ANY (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 4 │
│ 5 │
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

<div id="some-all-on-arrays">
  ### 配列に対する `SOME` / `ALL`
</div>

前述のサブクエリ形式に加えて、`SOME` / `ALL` の右辺には配列式 (配列リテラル、配列型のカラム、または配列を返す任意の式) を指定できます。これは PostgreSQL スタイルの配列量化子構文です。これは parse 時に認識され、配列関数に書き換えられるため、手動で書き換える必要はありません。

| 構文                                      | 書き換え後                              |
| --------------------------------------- | ---------------------------------- |
| `expr = SOME(arr)`                      | `has(arr, expr)`                   |
| `expr <> ALL(arr)`                      | `NOT has(arr, expr)`               |
| `expr OP SOME(arr)` (サポートされるその他の任意の演算子) | `arrayExists(x -> expr OP x, arr)` |
| `expr OP ALL(arr)` (サポートされるその他の任意の演算子)  | `arrayAll(x -> expr OP x, arr)`    |

`SOME` は存在量化子です (SQL における `ANY` の同義語) 。`=` と `<>` は最適化された実装があるため、特別に `has` / `NOT has` に書き換えられます。一般形では高階関数の `arrayExists` / `arrayAll` が使われます。

配列形式は、比較演算子 `=`, `==`, `!=`, `<>`, `<=>`, `<`, `<=`, `>`, `>=`、キーワード比較述語 `IS DISTINCT FROM` と `IS NOT DISTINCT FROM`、および文字列検索述語 `LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE`, `REGEXP` で認識されます。キーワード比較述語と文字列検索述語が認識されるのは配列形式のみで、サブクエリ形式では認識されません (こちらは `IN`/`NOT IN` に変換されます) 。配列量化子としての意味を持たない演算子 (たとえば `IN` 自体) は**書き換えられず**、通常の意味のまま扱われます。

文字列検索述語が機能するのは、`MatchImpl` (`LIKE` / `ILIKE` / `REGEXP` の実装) が、定数の haystack と非定数の needle をサポートしているためです。たとえば、`'abc' LIKE SOME(['a%', 'b%'])` は `arrayExists(x -> 'abc' LIKE x, ['a%', 'b%'])` に、`'abc' NOT LIKE ALL(['x%', 'y%'])` は `arrayAll(x -> 'abc' NOT LIKE x, ['x%', 'y%'])` に書き換えられます。これにより、1 つの文字列を複数のパターンに対して照合できます。1 回の結合パスで照合したい場合は、`multiMatchAny` (正規表現) や `multiSearchAny` (部分文字列) などの複数パターン検索関数も引き続き使用できます。

:::note 配列形式では `ANY` はサポートされていません
右辺に配列を指定できるのは `SOME` と `ALL` だけです。`ANY` は `any` という aggregate function でもあるため、`expr = any(x)` という形の式では関数呼び出しとして解釈されます。配列量化子には `SOME` を使用してください。
:::

```sql title="Query"
SELECT
    3 = SOME([1, 2, 3, 4])         AS in_array,
    5 < SOME([1, 2, 6])            AS less_than_some,
    5 > ALL([1, 2, 3])             AS greater_than_all,
    'abc' LIKE SOME(['a%', 'z%'])  AS like_some;
```

```text title="Response"
┌─in_array─┬─less_than_some─┬─greater_than_all─┬─like_some─┐
│        1 │              1 │                1 │         1 │
└──────────┴────────────────┴──────────────────┴───────────┘
```

:::note `NULL` の扱いはサブクエリ形式とは異なります
配列形式はパーサー内で書き換えられるため (この段階では `transform_null_in` などのクエリ設定は利用できず、また行ごとの配列カラムではアナライザの null-safe な `IN` パスも使用できません) 、`has` の二値セマンティクス (`=` / `<>` の場合) と `arrayExists` / `arrayAll` (`NULL` 比較の不明な結果を `0` に畳み込む) を使用します。そのため、`NULL` の扱いはサブクエリ形式と異なる場合があります。サブクエリ形式では、`NULL` の扱いは `IN` / `NOT IN` を通じて処理され、`transform_null_in` に依存します:

```sql
SELECT NULL = SOME([NULL]);   -- has([NULL], NULL)                  -> 1
SELECT NULL <> ALL([NULL]);   -- NOT has([NULL], NULL)              -> 0
SELECT NULL < SOME([1]);      -- arrayExists(x -> NULL < x, [1])    -> 0
SELECT NULL > ALL([1]);       -- arrayAll(x -> NULL > x, [1])       -> 0
```

:::

<div id="operators-for-working-with-dates-and-times">
  ## 日付と時刻を扱うための演算子
</div>

<div id="extract">
  ### EXTRACT
</div>

```sql
EXTRACT(part FROM date);
```

指定した日付から要素を抽出します。たとえば、指定した日付から月を、時刻から秒を取得できます。

`part` パラメータは、日付のどの要素を取得するかを指定します。使用できる値は次のとおりです。

* `NANOSECOND` — ナノ秒。設定可能な値: 0–999999999。
* `MICROSECOND` — マイクロ秒。設定可能な値: 0–999999。
* `MILLISECOND` — ミリ秒。設定可能な値: 0–999。
* `SECOND` — 秒。設定可能な値: 0–59。
* `MINUTE` — 分。設定可能な値: 0–59。
* `HOUR` — 時。設定可能な値: 0–23。
* `DAY` — 日。設定可能な値: 1–31。
* `WEEK` — ISO 8601 の週番号。設定可能な値: 1–53。
* `MONTH` — 月番号。設定可能な値: 1–12。
* `QUARTER` — 四半期。設定可能な値: 1–4。
* `YEAR` — 年。
* `EPOCH` — Unix timestamp (1970-01-01 00:00:00 UTC からの秒数) 。注: `DateTime64` では秒未満の部分は切り捨てられます。
* `DOW` — 曜日 (PostgreSQL 互換) 。0 = 日曜日、6 = 土曜日。
* `DOY` — 年内通算日。設定可能な値: 1–366。
* `ISODOW` — ISO の曜日。1 = 月曜日、7 = 日曜日。
* `ISOYEAR` — ISO 8601 の週番号ベースの年。
* `CENTURY` — 世紀。たとえば、2024 年は 21 世紀です。
* `DECADE` — 十年単位 (年を 10 で割った値) 。たとえば、2024 年の decade は 202 です。
* `MILLENNIUM` — 千年紀。たとえば、2024 年は第 3 千年紀です。
* `TIMEZONE_HOUR` — オペランドのタイムゾーンの UTC オフセットにおける、符号付きの時の部分。たとえば、`+5:30` は `5` を返し、`-3:30` は `-3` を返します。
* `TIMEZONE_MINUTE` — オペランドのタイムゾーンの UTC オフセットにおける、符号付きの分の部分。たとえば、`+5:30` は `30` を返し、`-3:30` は `-30` を返します。

`part` パラメータでは大文字と小文字は区別されません。

`date` パラメータは処理する値を指定します。[Date](../../sql-reference/data-types/date.md)、[Date32](../../sql-reference/data-types/date32.md)、[DateTime](../../sql-reference/data-types/datetime.md)、[DateTime64](../../sql-reference/data-types/datetime64.md)、および [Interval](../../sql-reference/data-types/special-data-types/interval.md) 型がサポートされます。`date` が `Interval` の場合、要求された `part` はその Interval に格納されている kind と一致している必要があります (たとえば、`EXTRACT(DAY FROM INTERVAL 5 DAY)` は許可されますが、`EXTRACT(HOUR FROM INTERVAL 5 DAY)` は拒否されます。これは ClickHouse の interval が単一の kind だからです) 。`Interval` オペランドの結果は `Int64` です。

例:

```sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
SELECT EXTRACT(EPOCH FROM toDateTime('2024-01-15 12:30:45', 'UTC'));
SELECT EXTRACT(DOW FROM toDate('2024-01-15'));
SELECT EXTRACT(CENTURY FROM toDate('2024-01-01'));
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 5
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 30
SELECT EXTRACT(DAY   FROM INTERVAL 40 DAY);                                                -- 40
SELECT EXTRACT(MONTH FROM INTERVAL 7 MONTH);                                               -- 7
```

次の例では、テーブルを作成し、`DateTime` 型の値を挿入します。

```sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
) ENGINE = MergeTree
ORDER BY ();
```

```sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

```sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

```text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

より多くの例は[tests](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql)で確認できます。

<div id="interval">
  ### INTERVAL
</div>

算術演算で [Date](../../sql-reference/data-types/date.md) 型および [DateTime](../../sql-reference/data-types/datetime.md) 型の値とともに使用する、[Interval](../../sql-reference/data-types/special-data-types/interval.md) 型の値を作成します。

インターバルの型:

* `SECOND`
* `MINUTE`
* `HOUR`
* `DAY`
* `WEEK`
* `MONTH`
* `QUARTER`
* `YEAR`

`INTERVAL` の値を設定する際には、文字列リテラルを使用することもできます。たとえば、`INTERVAL 1 HOUR` は `INTERVAL '1 hour'` または `INTERVAL '1' hour` と同じです。

:::tip
型の異なるインターバルは組み合わせられません。`INTERVAL 4 DAY 1 HOUR` のような式は使用できません。インターバルは、その最小単位と同じかそれより小さい単位で指定してください。たとえば `INTERVAL 25 HOUR` です。以下の例のように、連続した演算を使用できます。
:::

例:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:09:50 │                                    2020-11-08 01:09:50 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4 day' + INTERVAL '3 hour';
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:12:10 │                                    2020-11-08 01:12:10 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4' day + INTERVAL '3' hour;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay('4')), toIntervalHour('3'))─┐
│ 2020-11-03 22:33:19 │                                        2020-11-08 01:33:19 │
└─────────────────────┴────────────────────────────────────────────────────────────┘
```

:::note
常に `INTERVAL` 構文または `addDays` 関数を使用することを推奨します。単純な加算や減算 (`now() + ...` のような構文) では、時刻の設定が考慮されません。たとえば、夏時間などです。
:::

例:

```sql
SELECT toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul') AS time, time + 60 * 60 * 24 AS time_plus_24_hours, time + toIntervalDay(1) AS time_plus_1_day;
```

```text
┌────────────────time─┬──time_plus_24_hours─┬─────time_plus_1_day─┐
│ 2014-10-26 00:00:00 │ 2014-10-26 23:00:00 │ 2014-10-27 00:00:00 │
└─────────────────────┴─────────────────────┴─────────────────────┘
```

**関連項目**

* [Interval](../../sql-reference/data-types/special-data-types/interval.md) データ型
* [toInterval](/ja/sql-reference/functions/type-conversion-functions#toIntervalYear) 型変換関数

<div id="date-time-addition">
  ### 日付と時刻の加算
</div>

[Date](../../sql-reference/data-types/date.md) または [Date32](../../sql-reference/data-types/date32.md) の値には、`+` 演算子を使って [Time](../../sql-reference/data-types/time.md) または [Time64](../../sql-reference/data-types/time64.md) の値を加算できます。結果は、その日の指定時刻を表す [DateTime](../../sql-reference/data-types/datetime.md) または [DateTime64](../../sql-reference/data-types/datetime64.md) になります。この操作は可換です。

結果の型は、オペランドの型によって異なります。

| 左オペランド   | 右オペランド      | 結果の型            |
| -------- | ----------- | --------------- |
| `Date`   | `Time`      | `DateTime`      |
| `Date`   | `Time64(s)` | `DateTime64(s)` |
| `Date32` | `Time`      | `DateTime64(0)` |
| `Date32` | `Time64(s)` | `DateTime64(s)` |

:::note
結果には、[セッションのタイムゾーン](../../operations/settings/settings.md#session_timezone) が使用されます (セッションのタイムゾーンが設定されていない場合は、server のデフォルトのタイムゾーンが使用されます) 。[`date_time_overflow_behavior`](../../operations/settings/settings-formats.md#date_time_overflow_behavior) 設定は、結果が表現可能な範囲外になった場合の動作を制御します。
:::

例:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') AS dt, toTypeName(dt);
```

```text
┌──────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25 │ DateTime       │
└─────────────────────┴────────────────┘
```

```sql
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
┌─────────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
└────────────────────────────┴────────────────┘
```

```sql
SELECT toTime64('23:59:59.999', 3) + toDate32('2024-07-15') AS dt, toTypeName(dt);
```

```text
┌──────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 23:59:59.999 │ DateTime64(3)  │
└─────────────────────────┴────────────────┘
```

<div id="at-time-zone">
  ### AT TIME ZONE と AT LOCAL
</div>

後置演算子 `AT TIME ZONE` と `AT LOCAL` は、`DateTime` または `DateTime64` の値を別のタイムゾーンへ変換します。これらは既存の [`toTimeZone`](/ja/sql-reference/functions/date-time-functions#totimezone) 関数のシンタックスシュガーです。

| 構文                       | 等価な式                           |
| ------------------------ | ------------------------------ |
| `expr AT TIME ZONE zone` | `toTimeZone(expr, zone)`       |
| `expr AT LOCAL`          | `toTimeZone(expr, timeZone())` |

`zone` には、有効なタイムゾーン名として評価される任意の定数文字列式を指定できます (例: `'America/Denver'`、`'UTC'`、`concat('America', '/', 'Denver')`) 。`AT TIME ZONE` は `toTimeZone` に展開されるため、タイムゾーン引数についても同じルールが適用されます。カラム参照のような非定数式を使う場合は、[`allow_nonconst_timezone_arguments = 1`](../../operations/settings/settings.md#allow_nonconst_timezone_arguments) が必要です。

`AT LOCAL` は現在の[セッションタイムゾーン](../../operations/settings/settings.md#session_timezone)を使用します (セッションタイムゾーンが設定されていない場合は、server のデフォルトが使用されます) 。`Distributed` tables では、`session_timezone` を明示的に設定する必要があります。これが空の場合、`timeZone()` は分片ごとのローカル値となるため、定数の `toTimeZone` 引数としては使用できず、`ILLEGAL_COLUMN` 例外が発生します。

:::note
PostgreSQL では、`timestamp without time zone AT TIME ZONE zone` は、変換前に壁時計上の値を指定したタイムゾーンの時刻として再解釈します。一方、ClickHouse では常に同じ絶対時刻が維持され、表示に使うタイムゾーンのラベルだけが変更されます。どちらの形式も `toTimeZone` と等価であり、基になる timestamp は変更しません。
:::

`AT TIME ZONE` の演算子の優先順位は 13 です (12 の `*`/`/`/`%` より高く、11 の `+`/`-` よりも高い) 。これは PostgreSQL と同じです。つまり、`a * ts AT TIME ZONE 'tz'` は `a * (ts AT TIME ZONE 'tz')` として結び付けられ、`ts + interval AT TIME ZONE 'tz'` は `ts + (interval AT TIME ZONE 'tz')` として結び付けられます。算術演算の後にタイムゾーン変換を適用するには、明示的に括弧を付けてください。

```sql
-- Explicit parens required to add first, then convert timezone
SELECT (TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR) AT TIME ZONE 'America/Denver';
-- Equivalent to:
SELECT toTimeZone(TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR, 'America/Denver');
```

例:

```sql
SET session_timezone = 'UTC';

SELECT TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE 'America/Denver';
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), 'America/Denver')─┐
│ 2001-02-16 13:38:40                                              │
└──────────────────────────────────────────────────────────────────┘
```

```sql
SELECT TIMESTAMP '2001-02-16 20:38:40' AT LOCAL;
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), timeZone())─┐
│ 2001-02-16 20:38:40                                        │
└────────────────────────────────────────────────────────────┘
```

**関連項目**

* [`toTimeZone`](/ja/sql-reference/functions/date-time-functions#totimezone)
* [`timeZone`](/ja/sql-reference/functions/date-time-functions#timezone)

<div id="logical-and-operator">
  ## 論理 AND 演算子
</div>

構文 `SELECT a AND b` — 関数 [and](/ja/sql-reference/functions/logical-functions#and) を使用して、`a` と `b` の論理積を計算します。

<div id="logical-or-operator">
  ## 論理 OR 演算子
</div>

構文 `SELECT a OR b` — 関数 [or](/ja/sql-reference/functions/logical-functions#or) により、`a` と `b` の論理和を計算します。

<div id="logical-negation-operator">
  ## 論理否定演算子
</div>

構文 `SELECT NOT a` — 関数 [not](/ja/sql-reference/functions/logical-functions#not) を使って、`a` の論理否定を計算します。

<div id="conditional-operator">
  ## 条件演算子
</div>

`a ? b : c` – `if(a, b, c)` 関数です。

注:

条件演算子はまず b と c の値を計算し、次に条件 a を評価して、対応する値を返します。`b` または `C` が [arrayJoin()](/ja/sql-reference/functions/array-join) 関数である場合、`a` の条件に関係なく各行が複製されます。

<div id="conditional-expression">
  ## 条件式
</div>

```sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

`x` が指定されている場合は、`transform(x, [a, ...], [b, ...], c)` 関数が使用されます。指定されていない場合は、`multiIf(a, b, ..., c)` が使用されます。

式に `ELSE c` 句がない場合、デフォルト値は `NULL` です。

`transform` 関数は `NULL` では動作しません。

<div id="concatenation-operator">
  ## 連結演算子
</div>

`s1 || s2` – `concat(s1, s2) 関数。`

<div id="lambda-creation-operator">
  ## ラムダ生成演算子
</div>

`x -> expr` – `lambda(x, expr)` 関数。

以下の演算子は括弧であるため、優先順位はありません:

<div id="array-creation-operator">
  ## Array 作成演算子
</div>

`[x1, ...]` – `array(x1, ...) function.` を意味します。

<div id="tuple-creation-operator">
  ## Tuple 作成演算子
</div>

`(x1, x2, ...)` – `tuple(x2, x2, ...)` 関数。

<div id="associativity">
  ## 結合性
</div>

すべての二項演算子は左結合です。たとえば、`1 + 2 + 3` は `plus(plus(1, 2), 3)` に変換されます。
ただし、これは必ずしも期待どおりには動作しません。たとえば、`SELECT 4 > 2 > 3` の結果は 0 になります。

効率上の理由から、`and` 関数と `or` 関数は任意の数の引数を受け付けます。対応する `AND` 演算子と `OR` 演算子の連鎖は、これらの関数への単一の呼び出しに変換されます。

<div id="checking-for-null">
  ## `NULL` かどうかの判定
</div>

ClickHouse は `IS NULL` 演算子と `IS NOT NULL` 演算子をサポートしています。

<div id="is_null">
  ### IS NULL
</div>

* [Nullable](../../sql-reference/data-types/nullable.md) 型の値に対して、`IS NULL` 演算子は次を返します。
  * 値が `NULL` の場合は `1`。
  * それ以外の場合は `0`。
* その他の値に対しては、`IS NULL` 演算子は常に `0` を返します。

[optimize&#95;functions&#95;to&#95;subcolumns](/ja/operations/settings/settings#optimize_functions_to_subcolumns) 設定を有効にすると、この処理を最適化できます。`optimize_functions_to_subcolumns = 1` の場合、この関数はカラム全体のカラムデータを読み取って処理する代わりに、[null](../../sql-reference/data-types/nullable.md#finding-null) サブカラムだけを読み取ります。クエリ `SELECT n IS NULL FROM table` は `SELECT n.null FROM TABLE` に変換されます。

{/* */ }

```sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

```text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

<div id="is_not_null">
  ### IS NOT NULL
</div>

* [Nullable](../../sql-reference/data-types/nullable.md) 型の値に対して、`IS NOT NULL` 演算子は次の値を返します。
  * 値が `NULL` の場合は `0`
  * それ以外の場合は `1`
* その他の値に対しては、`IS NOT NULL` 演算子は常に `1` を返します。

{/* */ }

```sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

```text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

[optimize&#95;functions&#95;to&#95;subcolumns](/ja/operations/settings/settings#optimize_functions_to_subcolumns) 設定を有効にすることで最適化できます。`optimize_functions_to_subcolumns = 1` の場合、この関数はカラム全体のカラムデータを読み取って処理する代わりに、[null](../../sql-reference/data-types/nullable.md#finding-null) サブカラムのみを読み取ります。クエリ `SELECT n IS NOT NULL FROM table` は `SELECT NOT n.null FROM TABLE` に変換されます。

<div id="checking-boolean-values">
  ## ブール値の判定
</div>

ClickHouse は、`IS TRUE`、`IS FALSE`、`IS UNKNOWN`、`IS NOT TRUE`、`IS NOT FALSE`、`IS NOT UNKNOWN` の各演算子をサポートしています。
これらは [Bool](../../sql-reference/data-types/boolean.md) および `Nullable(Bool)` の式に対して使用されます。

* `expr IS TRUE` は、`expr` が `true` の場合にのみ `1` を返します。
* `expr IS FALSE` は、`expr` が `false` の場合にのみ `1` を返します。
* `expr IS UNKNOWN` は、`expr` が `NULL` の場合にのみ `1` を返します。
* `expr IS NOT TRUE` は、`expr` が `false` または `NULL` の場合に `1` を返します。
* `expr IS NOT FALSE` は、`expr` が `true` または `NULL` の場合に `1` を返します。
* `expr IS NOT UNKNOWN` は、`expr` が `NULL` ではない場合に `1` を返します。

ブール式では、`IS UNKNOWN` は `IS NULL` と等価であり、`IS NOT UNKNOWN` は `IS NOT NULL` と等価です。

{/* */ }

```sql
CREATE TABLE t_bool (x Nullable(Bool)) ENGINE = Memory;
INSERT INTO t_bool VALUES (true), (false), (NULL);

SELECT
    x,
    x IS TRUE,
    x IS FALSE,
    x IS UNKNOWN,
    x IS NOT TRUE,
    x IS NOT FALSE,
    x IS NOT UNKNOWN
FROM t_bool;
```