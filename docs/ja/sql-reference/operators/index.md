---
slug: /ja/sql-reference/operators/
sidebar_position: 38
sidebar_label: Operators
---

# Operators

ClickHouseは、演算子をその優先度、結合性、結びつきを考慮してクエリ解析時に対応する関数に変換します。

## アクセス演算子

`a[N]` – 配列要素へのアクセス。`arrayElement(a, N)`関数。

`a.N` – タプル要素へのアクセス。`tupleElement(a, N)`関数。

## 数値の否定演算子

`-a` – `negate(a)`関数。

タプルの否定については：[tupleNegate](../../sql-reference/functions/tuple-functions.md#tuplenegate)。

## 乗算と除算演算子

`a * b` – `multiply(a, b)`関数。

タプルに数値を乗じる場合：[tupleMultiplyByNumber](../../sql-reference/functions/tuple-functions.md#tuplemultiplybynumber)、スカラー積の場合：[dotProduct](../../sql-reference/functions/tuple-functions.md#dotproduct)。

`a / b` – `divide(a, b)`関数。

タプルを数値で除算する場合：[tupleDivideByNumber](../../sql-reference/functions/tuple-functions.md#tupledividebynumber)。

`a % b` – `modulo(a, b)`関数。

## 加算と減算演算子

`a + b` – `plus(a, b)`関数。

タプルの加算については：[tuplePlus](../../sql-reference/functions/tuple-functions.md#tupleplus)。

`a - b` – `minus(a, b)`関数。

タプルの減算については：[tupleMinus](../../sql-reference/functions/tuple-functions.md#tupleminus)。

## 比較演算子

### equals関数
`a = b` – `equals(a, b)`関数。

`a == b` – `equals(a, b)`関数。

### notEquals関数
`a != b` – `notEquals(a, b)`関数。

`a <> b` – `notEquals(a, b)`関数。

### lessOrEquals関数
`a <= b` – `lessOrEquals(a, b)`関数。

### greaterOrEquals関数
`a >= b` – `greaterOrEquals(a, b)`関数。

### less関数
`a < b` – `less(a, b)`関数。

### greater関数
`a > b` – `greater(a, b)`関数。

### like関数
`a LIKE s` – `like(a, b)`関数。

### notLike関数
`a NOT LIKE s` – `notLike(a, b)`関数。

### ilike関数
`a ILIKE s` – `ilike(a, b)`関数。

### BETWEEN関数
`a BETWEEN b AND c` – `a >= b AND a <= c`と同じ。

`a NOT BETWEEN b AND c` – `a < b OR a > c`と同じ。

## データセット操作用演算子

[IN演算子](../../sql-reference/operators/in.md)および[EXISTS](../../sql-reference/operators/exists.md)演算子を参照してください。

### in関数
`a IN ...` – `in(a, b)`関数。

### notIn関数
`a NOT IN ...` – `notIn(a, b)`関数。

### globalIn関数
`a GLOBAL IN ...` – `globalIn(a, b)`関数。

### globalNotIn関数
`a GLOBAL NOT IN ...` – `globalNotIn(a, b)`関数。

### inサブクエリ関数
`a = ANY (subquery)` – `in(a, subquery)`関数。  

### notInサブクエリ関数
`a != ANY (subquery)` – `a NOT IN (SELECT singleValueOrNull(*) FROM subquery)`と同じ。

### inサブクエリ関数
`a = ALL (subquery)` – `a IN (SELECT singleValueOrNull(*) FROM subquery)`と同じ。

### notInサブクエリ関数
`a != ALL (subquery)` – `notIn(a, subquery)`関数。 

**例**

ALLを使用したクエリ:

```sql
SELECT number AS a FROM numbers(10) WHERE a > ALL (SELECT number FROM numbers(3, 3));
```

結果:

```text
┌─a─┐
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

ANYを使用したクエリ:

```sql
SELECT number AS a FROM numbers(10) WHERE a > ANY (SELECT number FROM numbers(3, 3));
```

結果:

```text
┌─a─┐
│ 4 │
│ 5 │
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

## 日付および時刻の操作用演算子

### EXTRACT

```sql
EXTRACT(part FROM date);
```

日付から部分を抽出します。例えば、与えられた日付から月を取得したり、時刻から秒を取得できます。

`part`パラメーターは、取得したい日付の部分を指定します。以下の値が利用可能です：

- `DAY` — 月の日。可能な値：1〜31。
- `MONTH` — 月。可能な値：1〜12。
- `YEAR` — 年。
- `SECOND` — 秒。可能な値：0〜59。
- `MINUTE` — 分。可能な値：0〜59。
- `HOUR` — 時。可能な値：0〜23。

`part`パラメーターは大文字小文字を区別しません。

`date`パラメーターは、処理対象の日付または時刻を指定します。[Date](../../sql-reference/data-types/date.md)または[DateTime](../../sql-reference/data-types/datetime.md)型がサポートされます。

例：

```sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

次の例では、テーブルを作成し、そのテーブルに`DateTime`タイプの値を挿入しています。

```sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
)
ENGINE = Log;
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

追加の例は[テスト](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql)で確認できます。

### INTERVAL

[Date](../../sql-reference/data-types/date.md)および[DateTime](../../sql-reference/data-types/datetime.md)タイプとの算術操作で使用される[Interval](../../sql-reference/data-types/special-data-types/interval.md)タイプの値を作成します。

インターバルの種類：
- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `WEEK`
- `MONTH`
- `QUARTER`
- `YEAR`

`INTERVAL`の値を設定する際に文字列リテラルも使用できます。例えば、`INTERVAL 1 HOUR`は`INTERVAL '1 hour'`または`INTERVAL '1' hour`と同一です。

:::tip    
異なるタイプのインターバルは組み合わせることができません。`INTERVAL 4 DAY 1 HOUR`のような式は使用できません。インターバルを指定する際には、最小のインターバルユニットと同等またはそれより小さい単位を使用してください。例えば`INTERVAL 25 HOUR`など。以下の例のように逐次的な操作を行うことができます。
:::

例：

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
`INTERVAL`構文または`addDays`関数は常に優先されます。単純な加算または減算（例：`now() + ...`のような構文）は、時間の設定を考慮しません。例えば、夏時間など。
:::

例：

```sql
SELECT toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul') AS time, time + 60 * 60 * 24 AS time_plus_24_hours, time + toIntervalDay(1) AS time_plus_1_day;
```

```text
┌────────────────time─┬──time_plus_24_hours─┬─────time_plus_1_day─┐
│ 2014-10-26 00:00:00 │ 2014-10-26 23:00:00 │ 2014-10-27 00:00:00 │
└─────────────────────┴─────────────────────┴─────────────────────┘
```

**関連項目**

- [Interval](../../sql-reference/data-types/special-data-types/interval.md)データタイプ
- [toInterval](../../sql-reference/functions/type-conversion-functions.md#function-tointerval)タイプ変換関数

## 論理AND演算子

構文 `SELECT a AND b` — `a`と`b`の論理積を計算する関数[and](../../sql-reference/functions/logical-functions.md#logical-and-function)。

## 論理OR演算子

構文 `SELECT a OR b` — `a`と`b`の論理和を計算する関数[or](../../sql-reference/functions/logical-functions.md#logical-or-function)。

## 論理否定演算子

構文 `SELECT NOT a` — `a`の論理否定を計算する関数[not](../../sql-reference/functions/logical-functions.md#logical-not-function)。

## 条件演算子

`a ? b : c` – `if(a, b, c)`関数。

注意：

条件演算子は、bとcの値を計算した後、条件aが満たされているかを確認し、対応する値を返します。`b`または`c`が[arrayJoin()](../../sql-reference/functions/array-join.md#functions_arrayjoin)関数であるとき、a条件に関わらず各行が複製されます。

## 条件式

```sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

`x`が指定されている場合、`transform(x, [a, ...], [b, ...], c)`関数が使用されます。そうでなければ`multiIf(a, b, ..., c)`。

式中に`ELSE c`句がない場合、デフォルト値は`NULL`です。

`transform`関数は`NULL`を扱いません。

## 連結演算子

`s1 || s2` – `concat(s1, s2)関数。`

## ラムダ生成演算子

`x -> expr` – `lambda(x, expr)`関数。

以下の演算子は優先度を持たず、括弧です：

## 配列生成演算子

`[x1, ...]` – `array(x1, ...)`関数。

## タプル生成演算子

`(x1, x2, ...)` – `tuple(x1, x2, ...)`関数。

## 結合性

全ての二項演算子は左結合性を持ちます。例えば、`1 + 2 + 3`は`plus(plus(1, 2), 3)`に変換されます。このため、時には予想通りに動作しないことがあります。例えば、`SELECT 4 > 2 > 3`は0を返します。

効率化のため、`and`や`or`関数は任意の数の引数を受け付けることができます。対応する`AND`と`OR`演算子のチェーンはこれらの関数の単一の呼び出しに変換されます。

## `NULL`の確認

ClickHouseは`IS NULL`と`IS NOT NULL`演算子をサポートしています。

### IS NULL {#is_null}

- [Nullable](../../sql-reference/data-types/nullable.md)型の値の場合、`IS NULL`演算子は以下を返します：
    - 値が`NULL`の場合は`1`。
    - それ以外の場合は`0`。
- その他の値の場合、`IS NULL`演算子は常に`0`を返します。

[optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns)設定を有効にすることで最適化することができます。`optimize_functions_to_subcolumns = 1`の場合、関数は全カラムデータを読み取るのではなく、[null](../../sql-reference/data-types/nullable.md#finding-null)サブカラムのみを読み取ります。クエリ`SELECT n IS NULL FROM table`は`SELECT n.null FROM TABLE`に変換されます。

```sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

```text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

### IS NOT NULL {#is_not_null}

- [Nullable](../../sql-reference/data-types/nullable.md)型の値の場合、`IS NOT NULL`演算子は以下を返します：
    - 値が`NULL`の場合は`0`。
    - それ以外の場合は`1`。
- その他の値の場合、`IS NOT NULL`演算子は常に`1`を返します。

```sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

```text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

[optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns)設定を有効にすることで最適化することができます。`optimize_functions_to_subcolumns = 1`の場合、関数は全カラムデータを読み取るのではなく、[null](../../sql-reference/data-types/nullable.md#finding-null)サブカラムのみを読み取ります。クエリ`SELECT n IS NOT NULL FROM table`は`SELECT NOT n.null FROM TABLE`に変換されます。
