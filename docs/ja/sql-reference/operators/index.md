---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: "\u6F14\u7B97\u5B50"
---

# 演算子 {#operators}

ClickHouseは、優先順位、優先順位、および連想に従って、クエリ解析段階で演算子を対応する関数に変換します。

## アクセス演算子 {#access-operators}

`a[N]` – Access to an element of an array. The `arrayElement(a, N)` 機能。

`a.N` – Access to a tuple element. The `tupleElement(a, N)` 機能。

## 数値否定演算子 {#numeric-negation-operator}

`-a` – The `negate (a)` 機能。

## 乗算演算子と除算演算子 {#multiplication-and-division-operators}

`a * b` – The `multiply (a, b)` 機能。

`a / b` – The `divide(a, b)` 機能。

`a % b` – The `modulo(a, b)` 機能。

## 加算演算子と減算演算子 {#addition-and-subtraction-operators}

`a + b` – The `plus(a, b)` 機能。

`a - b` – The `minus(a, b)` 機能。

## 比較演算子 {#comparison-operators}

`a = b` – The `equals(a, b)` 機能。

`a == b` – The `equals(a, b)` 機能。

`a != b` – The `notEquals(a, b)` 機能。

`a <> b` – The `notEquals(a, b)` 機能。

`a <= b` – The `lessOrEquals(a, b)` 機能。

`a >= b` – The `greaterOrEquals(a, b)` 機能。

`a < b` – The `less(a, b)` 機能。

`a > b` – The `greater(a, b)` 機能。

`a LIKE s` – The `like(a, b)` 機能。

`a NOT LIKE s` – The `notLike(a, b)` 機能。

`a BETWEEN b AND c` – The same as `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – The same as `a < b OR a > c`.

## データセットを操作する演算子 {#operators-for-working-with-data-sets}

*見る [演算子で](in.md).*

`a IN ...` – The `in(a, b)` 機能。

`a NOT IN ...` – The `notIn(a, b)` 機能。

`a GLOBAL IN ...` – The `globalIn(a, b)` 機能。

`a GLOBAL NOT IN ...` – The `globalNotIn(a, b)` 機能。

## 日付と時刻を操作する演算子 {#operators-datetime}

### EXTRACT {#operator-extract}

``` sql
EXTRACT(part FROM date);
```

特定の日付から部品を抽出します。 たとえば、特定の日付から月、または時間から秒を取得できます。

その `part` パラメータ取得する日付のどの部分を指定します。 使用可能な値は次のとおりです:

-   `DAY` — The day of the month. Possible values: 1–31.
-   `MONTH` — The number of a month. Possible values: 1–12.
-   `YEAR` — The year.
-   `SECOND` — The second. Possible values: 0–59.
-   `MINUTE` — The minute. Possible values: 0–59.
-   `HOUR` — The hour. Possible values: 0–23.

その `part` パラメータは大文字と小文字を区別する.

その `date` パラメータ処理する日付または時刻を指定します。 どちらか [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md) タイプに対応しています。

例:

``` sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

次の例では、テーブルを作成し、そこに値を挿入します `DateTime` タイプ。

``` sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
)
ENGINE = Log;
```

``` sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

``` sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

``` text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

以下の例を見ることができます [テスト](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#operator-interval}

を作成します。 [間隔](../../sql-reference/data-types/special-data-types/interval.md)-算術演算で使用されるべき型の値 [日付](../../sql-reference/data-types/date.md) と [DateTime](../../sql-reference/data-types/datetime.md)-値を入力します。

間隔のタイプ:
- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `WEEK`
- `MONTH`
- `QUARTER`
- `YEAR`

!!! warning "警告"
    異なるタイプの間隔は結合できません。 次のような式は使用できません `INTERVAL 4 DAY 1 HOUR`. 間隔は、間隔の最小単位より小さい単位または等しい単位で指定します。, `INTERVAL 25 HOUR`. 次の例のように、連続した操作を使用できます。

例:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

**も参照。**

-   [間隔](../../sql-reference/data-types/special-data-types/interval.md) データ型
-   [toInterval](../../sql-reference/functions/type-conversion-functions.md#function-tointerval) 型変換関数

## 論理否定演算子 {#logical-negation-operator}

`NOT a` – The `not(a)` 機能。

## 論理And演算子 {#logical-and-operator}

`a AND b` – The`and(a, b)` 機能。

## 論理OR演算子 {#logical-or-operator}

`a OR b` – The `or(a, b)` 機能。

## 条件演算子 {#conditional-operator}

`a ? b : c` – The `if(a, b, c)` 機能。

注:

条件演算子は、bとcの値を計算し、条件aが満たされているかどうかをチェックし、対応する値を返します。 もし `b` または `C` は [アレイジョイン()](../../sql-reference/functions/array-join.md#functions_arrayjoin) 関数は、各行は関係なく、レプリケートされます “a” 条件だ

## 条件式 {#operator_case}

``` sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

もし `x` が指定される。 `transform(x, [a, ...], [b, ...], c)` function is used. Otherwise – `multiIf(a, b, ..., c)`.

がない場合 `ELSE c` 句式のデフォルト値は次のとおりです `NULL`.

その `transform` 関数は動作しません `NULL`.

## 連結演算子 {#concatenation-operator}

`s1 || s2` – The `concat(s1, s2) function.`

## ラムダ作成演算子 {#lambda-creation-operator}

`x -> expr` – The `lambda(x, expr) function.`

次の演算子は角かっこであるため、優先順位はありません:

## 配列作成演算子 {#array-creation-operator}

`[x1, ...]` – The `array(x1, ...) function.`

## タプル作成演算子 {#tuple-creation-operator}

`(x1, x2, ...)` – The `tuple(x2, x2, ...) function.`

## 連想性 {#associativity}

すべての二項演算子は連想性を残しています。 例えば, `1 + 2 + 3` に変換されます `plus(plus(1, 2), 3)`.
時にはこれはあなたが期待するように動作しません。 例えば, `SELECT 4 > 2 > 3` 結果は0になります。

効率のために、 `and` と `or` 関数は任意の数の引数を受け入れます。 の対応する鎖 `AND` と `OR` 演算子は、これらの関数の単一の呼び出しに変換されます。

## チェック `NULL` {#checking-for-null}

クリックハウスは `IS NULL` と `IS NOT NULL` 演算子。

### IS NULL {#operator-is-null}

-   のために [Null可能](../../sql-reference/data-types/nullable.md) 型の値は、 `IS NULL` 演算子の戻り値:
    -   `1` 値が `NULL`.
    -   `0` そうでなければ
-   その他の値については、 `IS NULL` 演算子は常に返します `0`.

<!-- -->

``` sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

``` text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

### IS NOT NULL {#is-not-null}

-   のために [Null可能](../../sql-reference/data-types/nullable.md) 型の値は、 `IS NOT NULL` 演算子の戻り値:
    -   `0` 値が `NULL`.
    -   `1` そうでなければ
-   その他の値については、 `IS NOT NULL` 演算子は常に返します `1`.

<!-- -->

``` sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

``` text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/operators/) <!--hide-->
