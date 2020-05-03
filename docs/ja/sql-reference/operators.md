---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 37
toc_title: "\u6F14\u7B97\u5B50"
---

# 演算子 {#operators}

すべての演算子は、優先順位と結合性に従って、クエリの解析段階で対応する関数に変換されます。
演算子のグループは優先順位の順にリストされます（リスト内の高いほど、演算子はその引数に接続されています）。

## アクセス事業者 {#access-operators}

`a[N]` – Access to an element of an array. The `arrayElement(a, N)` 機能。

`a.N` – Access to a tuple element. The `tupleElement(a, N)` 機能。

## 数値の否定の演算子 {#numeric-negation-operator}

`-a` – The `negate (a)` 機能。

## 乗算演算子と除算演算子 {#multiplication-and-division-operators}

`a * b` – The `multiply (a, b)` 機能。

`a / b` – The `divide(a, b)` 機能。

`a % b` – The `modulo(a, b)` 機能。

## 加算および減算演算子 {#addition-and-subtraction-operators}

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

*見る [演算子の場合](statements/select.md#select-in-operators).*

`a IN ...` – The `in(a, b)` 機能。

`a NOT IN ...` – The `notIn(a, b)` 機能。

`a GLOBAL IN ...` – The `globalIn(a, b)` 機能。

`a GLOBAL NOT IN ...` – The `globalNotIn(a, b)` 機能。

## 日付と時刻を操作する演算子 {#operators-datetime}

### EXTRACT {#operator-extract}

``` sql
EXTRACT(part FROM date);
```

指定した日付からパートを抽出します。 たとえば、指定した日付から月、または時刻から秒を取得できます。

その `part` パラメーター取得する日付の部分を指定します。 次の値を使用できます:

-   `DAY` — The day of the month. Possible values: 1–31.
-   `MONTH` — The number of a month. Possible values: 1–12.
-   `YEAR` — The year.
-   `SECOND` — The second. Possible values: 0–59.
-   `MINUTE` — The minute. Possible values: 0–59.
-   `HOUR` — The hour. Possible values: 0–23.

その `part` パラ

その `date` パラメーター処理する日付または時刻を指定します。 どちらか [日付](../sql-reference/data-types/date.md) または [DateTime](../sql-reference/data-types/datetime.md) タイプに対応しています。

例:

``` sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

次の例では、テーブルを作成し、その中に値を挿入します `DateTime` タイプ。

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

より多くの例を見ることができる [テスト](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#operator-interval}

を作成します。 [間隔](../sql-reference/data-types/special-data-types/interval.md)-との算術演算で使用されるべきであるタイプ値 [日付](../sql-reference/data-types/date.md) と [DateTime](../sql-reference/data-types/datetime.md)-タイプの値。

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
    間隔の異なる種類できない。 次のような式は使用できません `INTERVAL 4 DAY 1 HOUR`. たとえば、区間の最小単位が小さいか等しい単位で区間を表します `INTERVAL 25 HOUR`. 以下の例のように、結果としての操作を使用できます。

例えば:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

**また見なさい**

-   [間隔](../sql-reference/data-types/special-data-types/interval.md) データ型
-   [toInterval](../sql-reference/functions/type-conversion-functions.md#function-tointerval) 型変換関数

## 論理否定演算子 {#logical-negation-operator}

`NOT a` – The `not(a)` 機能。

## 論理and演算子 {#logical-and-operator}

`a AND b` – The`and(a, b)` 機能。

## 論理or演算子 {#logical-or-operator}

`a OR b` – The `or(a, b)` 機能。

## 条件演算子 {#conditional-operator}

`a ? b : c` – The `if(a, b, c)` 機能。

メモ:

条件演算子は、bとcの値を計算し、条件aが満たされているかどうかをチェックし、対応する値を返します。 もし `b` または `C` は [arrayJoin()](../sql-reference/functions/array-join.md#functions_arrayjoin) この関数は、各行は関係なくレプリケートされます。 “a” 条件。

## 条件式 {#operator_case}

``` sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

もし `x` が指定されたら、 `transform(x, [a, ...], [b, ...], c)` function is used. Otherwise – `multiIf(a, b, ..., c)`.

がない場合 `ELSE c` 式中の句は、デフォルト値は次のとおりです `NULL`.

その `transform` 機能は動作しません `NULL`.

## 連結演算子 {#concatenation-operator}

`s1 || s2` – The `concat(s1, s2) function.`

## ラムダ作成演算子 {#lambda-creation-operator}

`x -> expr` – The `lambda(x, expr) function.`

次の演算子は、角かっこであるため、優先順位がありません:

## 配列作成演算子 {#array-creation-operator}

`[x1, ...]` – The `array(x1, ...) function.`

## タプル作成演算子 {#tuple-creation-operator}

`(x1, x2, ...)` – The `tuple(x2, x2, ...) function.`

## 結合性 {#associativity}

すべての二項演算子は結合性を残しています。 例えば, `1 + 2 + 3` に変換される。 `plus(plus(1, 2), 3)`.
時にはこれはあなたが期待するように動作しません。 例えば, `SELECT 4 > 2 > 3` 結果は0になります。

効率のため、 `and` と `or` 関数は任意の数の引数を受け取ります。 対応する鎖の `AND` と `OR` 演算子は、これらの関数の単一の呼び出しに変換されます。

## のチェック `NULL` {#checking-for-null}

クリックハウスは `IS NULL` と `IS NOT NULL` 演算子。

### IS NULL {#operator-is-null}

-   のために [Nullable](../sql-reference/data-types/nullable.md) タイプ値は、 `IS NULL` 演算子の戻り値:
    -   `1` 値が `NULL`.
    -   `0` そうでなければ
-   その他の値については、 `IS NULL` 演算子は常に戻ります `0`.

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

-   のために [Nullable](../sql-reference/data-types/nullable.md) タイプ値は、 `IS NOT NULL` 演算子の戻り値:
    -   `0` 値が `NULL`.
    -   `1` そうでなければ
-   その他の値については、 `IS NOT NULL` 演算子は常に戻ります `1`.

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
