# 操作符 {#cao-zuo-fu}

所有的操作符（运算符）都会在查询时依据他们的优先级及其结合顺序在被解析时转换为对应的函数。下面按优先级从高到低列出各组运算符及其对应的函数：

## 下标运算符 {#xia-biao-yun-suan-fu}

`a[N]` – 数组中的第N个元素; 对应函数 `arrayElement(a, N)`

`a.N` – 元组中第N个元素; 对应函数 `tupleElement(a, N)`

## 负号 {#fu-hao}

`-a` – 对应函数 `negate(a)`

## 乘号、除号和取余 {#cheng-hao-chu-hao-he-qu-yu}

`a * b` – 对应函数 `multiply(a, b)`

`a / b` – 对应函数 `divide(a, b)`

`a % b` – 对应函数 `modulo(a, b)`

## 加号和减号 {#jia-hao-he-jian-hao}

`a + b` – 对应函数 `plus(a, b)`

`a - b` – 对应函数 `minus(a, b)`

## 关系运算符 {#guan-xi-yun-suan-fu}

`a = b` – 对应函数 `equals(a, b)`

`a == b` – 对应函数 `equals(a, b)`

`a != b` – 对应函数 `notEquals(a, b)`

`a <> b` – 对应函数 `notEquals(a, b)`

`a <= b` – 对应函数 `lessOrEquals(a, b)`

`a >= b` – 对应函数 `greaterOrEquals(a, b)`

`a < b` – 对应函数 `less(a, b)`

`a > b` – 对应函数 `greater(a, b)`

`a LIKE b` – 对应函数 `like(a, b)`

`a NOT LIKE b` – 对应函数 `notLike(a, b)`

`a BETWEEN b AND c` – 等价于 `a >= b AND a <= c`

## 集合关系运算符 {#ji-he-guan-xi-yun-suan-fu}

*详见此节 [IN 相关操作符](in.md#select-in-operators) 。*

`a IN ...` – 对应函数 `in(a, b)`

`a NOT IN ...` – 对应函数 `notIn(a, b)`

`a GLOBAL IN ...` – 对应函数 `globalIn(a, b)`

`a GLOBAL NOT IN ...` – 对应函数 `globalNotIn(a, b)`

## 逻辑非 {#luo-ji-fei}

`NOT a` – 对应函数 `not(a)`

## 逻辑与 {#luo-ji-yu}

`a AND b` – 对应函数`and(a, b)`

## 逻辑或 {#luo-ji-huo}

`a OR b` – 对应函数 `or(a, b)`

## 条件运算符 {#tiao-jian-yun-suan-fu}

`a ? b : c` – 对应函数 `if(a, b, c)`

注意:

条件运算符会先计算表达式b和表达式c的值，再根据表达式a的真假，返回相应的值。如果表达式b和表达式c是 [arrayJoin()](../../sql-reference/functions/array-join.md#functions_arrayjoin) 函数，则不管表达式a是真是假，每行都会被复制展开。

## 使用日期和时间的操作员 {#operators-datetime}

### EXTRACT {#operator-extract}

``` sql
EXTRACT(part FROM date);
```

从给定日期中提取部件。 例如，您可以从给定日期检索一个月，或从时间检索一秒钟。

该 `part` 参数指定要检索的日期部分。 以下值可用:

-   `DAY` — The day of the month. Possible values: 1–31.
-   `MONTH` — The number of a month. Possible values: 1–12.
-   `YEAR` — The year.
-   `SECOND` — The second. Possible values: 0–59.
-   `MINUTE` — The minute. Possible values: 0–59.
-   `HOUR` — The hour. Possible values: 0–23.

该 `part` 参数不区分大小写。

该 `date` 参数指定要处理的日期或时间。 无论是 [日期](../../sql-reference/data-types/date.md) 或 [日期时间](../../sql-reference/data-types/datetime.md) 支持类型。

例:

``` sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

在下面的例子中，我们创建一个表，并在其中插入一个值 `DateTime` 类型。

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

你可以看到更多的例子 [测试](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#operator-interval}

创建一个 [间隔](../../sql-reference/operators/index.md)-应在算术运算中使用的类型值 [日期](../../sql-reference/data-types/date.md) 和 [日期时间](../../sql-reference/data-types/datetime.md)-类型值。

示例:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

**另请参阅**

-   [间隔](../../sql-reference/operators/index.md) 数据类型
-   [toInterval](../../sql-reference/operators/index.md#function-tointerval) 类型转换函数

## CASE条件表达式 {#operator_case}

``` sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

如果指定了 `x` ，该表达式会转换为 `transform(x, [a, ...], [b, ...], c)` 函数。否则转换为 `multiIf(a, b, ..., c)`

如果该表达式中没有 `ELSE c` 子句，则默认值就是 `NULL`

但 `transform` 函数不支持 `NULL` <!-- If `x` is NULL, return NULL; if `c` is NULL, work fine. -->

## 连接运算符 {#lian-jie-yun-suan-fu}

`s1 || s2` – 对应函数 `concat(s1, s2)`

## 创建 Lambda 函数 {#chuang-jian-lambda-han-shu}

`x -> expr` – 对应函数 `lambda(x, expr)`

接下来的这些操作符因为其本身是括号没有优先级：

## 创建数组 {#chuang-jian-shu-zu}

`[x1, ...]` – 对应函数 `array(x1, ...)`

## 创建元组 {#chuang-jian-yuan-zu}

`(x1, x2, ...)` – 对应函数 `tuple(x2, x2, ...)`

## 结合方式 {#jie-he-fang-shi}

所有的同级操作符从左到右结合。例如， `1 + 2 + 3` 会转换成 `plus(plus(1, 2), 3)`。
所以，有时他们会跟我们预期的不太一样。例如， `SELECT 4 > 2 > 3` 的结果是0。

为了高效， `and` 和 `or` 函数支持任意多参数，一连串的 `AND` 和 `OR` 运算符会转换成其对应的单个函数。

## 判断是否为 `NULL` {#pan-duan-shi-fou-wei-null}

ClickHouse 支持 `IS NULL` 和 `IS NOT NULL` 。

### IS NULL {#operator-is-null}

-   对于 [可为空](../../sql-reference/operators/index.md) 类型的值， `IS NULL` 会返回：
    -   `1` 值为 `NULL`
    -   `0` 否则
-   对于其他类型的值， `IS NULL` 总会返回 `0`

<!-- -->

``` bash
:) SELECT x+100 FROM t_null WHERE y IS NULL

SELECT x + 100
FROM t_null
WHERE isNull(y)

┌─plus(x, 100)─┐
│          101 │
└──────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

### IS NOT NULL {#is-not-null}

-   对于 [可为空](../../sql-reference/operators/index.md) 类型的值， `IS NOT NULL` 会返回：
    -   `0` 值为 `NULL`
    -   `1` 否则
-   对于其他类型的值，`IS NOT NULL` 总会返回 `1`

<!-- -->

``` bash
:) SELECT * FROM t_null WHERE y IS NOT NULL

SELECT *
FROM t_null
WHERE isNotNull(y)

┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘

1 rows in set. Elapsed: 0.002 sec.
```

[来源文章](https://clickhouse.com/docs/en/query_language/operators/) <!--hide-->
