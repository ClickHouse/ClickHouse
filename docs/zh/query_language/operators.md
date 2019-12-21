# 操作符

所有的操作符（运算符）都会在查询时依据他们的优先级及其结合顺序在被解析时转换为对应的函数。下面按优先级从高到低列出各组运算符及其对应的函数：

## 下标运算符

`a[N]` – 数组中的第N个元素; 对应函数 `arrayElement(a, N)`

`a.N` – 元组中第N个元素; 对应函数 `tupleElement(a, N)`

## 负号

`-a`  – 对应函数 `negate(a)`

## 乘号、除号和取余

`a * b`  – 对应函数 `multiply(a, b)`

`a / b`  – 对应函数 `divide(a, b)`

`a % b` – 对应函数 `modulo(a, b)`

## 加号和减号

`a + b` – 对应函数 `plus(a, b)`

`a - b`  – 对应函数 `minus(a, b)`

## 关系运算符

`a = b` – 对应函数 `equals(a, b)`

`a == b` – 对应函数 `equals(a, b)`

`a != b` – 对应函数 `notEquals(a, b)`

`a <> b` – 对应函数 `notEquals(a, b)`

`a <= b` – 对应函数 `lessOrEquals(a, b)`

`a >= b` – 对应函数 `greaterOrEquals(a, b)`

`a < b` – 对应函数 `less(a, b)`

`a > b` – 对应函数 `greater(a, b)`

`a LIKE s` – 对应函数 `like(a, b)`

`a NOT LIKE s` – 对应函数 `notLike(a, b)`

`a BETWEEN b AND c` – 等价于 `a >= b AND a <= c` 

## 集合关系运算符

*详见此节 [IN 相关操作符](select.md#select-in-operators) 。*

`a IN ...` – 对应函数 `in(a, b)`

`a NOT IN ...` – 对应函数 `notIn(a, b)`

`a GLOBAL IN ...` – 对应函数 `globalIn(a, b)`

`a GLOBAL NOT IN ...` – 对应函数 `globalNotIn(a, b)`

## 逻辑非

`NOT a` – 对应函数 `not(a)`

## 逻辑与

`a AND b` – 对应函数`and(a, b)`

## 逻辑或

`a OR b` – 对应函数 `or(a, b)`

## 条件运算符

`a ? b : c` – 对应函数 `if(a, b, c)`

注意:

条件运算符会先计算表达式b和表达式c的值，再根据表达式a的真假，返回相应的值。如果表达式b和表达式c是 [arrayJoin()](functions/array_join.md#functions_arrayjoin) 函数，则不管表达式a是真是假，每行都会被复制展开。


## Operators for Working with Dates and Times {#operators-datetime}

### EXTRACT {#operator-extract}

```sql
EXTRACT(part FROM date);
```

Extracts a part from a given date. For example, you can retrieve a month from a given date, or a second from a time.

The `part` parameter specifies which part of the date to retrieve. The following values are available:

- `DAY` — The day of the month. Possible values: 1–31.
- `MONTH` — The number of a month. Possible values: 1–12.
- `YEAR` — The year.
- `SECOND` — The second. Possible values: 0–59.
- `MINUTE` — The minute. Possible values: 0–59.
- `HOUR` — The hour. Possible values: 0–23.

The `part` parameter is case-insensitive.

The `date` parameter specifies the date or the time to process. Either [Date](../data_types/date.md) or [DateTime](../data_types/datetime.md) type is supported.

Examples:

```sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

In the following example we create a table and insert into it a value with the `DateTime` type.

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

You can see more examples in [tests](https://github.com/ClickHouse/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#operator-interval}

Creates an [Interval](../data_types/special_data_types/interval.md)-type value that should be used in arithmetical operations with [Date](../data_types/date.md) and [DateTime](../data_types/datetime.md)-type values.

Example:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```
```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

**See Also**

- [Interval](../data_types/special_data_types/interval.md) data type
- [toInterval](functions/type_conversion_functions.md#function-tointerval) type convertion functions


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

## 连接运算符

`s1 || s2` – 对应函数 `concat(s1, s2)`

## 创建 Lambda 函数

`x -> expr` – 对应函数 `lambda(x, expr)`

接下来的这些操作符因为其本身是括号没有优先级：

## 创建数组

`[x1, ...]` – 对应函数 `array(x1, ...)`

## 创建元组

`(x1, x2, ...)` – 对应函数 `tuple(x2, x2, ...)`

## 结合方式

所有的同级操作符从左到右结合。例如， `1 + 2 + 3` 会转换成 `plus(plus(1, 2), 3)`。
所以，有时他们会跟我们预期的不太一样。例如， `SELECT 4 > 2 > 3` 的结果是0。

为了高效， `and` 和 `or` 函数支持任意多参数，一连串的 `AND` 和 `OR` 运算符会转换成其对应的单个函数。

## 判断是否为 `NULL`

ClickHouse 支持 `IS NULL` 和 `IS NOT NULL` 。

### IS NULL {#operator-is-null}

- 对于 [Nullable](../data_types/nullable.md) 类型的值， `IS NULL` 会返回：
    - `1` 值为 `NULL`
    - `0` 否则
- 对于其他类型的值， `IS NULL` 总会返回 `0`

```bash
:) SELECT x+100 FROM t_null WHERE y IS NULL

SELECT x + 100
FROM t_null
WHERE isNull(y)

┌─plus(x, 100)─┐
│          101 │
└──────────────┘

1 rows in set. Elapsed: 0.002 sec.
```


### IS NOT NULL

- 对于 [Nullable](../data_types/nullable.md) 类型的值， `IS NOT NULL` 会返回：
    - `0` 值为 `NULL`
    - `1` 否则
- 对于其他类型的值，`IS NOT NULL` 总会返回 `1`

```bash
:) SELECT * FROM t_null WHERE y IS NOT NULL

SELECT *
FROM t_null
WHERE isNotNull(y)

┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘

1 rows in set. Elapsed: 0.002 sec.
```

[来源文章](https://clickhouse.yandex/docs/en/query_language/operators/) <!--hide-->
