---
description: '运算符文档'
sidebar_label: '运算符'
sidebar_position: 38
slug: /sql-reference/operators/
title: '运算符'
doc_type: 'reference'
---

ClickHouse 会在查询解析阶段根据运算符的优先级、优先次序和结合性，将其转换为相应的函数。

<div id="access-operators">
  ## 访问运算符
</div>

`a[N]` – 访问数组元素。`arrayElement(a, N)` 函数。

`a.N` – 访问元组元素。`tupleElement(a, N)` 函数。

<div id="numeric-negation-operator">
  ## 数值取负运算符
</div>

`-a` – `negate (a)` 函数。

对于元组取负：[tupleNegate](../../sql-reference/functions/tuple-functions.md#tupleNegate)。

<div id="multiplication-and-division-operators">
  ## 乘法和除法运算符
</div>

`a * b` – `multiply(a, b)` 函数。

如需将 Tuple 与数值相乘，请参阅 [tupleMultiplyByNumber](../../sql-reference/functions/tuple-functions.md#tupleMultiplyByNumber)；如需计算标量积，请参阅 [dotProduct](/zh/sql-reference/functions/array-functions#arrayDotProduct)。

`a / b` – `divide(a, b)` 函数。

如需将 Tuple 与数值相除，请参阅 [tupleDivideByNumber](../../sql-reference/functions/tuple-functions.md#tupleDivideByNumber)。

`a % b` – `modulo(a, b)` 函数。

<div id="addition-and-subtraction-operators">
  ## 加法和减法运算符
</div>

`a + b` – `plus(a, b)` 函数。

对于 Tuple 的加法，请参见：[tuplePlus](../../sql-reference/functions/tuple-functions.md#tuplePlus)。

`a - b` – `minus(a, b)` 函数。

对于 Tuple 的减法，请参见：[tupleMinus](../../sql-reference/functions/tuple-functions.md#tupleMinus)。

<div id="comparison-operators">
  ## 比较运算符
</div>

<div id="equals-function">
  ### equals 函数
</div>

`a = b` – `equals(a, b)` 函数。

`a == b` – `equals(a, b)` 函数。

<div id="notequals-function">
  ### `notEquals` 函数
</div>

`a != b` – 表示 `notEquals(a, b)` 函数。

`a <> b` – 表示 `notEquals(a, b)` 函数。

<div id="lessorequals-function">
  ### lessOrEquals 函数
</div>

`a <= b` — `lessOrEquals(a, b)` 函数。

<div id="greaterorequals-function">
  ### greaterOrEquals 函数
</div>

`a >= b` —— `greaterOrEquals(a, b)` 函数。

<div id="less-function">
  ### less 函数
</div>

`a < b` —— `less(a, b)` 函数。

<div id="greater-function">
  ### greater 函数
</div>

`a > b` —— `greater(a, b)` 函数。

<div id="like-function">
  ### like 函数
</div>

`a LIKE b` —— `like(a, b)` 函数。

<div id="notlike-function">
  ### notLike 函数
</div>

`a NOT LIKE b` — 即 `notLike(a, b)` 函数。

<div id="ilike-function">
  ### ilike 函数
</div>

`a ILIKE b` – 即 `ilike(a, b)` 函数。

<div id="between-function">
  ### BETWEEN 函数
</div>

`a BETWEEN b AND c` – 与 `a >= b AND a <= c` 相同。

`a NOT BETWEEN b AND c` – 与 `a < b OR a > c` 相同。

<div id="is-not-distinct-from">
  ### is not distinct from 运算符 (`<=>`)
</div>

:::note
从 25.10 起，你可以像使用其他运算符一样使用 `<=>`。
在 25.10 之前，它只能用于 JOIN 表达式，例如：

```sql
CREATE TABLE a (x String) ENGINE = Memory;
INSERT INTO a VALUES ('ClickHouse');

SELECT * FROM a AS a1 JOIN a AS a2 ON a1.x <=> a2.x;

┌─x──────────┬─a2.x───────┐
│ ClickHouse │ ClickHouse │
└────────────┴────────────┘
```

:::

`<=>` 运算符是对 `NULL` 安全的相等运算符，等价于 `IS NOT DISTINCT FROM`。
它的行为与普通相等运算符 (`=`) 类似，但会将 `NULL` 值视为可比较的值。
两个 `NULL` 值被视为相等，而 `NULL` 与任何非 `NULL` 值比较时，返回 0 (false) 而不是 `NULL`。

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
  ## 字符串操作运算符
</div>

<div id="overlay">
  ### OVERLAY
</div>

* `OVERLAY(string PLACING replacement FROM offset)` - `overlay(string, replacement, offset)` 函数。
* `OVERLAY(string PLACING replacement FROM offset FOR length)` - `overlay(string, replacement, offset, length)` 函数。
* `OVERLAYUTF8(string PLACING replacement FROM offset)` - `overlayUTF8(string, replacement, offset)` 函数。
* `OVERLAYUTF8(string PLACING replacement FROM offset FOR length)` - `overlayUTF8(string, replacement, offset, length)` 函数。

<div id="operators-for-working-with-data-sets">
  ## 用于处理数据集的运算符
</div>

请参阅 [IN 运算符](../../sql-reference/operators/in.md) 和 [EXISTS](../../sql-reference/operators/exists.md) 运算符。

<div id="in-function">
  ### in 函数
</div>

`a IN ...` —— `in(a, b)` 函数。

<div id="notin-function">
  ### notIn 函数
</div>

`a NOT IN ...` —— `notIn(a, b)` 函数。

<div id="globalin-function">
  ### globalIn 函数
</div>

`a GLOBAL IN ...` – `globalIn(a, b)` 函数。

<div id="globalnotin-function">
  ### globalNotIn 函数
</div>

`a GLOBAL NOT IN ...` – 即 `globalNotIn(a, b)` 函数。

<div id="in-subquery-function">
  ### in 子查询函数
</div>

`a = ANY (subquery)` —— 即 `in(a, subquery)` 函数。

<div id="notin-subquery-function">
  ### notIn 子查询函数
</div>

`a != ANY (subquery)`——等同于 `a NOT IN (SELECT singleValueOrNull(*) FROM subquery)`。

<div id="in-subquery-function">
  ### in 子查询函数
</div>

`a = ALL (subquery)` – 等同于 `a IN (SELECT singleValueOrNull(*) FROM subquery)`。

<div id="notin-subquery-function">
  ### notIn 子查询函数
</div>

`a != ALL (subquery)` — `notIn(a, subquery)` 函数。

**示例**

带有 ALL 的查询：

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

使用 ANY 的查询：

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
  ### 数组上的 `SOME` / `ALL`
</div>

除了上文所述的子查询形式外，`SOME` / `ALL` 的右侧也可以是数组表达式 (数组字面量、数组类型的列，或任何返回数组的表达式) 。这是 PostgreSQL 风格的数组量词语法。它会在解析阶段被识别并重写为数组函数，因此无需手动重写：

| 语法                                | 重写为                                |
| --------------------------------- | ---------------------------------- |
| `expr = SOME(arr)`                | `has(arr, expr)`                   |
| `expr <> ALL(arr)`                | `NOT has(arr, expr)`               |
| `expr OP SOME(arr)` (任何其他受支持的运算符) | `arrayExists(x -> expr OP x, arr)` |
| `expr OP ALL(arr)` (任何其他受支持的运算符)  | `arrayAll(x -> expr OP x, arr)`    |

`SOME` 是存在量词 (即 SQL 中 `ANY` 的同义词) 。`=` 和 `<>` 会被特殊重写为 `has` / `NOT has`，因为它们有优化实现；通用形式则会退回到高阶函数 `arrayExists` / `arrayAll`。

对于比较运算符 `=`, `==`, `!=`, `<>`, `<=>`, `<`, `<=`, `>`, `>=`，关键字比较谓词 `IS DISTINCT FROM` 和 `IS NOT DISTINCT FROM`，以及字符串搜索谓词 `LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE`, 和 `REGEXP`，系统都会识别其数组形式。关键字比较谓词和字符串搜索谓词仅对数组形式生效，不适用于子查询形式 (后者会被降为 `IN`/`NOT IN`) 。没有数组量词语义的运算符——例如 `IN` 本身——**不会**被重写，并保留其通常含义。

字符串搜索谓词之所以可用，是因为 `MatchImpl` (`LIKE` / `ILIKE` / `REGEXP` 背后的实现) 支持常量 haystack 搭配非常量 needle。例如，`'abc' LIKE SOME(['a%', 'b%'])` 会被重写为 `arrayExists(x -> 'abc' LIKE x, ['a%', 'b%'])`，而 `'abc' NOT LIKE ALL(['x%', 'y%'])` 会被重写为 `arrayAll(x -> 'abc' NOT LIKE x, ['x%', 'y%'])`。这相当于用多个 pattern 去匹配同一个字符串；如果想通过一次组合扫描完成匹配，仍可使用多 pattern 搜索函数，例如 `multiMatchAny` (正则表达式) 或 `multiSearchAny` (子字符串) 。

:::note `ANY` 不支持数组形式
只有 `SOME` 和 `ALL` 接受数组作为右侧参数。`ANY` 被排除在外，因为 `any` 同时也是一个 aggregate function，因此形态为 `expr = any(x)` 的表达式会保留其函数调用含义。数组量词请使用 `SOME`。
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

:::note `NULL` 处理与子查询形式不同
由于数组形式会在 parser 中被重写 (此时诸如 `transform_null_in` 之类的查询设置尚不可用，而且按行变化的数组列也无法走 analyzer 的 NULL 安全 `IN` 路径) ，因此它采用 `has` (用于 `=` / `<>`) 以及 `arrayExists` / `arrayAll` 的二值语义 (它们会将未知的 `NULL` 比较结果归并为 `0`) 。这可能与子查询形式有所不同；后者对 `NULL` 的处理会经由 `IN` / `NOT IN` 下推，并取决于 `transform_null_in`：

```sql
SELECT NULL = SOME([NULL]);   -- has([NULL], NULL)                  -> 1
SELECT NULL <> ALL([NULL]);   -- NOT has([NULL], NULL)              -> 0
SELECT NULL < SOME([1]);      -- arrayExists(x -> NULL < x, [1])    -> 0
SELECT NULL > ALL([1]);       -- arrayAll(x -> NULL > x, [1])       -> 0
```

:::

<div id="operators-for-working-with-dates-and-times">
  ## 日期和时间运算符
</div>

<div id="extract">
  ### EXTRACT
</div>

```sql
EXTRACT(part FROM date);
```

从给定的日期中提取某个部分。例如，可以从日期中取出月份，或从时间中取出秒。

`part` 参数用于指定要提取日期的哪个部分。可用值如下：

* `NANOSECOND` — 纳秒。可能的值：0–999999999。
* `MICROSECOND` — 微秒。可能的值：0–999999。
* `MILLISECOND` — 毫秒。可能的值：0–999。
* `SECOND` — 秒。可能的值：0–59。
* `MINUTE` — 分钟。可能的值：0–59。
* `HOUR` — 小时。可能的值：0–23。
* `DAY` — 一个月中的第几天。可能的值：1–31。
* `WEEK` — ISO 8601 周数。可能的值：1–53。
* `MONTH` — 月份编号。可能的值：1–12。
* `QUARTER` — 季度。可能的值：1–4。
* `YEAR` — 年份。
* `EPOCH` — Unix timestamp (自 1970-01-01 00:00:00 UTC 起的秒数) 。注意：对于 `DateTime64`，亚秒部分会被截断。
* `DOW` — 一周中的第几天 (与 PostgreSQL 兼容) 。0 = 星期日，6 = 星期六。
* `DOY` — 一年中的第几天。可能的值：1–366。
* `ISODOW` — ISO 一周中的第几天。1 = 星期一，7 = 星期日。
* `ISOYEAR` — ISO 8601 周编号年份。
* `CENTURY` — 世纪。例如，2024 年属于 21 世纪。
* `DECADE` — 十年期 (年份除以 10) 。例如，2024 年的十年期值为 202。
* `MILLENNIUM` — 千年。例如，2024 年属于第 3 个千年。
* `TIMEZONE_HOUR` — 操作数时区的 UTC 偏移中的有符号小时部分。例如，`+5:30` 返回 `5`，`-3:30` 返回 `-3`。
* `TIMEZONE_MINUTE` — 操作数时区的 UTC 偏移中的有符号分钟部分。例如，`+5:30` 返回 `30`，`-3:30` 返回 `-30`。

`part` 参数不区分大小写。

`date` 参数指定要处理的值。支持 [Date](../../sql-reference/data-types/date.md)、[Date32](../../sql-reference/data-types/date32.md)、[DateTime](../../sql-reference/data-types/datetime.md)、[DateTime64](../../sql-reference/data-types/datetime64.md) 和 [Interval](../../sql-reference/data-types/special-data-types/interval.md) 类型。当 `date` 为 `Interval` 时，请求的 `part` 必须与该 Interval 存储的 kind 一致 (例如，允许 `EXTRACT(DAY FROM INTERVAL 5 DAY)`；`EXTRACT(HOUR FROM INTERVAL 5 DAY)` 会被拒绝，因为 ClickHouse 的 interval 仅支持单一 kind) 。`Interval` 操作数的结果为 `Int64`。

示例：

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

在下面的示例中，我们将创建一个表，并向其中插入一个 `DateTime` 类型的值。

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

你可以在[测试](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql)中查看更多示例。

<div id="interval">
  ### INTERVAL
</div>

创建一个 [Interval](../../sql-reference/data-types/special-data-types/interval.md) 类型的值，用于与 [Date](../../sql-reference/data-types/date.md) 和 [DateTime](../../sql-reference/data-types/datetime.md) 类型的值进行算术运算。

时间间隔类型：

* `SECOND`
* `MINUTE`
* `HOUR`
* `DAY`
* `WEEK`
* `MONTH`
* `QUARTER`
* `YEAR`

设置 `INTERVAL` 值时，也可以使用字符串字面量。例如，`INTERVAL 1 HOUR` 与 `INTERVAL '1 hour'` 或 `INTERVAL '1' hour` 等价。

:::tip
不同类型的时间间隔不能组合使用。你不能使用 `INTERVAL 4 DAY 1 HOUR` 这样的表达式。请使用小于或等于该时间间隔最小单位的单位来指定时间间隔，例如 `INTERVAL 25 HOUR`。你也可以像下面的示例那样连续使用多个操作。
:::

示例：

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
始终建议优先使用 `INTERVAL` 语法或 `addDays` 函数。简单的加减运算 (如 `now() + ...` 这样的写法) 不会考虑时间设置，例如夏令时。
:::

示例：

```sql
SELECT toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul') AS time, time + 60 * 60 * 24 AS time_plus_24_hours, time + toIntervalDay(1) AS time_plus_1_day;
```

```text
┌────────────────time─┬──time_plus_24_hours─┬─────time_plus_1_day─┐
│ 2014-10-26 00:00:00 │ 2014-10-26 23:00:00 │ 2014-10-27 00:00:00 │
└─────────────────────┴─────────────────────┴─────────────────────┘
```

**另请参阅**

* [Interval](../../sql-reference/data-types/special-data-types/interval.md) 数据类型
* [toInterval](/zh/sql-reference/functions/type-conversion-functions#toIntervalYear) 类型转换函数

<div id="date-time-addition">
  ### 日期和时间相加
</div>

[Date](../../sql-reference/data-types/date.md) 或 [Date32](../../sql-reference/data-types/date32.md) 值可以通过 `+` 运算符与 [Time](../../sql-reference/data-types/time.md) 或 [Time64](../../sql-reference/data-types/time64.md) 值相加。结果为 [DateTime](../../sql-reference/data-types/datetime.md) 或 [DateTime64](../../sql-reference/data-types/datetime64.md)，表示该日期中给定时刻的日期时间值。该操作满足交换律。

结果类型取决于操作数类型：

| 左操作数     | 右操作数        | 结果类型            |
| -------- | ----------- | --------------- |
| `Date`   | `Time`      | `DateTime`      |
| `Date`   | `Time64(s)` | `DateTime64(s)` |
| `Date32` | `Time`      | `DateTime64(0)` |
| `Date32` | `Time64(s)` | `DateTime64(s)` |

:::note
结果使用[会话时区](../../operations/settings/settings.md#session_timezone) (如果未设置会话时区，则使用服务器默认时区) 。[`date_time_overflow_behavior`](../../operations/settings/settings-formats.md#date_time_overflow_behavior) 设置用于控制结果超出可表示范围时的处理方式。
:::

示例：

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
  ### AT TIME ZONE 和 AT LOCAL
</div>

后缀运算符 `AT TIME ZONE` 和 `AT LOCAL` 用于将 `DateTime` 或 `DateTime64` 值转换到其他时区。它们是现有 [`toTimeZone`](/zh/sql-reference/functions/date-time-functions#totimezone) 函数的语法糖：

| Syntax                   | Equivalent                     |
| ------------------------ | ------------------------------ |
| `expr AT TIME ZONE zone` | `toTimeZone(expr, zone)`       |
| `expr AT LOCAL`          | `toTimeZone(expr, timeZone())` |

`zone` 可以是任何求值结果为有效时区名称的常量字符串表达式 (例如 `'America/Denver'`、`'UTC'` 或 `concat('America', '/', 'Denver')`) 。由于 `AT TIME ZONE` 会被展开为 `toTimeZone`，因此同样适用时区参数规则：列引用等非常量表达式需要设置 [`allow_nonconst_timezone_arguments = 1`](../../operations/settings/settings.md#allow_nonconst_timezone_arguments)。

`AT LOCAL` 使用当前[会话时区](../../operations/settings/settings.md#session_timezone) (如果未设置会话时区，则使用服务器默认时区) 。在 `Distributed` 表上，必须显式设置 `session_timezone`；当其为空时，`timeZone()` 是分片本地的，不能作为常量 `toTimeZone` 参数使用，因此会导致 `ILLEGAL_COLUMN` 异常。

:::note
与 PostgreSQL 不同，在 PostgreSQL 中，`timestamp without time zone AT TIME ZONE zone` 会先将 wall-clock 值重新解释为给定时区中的时间，再进行转换；而 ClickHouse 始终保持相同的绝对时间点，只会更改用于显示的时区标签。这两种形式都等同于 `toTimeZone`，不会更改底层时间戳。
:::

`AT TIME ZONE` 的运算符优先次序为 13 (高于 12 的 `*`/`/`/`%`，也高于 11 的 `+`/`-`) ，与 PostgreSQL 一致。这意味着 `a * ts AT TIME ZONE 'tz'` 会结合为 `a * (ts AT TIME ZONE 'tz')`，而 `ts + interval AT TIME ZONE 'tz'` 会结合为 `ts + (interval AT TIME ZONE 'tz')`。若要在算术运算后再应用时区转换，请使用显式括号：

```sql
-- Explicit parens required to add first, then convert timezone
SELECT (TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR) AT TIME ZONE 'America/Denver';
-- Equivalent to:
SELECT toTimeZone(TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR, 'America/Denver');
```

示例：

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

**另请参见**

* [`toTimeZone`](/zh/sql-reference/functions/date-time-functions#totimezone)
* [`timeZone`](/zh/sql-reference/functions/date-time-functions#timezone)

<div id="logical-and-operator">
  ## 逻辑与运算符
</div>

语法 `SELECT a AND b` — 使用函数 [and](/zh/sql-reference/functions/logical-functions#and) 对 `a` 和 `b` 进行逻辑合取运算。

<div id="logical-or-operator">
  ## 逻辑或运算符
</div>

语法 `SELECT a OR b` — 使用函数 [or](/zh/sql-reference/functions/logical-functions#or) 计算 `a` 与 `b` 的逻辑析取。

<div id="logical-negation-operator">
  ## 逻辑非运算符
</div>

语法 `SELECT NOT a` — 使用函数 [not](/zh/sql-reference/functions/logical-functions#not) 对 `a` 进行逻辑非运算。

<div id="conditional-operator">
  ## 条件运算符
</div>

`a ? b : c` – `if(a, b, c)` 函数。

注意：

条件运算符会先计算 b 和 c 的值，再检查条件 a 是否成立，然后返回对应的值。如果 `b` 或 `C` 是 [arrayJoin()](/zh/sql-reference/functions/array-join) 函数，则无论条件 “a” 是否成立，每一行都会被复制。

<div id="conditional-expression">
  ## 条件表达式
</div>

```sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

如果指定了 `x`，则使用 `transform(x, [a, ...], [b, ...], c)` 函数；否则使用 `multiIf(a, b, ..., c)`。

如果表达式中没有 `ELSE c` 子句，则默认值为 `NULL`。

`transform` 函数不能处理 `NULL`。

<div id="concatenation-operator">
  ## 拼接运算符
</div>

`s1 || s2` – `concat(s1, s2)` 函数。

<div id="lambda-creation-operator">
  ## Lambda 创建运算符
</div>

`x -> expr` – 即 `lambda(x, expr)` 函数。

以下运算符没有优先级，因为它们是括号：

<div id="array-creation-operator">
  ## Array 构造运算符
</div>

`[x1, ...]` – `array(x1, ...)` 函数。

<div id="tuple-creation-operator">
  ## Tuple 构造运算符
</div>

`(x1, x2, ...)` – `tuple(x2, x2, ...) 函数。`

<div id="associativity">
  ## 结合性
</div>

所有二元运算符都具有左结合性。例如，`1 + 2 + 3` 会被转换为 `plus(plus(1, 2), 3)`。
但有时其行为可能不符合你的预期。例如，`SELECT 4 > 2 > 3` 的结果将是 0。

为了提高效率，`and` 和 `or` 函数可接受任意数量的参数。相应的 `AND` 和 `OR` 运算符链会被转换为对这些函数的一次调用。

<div id="checking-for-null">
  ## 检查 `NULL` 值
</div>

ClickHouse 支持 `IS NULL` 和 `IS NOT NULL` 运算符。

<div id="is_null">
  ### IS NULL
</div>

* 对于 [Nullable](../../sql-reference/data-types/nullable.md) 类型的值，`IS NULL` 运算符的返回结果为：
  * 如果值为 `NULL`，则返回 `1`。
  * 否则返回 `0`。
* 对于其他值，`IS NULL` 运算符始终返回 `0`。

可通过启用 [optimize&#95;functions&#95;to&#95;subcolumns](/zh/operations/settings/settings#optimize_functions_to_subcolumns) 设置进行优化。当 `optimize_functions_to_subcolumns = 1` 时，该函数只会读取 [null](../../sql-reference/data-types/nullable.md#finding-null) 子列，而不是读取并处理整个列数据。查询 `SELECT n IS NULL FROM table` 会被转换为 `SELECT n.null FROM TABLE`。

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

* 对于 [Nullable](../../sql-reference/data-types/nullable.md) 类型的值，`IS NOT NULL` 运算符的返回结果为：
  * 如果该值为 `NULL`，则返回 `0`。
  * 否则返回 `1`。
* 对于其他类型的值，`IS NOT NULL` 运算符始终返回 `1`。

{/* */ }

```sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

```text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

可以通过启用 [optimize&#95;functions&#95;to&#95;subcolumns](/zh/operations/settings/settings#optimize_functions_to_subcolumns) 设置进行优化。启用 `optimize_functions_to_subcolumns = 1` 后，该函数只会读取 [null](../../sql-reference/data-types/nullable.md#finding-null) 子列，而无需读取和处理整列数据。查询 `SELECT n IS NOT NULL FROM table` 会被转换为 `SELECT NOT n.null FROM TABLE`。

<div id="checking-boolean-values">
  ## 检查布尔值
</div>

ClickHouse 支持 `IS TRUE`、`IS FALSE`、`IS UNKNOWN`、`IS NOT TRUE`、`IS NOT FALSE` 和 `IS NOT UNKNOWN` 运算符。
它们可用于 [Bool](../../sql-reference/data-types/boolean.md) 和 `Nullable(Bool)` 表达式。

* `expr IS TRUE` 仅当 `expr` 为 `true` 时返回 `1`。
* `expr IS FALSE` 仅当 `expr` 为 `false` 时返回 `1`。
* `expr IS UNKNOWN` 仅当 `expr` 为 `NULL` 时返回 `1`。
* `expr IS NOT TRUE` 在 `expr` 为 `false` 或 `NULL` 时返回 `1`。
* `expr IS NOT FALSE` 在 `expr` 为 `true` 或 `NULL` 时返回 `1`。
* `expr IS NOT UNKNOWN` 在 `expr` 不为 `NULL` 时返回 `1`。

对于布尔表达式，`IS UNKNOWN` 等同于 `IS NULL`，`IS NOT UNKNOWN` 等同于 `IS NOT NULL`。

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