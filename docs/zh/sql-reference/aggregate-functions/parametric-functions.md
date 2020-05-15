---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "\u53C2\u6570"
---

# 参数聚合函数 {#aggregate_functions_parametric}

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## 直方图 {#histogram}

计算自适应直方图。 它不能保证精确的结果。

``` sql
histogram(number_of_bins)(values)
```

该函数使用 [流式并行决策树算法](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). 当新数据输入函数时，hist图分区的边界将被调整。 在通常情况下，箱的宽度不相等。

**参数**

`number_of_bins` — Upper limit for the number of bins in the histogram. The function automatically calculates the number of bins. It tries to reach the specified number of bins, but if it fails, it uses fewer bins.
`values` — [表达式](../syntax.md#syntax-expressions) 导致输入值。

**返回值**

-   [阵列](../../sql-reference/data-types/array.md) 的 [元组](../../sql-reference/data-types/tuple.md) 下面的格式:

        ```
        [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
        ```

        - `lower` — Lower bound of the bin.
        - `upper` — Upper bound of the bin.
        - `height` — Calculated height of the bin.

**示例**

``` sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

您可以使用 [酒吧](../../sql-reference/functions/other-functions.md#function-bar) 功能，例如:

``` sql
WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height,
    bar(height, 0, 6, 5) AS bar
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

在这种情况下，您应该记住您不知道直方图bin边界。

## sequenceMatch(pattern)(timestamp, cond1, cond2, …) {#function-sequencematch}

检查序列是否包含与模式匹配的事件链。

``` sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

!!! warning "警告"
    在同一秒钟发生的事件可能以未定义的顺序排列在序列中，影响结果。

**参数**

-   `pattern` — Pattern string. See [模式语法](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` 和 `DateTime`. 您还可以使用任何支持的 [UInt](../../sql-reference/data-types/int-uint.md) 数据类型。

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. 最多可以传递32个条件参数。 该函数只考虑这些条件中描述的事件。 如果序列包含未在条件中描述的数据，则函数将跳过这些数据。

**返回值**

-   1，如果模式匹配。
-   0，如果模式不匹配。

类型: `UInt8`.

<a name="sequence-function-pattern-syntax"></a>
**模式语法**

-   `(?N)` — Matches the condition argument at position `N`. 条件在编号 `[1, 32]` 范围。 例如, `(?1)` 匹配传递给 `cond1` 参数。

-   `.*` — Matches any number of events. You don't need conditional arguments to match this element of the pattern.

-   `(?t operator value)` — Sets the time in seconds that should separate two events. For example, pattern `(?1)(?t>1800)(?2)` 匹配彼此发生超过1800秒的事件。 这些事件之间可以存在任意数量的任何事件。 您可以使用 `>=`, `>`, `<`, `<=` 运营商。

**例**

考虑在数据 `t` 表:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

执行查询:

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

该函数找到了数字2跟随数字1的事件链。 它跳过了它们之间的数字3，因为该数字没有被描述为事件。 如果我们想在搜索示例中给出的事件链时考虑这个数字，我们应该为它创建一个条件。

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

在这种情况下，函数找不到与模式匹配的事件链，因为数字3的事件发生在1和2之间。 如果在相同的情况下，我们检查了数字4的条件，则序列将与模式匹配。

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**另请参阅**

-   [sequenceCount](#function-sequencecount)

## sequenceCount(pattern)(time, cond1, cond2, …) {#function-sequencecount}

计数与模式匹配的事件链的数量。 该函数搜索不重叠的事件链。 当前链匹配后，它开始搜索下一个链。

!!! warning "警告"
    在同一秒钟发生的事件可能以未定义的顺序排列在序列中，影响结果。

``` sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**参数**

-   `pattern` — Pattern string. See [模式语法](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` 和 `DateTime`. 您还可以使用任何支持的 [UInt](../../sql-reference/data-types/int-uint.md) 数据类型。

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. 最多可以传递32个条件参数。 该函数只考虑这些条件中描述的事件。 如果序列包含未在条件中描述的数据，则函数将跳过这些数据。

**返回值**

-   匹配的非重叠事件链数。

类型: `UInt64`.

**示例**

考虑在数据 `t` 表:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

计算数字2在数字1之后出现的次数以及它们之间的任何其他数字:

``` sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

**另请参阅**

-   [sequenceMatch](#function-sequencematch)

## windowFunnel {#windowfunnel}

搜索滑动时间窗中的事件链，并计算从链中发生的最大事件数。

该函数根据算法工作:

-   该函数搜索触发链中的第一个条件并将事件计数器设置为1的数据。 这是滑动窗口启动的时刻。

-   如果来自链的事件在窗口内顺序发生，则计数器将递增。 如果事件序列中断，则计数器不会增加。

-   如果数据在不同的完成点具有多个事件链，则该函数将仅输出最长链的大小。

**语法**

``` sql
windowFunnel(window, [mode])(timestamp, cond1, cond2, ..., condN)
```

**参数**

-   `window` — Length of the sliding window in seconds.
-   `mode` -这是一个可选的参数。
    -   `'strict'` -当 `'strict'` 设置时，windowFunnel()仅对唯一值应用条件。
-   `timestamp` — Name of the column containing the timestamp. Data types supported: [日期](../../sql-reference/data-types/date.md), [日期时间](../../sql-reference/data-types/datetime.md#data_type-datetime) 和其他无符号整数类型（请注意，即使时间戳支持 `UInt64` 类型，它的值不能超过Int64最大值，即2^63-1）。
-   `cond` — Conditions or data describing the chain of events. [UInt8](../../sql-reference/data-types/int-uint.md).

**返回值**

滑动时间窗口内连续触发条件链的最大数目。
对选择中的所有链进行了分析。

类型: `Integer`.

**示例**

确定设定的时间段是否足以让用户选择手机并在在线商店中购买两次。

设置以下事件链:

1.  用户登录到其在应用商店中的帐户 (`eventID = 1003`).
2.  用户搜索手机 (`eventID = 1007, product = 'phone'`).
3.  用户下了订单 (`eventID = 1009`).
4.  用户再次下订单 (`eventID = 1010`).

输入表:

``` text
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-28 │       1 │ 2019-01-29 10:00:00 │    1003 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-31 │       1 │ 2019-01-31 09:00:00 │    1007 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-30 │       1 │ 2019-01-30 08:00:00 │    1009 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-02-01 │       1 │ 2019-02-01 08:00:00 │    1010 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
```

了解用户有多远 `user_id` 可以在2019的1-2月期间通过链条。

查询:

``` sql
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM trend
    WHERE (event_date >= '2019-01-01') AND (event_date <= '2019-02-02')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC
```

结果:

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

## 保留 {#retention}

该函数将一组条件作为参数，类型为1到32个参数 `UInt8` 表示事件是否满足特定条件。
任何条件都可以指定为参数（如 [WHERE](../../sql-reference/statements/select/where.md#select-where)).

除了第一个以外，条件成对适用：如果第一个和第二个是真的，第二个结果将是真的，如果第一个和fird是真的，第三个结果将是真的，等等。

**语法**

``` sql
retention(cond1, cond2, ..., cond32);
```

**参数**

-   `cond` — an expression that returns a `UInt8` 结果（1或0）。

**返回值**

数组为1或0。

-   1 — condition was met for the event.
-   0 — condition wasn't met for the event.

类型: `UInt8`.

**示例**

让我们考虑计算的一个例子 `retention` 功能，以确定网站流量。

**1.** Сreate a table to illustrate an example.

``` sql
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

输入表:

查询:

``` sql
SELECT * FROM retention_test
```

结果:

``` text
┌───────date─┬─uid─┐
│ 2020-01-01 │   0 │
│ 2020-01-01 │   1 │
│ 2020-01-01 │   2 │
│ 2020-01-01 │   3 │
│ 2020-01-01 │   4 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-02 │   0 │
│ 2020-01-02 │   1 │
│ 2020-01-02 │   2 │
│ 2020-01-02 │   3 │
│ 2020-01-02 │   4 │
│ 2020-01-02 │   5 │
│ 2020-01-02 │   6 │
│ 2020-01-02 │   7 │
│ 2020-01-02 │   8 │
│ 2020-01-02 │   9 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-03 │   0 │
│ 2020-01-03 │   1 │
│ 2020-01-03 │   2 │
│ 2020-01-03 │   3 │
│ 2020-01-03 │   4 │
│ 2020-01-03 │   5 │
│ 2020-01-03 │   6 │
│ 2020-01-03 │   7 │
│ 2020-01-03 │   8 │
│ 2020-01-03 │   9 │
│ 2020-01-03 │  10 │
│ 2020-01-03 │  11 │
│ 2020-01-03 │  12 │
│ 2020-01-03 │  13 │
│ 2020-01-03 │  14 │
└────────────┴─────┘
```

**2.** 按唯一ID对用户进行分组 `uid` 使用 `retention` 功能。

查询:

``` sql
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

结果:

``` text
┌─uid─┬─r───────┐
│   0 │ [1,1,1] │
│   1 │ [1,1,1] │
│   2 │ [1,1,1] │
│   3 │ [1,1,1] │
│   4 │ [1,1,1] │
│   5 │ [0,0,0] │
│   6 │ [0,0,0] │
│   7 │ [0,0,0] │
│   8 │ [0,0,0] │
│   9 │ [0,0,0] │
│  10 │ [0,0,0] │
│  11 │ [0,0,0] │
│  12 │ [0,0,0] │
│  13 │ [0,0,0] │
│  14 │ [0,0,0] │
└─────┴─────────┘
```

**3.** 计算每天的现场访问总数。

查询:

``` sql
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
```

结果:

``` text
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

哪里:

-   `r1`-2020-01-01期间访问该网站的独立访问者数量（ `cond1` 条件）。
-   `r2`-在2020-01-01和2020-01-02之间的特定时间段内访问该网站的唯一访问者的数量 (`cond1` 和 `cond2` 条件）。
-   `r3`-在2020-01-01和2020-01-03之间的特定时间段内访问该网站的唯一访问者的数量 (`cond1` 和 `cond3` 条件）。

## uniqUpTo(N)(x) {#uniquptonx}

Calculates the number of different argument values ​​if it is less than or equal to N. If the number of different argument values is greater than N, it returns N + 1.

建议使用小Ns，高达10。 N的最大值为100。

对于聚合函数的状态，它使用的内存量等于1+N\*一个字节值的大小。
对于字符串，它存储8个字节的非加密哈希。 也就是说，计算是近似的字符串。

该函数也适用于多个参数。

它的工作速度尽可能快，除了使用较大的N值并且唯一值的数量略小于N的情况。

用法示例:

``` text
Problem: Generate a report that shows only keywords that produced at least 5 unique users.
Solution: Write in the GROUP BY query SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5
```

[原始文章](https://clickhouse.tech/docs/en/query_language/agg_functions/parametric_functions/) <!--hide-->

## sumMapFiltered(keys\_to\_keep)(键值) {#summapfilteredkeys-to-keepkeys-values}

同样的行为 [sumMap](reference.md#agg_functions-summap) 除了一个键数组作为参数传递。 这在使用高基数密钥时尤其有用。
