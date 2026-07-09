---
description: '参数化聚合函数文档'
sidebar_label: '参数化'
sidebar_position: 38
slug: /sql-reference/aggregate-functions/parametric-functions
title: '参数化聚合函数'
doc_type: 'reference'
---

某些聚合函数不仅可以接受参数列 (用于压缩) ，还可以接受一组参数——即用于初始化的常量。其语法使用两对括号，而不是一对。第一对用于参数，第二对用于参数列。

<div id="histogram">
  ## 直方图
</div>

计算自适应直方图。不保证结果的精确性。

```sql
histogram(number_of_bins)(values)
```

该函数使用 [A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf)。随着新数据进入函数，histogram 各个 bin 的边界会动态调整。通常情况下，各个 bin 的宽度并不相同。

**Arguments**

`values` — 产生输入值的[表达式](/zh/sql-reference/syntax#expressions)。

**Parameters**

`number_of_bins` — histogram 中 bin 数量的上限。函数会自动计算 bin 的数量，并尽量达到指定的 bin 数量；如果无法达到，则会使用更少的 bin。

**Returned values**

* 由以下格式的 [Array](../../sql-reference/data-types/array.md) 和 [Tuples](../../sql-reference/data-types/tuple.md) 组成的数组：

  ```
  [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
  ```

  * `lower` — bin 的下界。
  * `upper` — bin 的上界。
  * `height` — bin 的计算高度。

**Example**

```sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

```text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

例如，你可以使用 [bar](/zh/sql-reference/functions/other-functions#bar) 函数将直方图可视化：

```sql
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

```text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

在这种情况下，需要注意的是，你并不知道直方图分桶的边界。

<div id="sequencematch">
  ## sequenceMatch
</div>

检查序列中是否包含与模式匹配的事件链。

**语法**

```sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

:::note
发生在同一秒内的事件，在序列中的先后顺序可能未定义，因此会影响结果。
:::

**参数**

* `timestamp` — 视为包含时间数据的列。典型的数据类型为 `Date` 和 `日期时间`。你也可以使用任意受支持的 [UInt](../../sql-reference/data-types/int-uint.md) 数据类型。

* `cond1`, `cond2` — 用于描述事件链的条件。数据类型：`UInt8`。最多可传入 32 个条件参数。函数只会考虑这些条件中描述的事件；如果序列中包含未被任何条件描述的事件，函数会跳过它们。

**Parameters**

* `pattern` — 模式字符串。参见 [模式语法](#pattern-syntax)。

**返回值**

* 1，表示模式匹配。
* 0，表示模式不匹配。

类型：`UInt8`。

<div id="pattern-syntax">
  #### 模式语法
</div>

* `(?N)` — 匹配位置为 `N` 的 condition 参数。condition 的编号范围为 `[1, 32]`。例如，`(?1)` 匹配传递给 `cond1` 参数的条件。

* `.*` — 匹配任意数量的事件。匹配模式中的这一元素时，不需要条件参数。

* `(?t operator value)` — 设置两个事件之间应相隔的时间 (以秒为单位) 。例如，模式 `(?1)(?t>1800)(?2)` 匹配彼此间隔超过 1800 秒的事件。这两个事件之间可以有任意数量的其他事件。你可以使用 `>=`、`>`、`<`、`<=`、`==` 运算符。

**示例**

假设 `t` 表中的数据如下：

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

执行以下查询：

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

该函数找到了数字 2 紧跟在数字 1 之后的事件链。它跳过了两者之间的数字 3，因为这个数字未被定义为事件。如果我们想在搜索示例中给出的事件链时把这个数字也考虑进去，就应该为它设置一个条件。

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

在这种情况下，该函数找不到与模式匹配的事件链，因为编号为 3 的事件发生在 1 和 2 之间。如果在同样的情况下检查编号为 4 的条件，那么该序列就会匹配该模式。

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**另请参阅**

* [sequenceCount](#sequencecount)

<div id="sequencecount">
  ## sequenceCount
</div>

统计与模式匹配的事件链数量。该函数会查找彼此不重叠的事件链，并在当前事件链匹配后开始查找下一条事件链。

:::note
发生在同一秒内的事件在序列中的顺序可能是不确定的，这会影响结果。
:::

**语法**

```sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**参数**

* `timestamp` — 视为包含时间数据的列。常见的数据类型为 `Date` 和 `日期时间`。你也可以使用任一受支持的 [UInt](../../sql-reference/data-types/int-uint.md) 数据类型。

* `cond1`, `cond2` — 用于描述事件链的条件。数据类型：`UInt8`。最多可传递 32 个条件参数。该函数只会考虑这些条件中描述的事件。如果序列中包含未被任何条件描述的数据，函数会跳过它们。

**参数**

* `pattern` — 模式字符串。参见 [模式语法](#pattern-syntax)。

**返回值**

* 匹配到的不重叠事件链数量。

类型：`UInt64`。

**示例**

考虑 `t` 表中的数据：

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

统计数字 2 在数字 1 之后出现的次数，两者之间可以间隔任意数量的其他数字：

```sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

<div id="sequencematchevents">
  ## sequenceMatchEvents
</div>

返回与该模式匹配的最长事件链中各事件的时间戳。

:::note
发生在同一秒内的事件在序列中的顺序可能是未定义的，这会影响结果。
:::

**语法**

```sql
sequenceMatchEvents(pattern)(timestamp, cond1, cond2, ...)
```

**参数**

* `timestamp` — 视为包含时间数据的列。典型数据类型为 `Date` 和 `日期时间`。你也可以使用任何受支持的 [UInt](../../sql-reference/data-types/int-uint.md) 数据类型。

* `cond1`, `cond2` — 描述事件链的条件。数据类型：`UInt8`。最多可传入 32 个条件参数。函数只会考虑这些条件所描述的事件。如果序列中包含未被任何条件描述的数据，函数会跳过这些数据。

**参数**

* `pattern` — 模式字符串。参见 [模式语法](#pattern-syntax)。

**返回值**

* 返回事件链中与条件参数 (?N) 匹配的时间戳数组。数组中的位置与模式中条件参数的位置对应。

类型：Array。

**示例**

考虑 `t` 表中的数据：

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

返回最长事件链中各事件的时间戳

```sql
SELECT sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│ [1,3,4]                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**另请参阅**

* [sequenceMatch](#sequencematch)

<div id="windowfunnel">
  ## windowFunnel
</div>

在滑动时间窗口中查找事件链，并计算该链中已发生事件的最大数量。

该函数按以下算法工作：

* 该函数会查找满足事件链中第一个条件的数据，并将事件计数器设为 1。此时滑动窗口开始。

* 如果事件链中的事件在窗口内按顺序发生，则计数器递增。如果事件序列中断，则计数器不会递增。

* 如果数据中存在多个完成程度不同的事件链，该函数只会输出最长事件链的长度。

**语法**

```sql
windowFunnel(window, [mode, [mode, ... ]])(timestamp, cond1, cond2, ..., condN)
```

**参数**

* `timestamp` — 包含时间戳的列名。支持的数据类型：[Date](../../sql-reference/data-types/date.md)、[日期时间](/zh/sql-reference/data-types/datetime) 以及其他无符号整数类型 (注意，虽然 `timestamp` 支持 `UInt64` 类型，但其值不能超过 Int64 的最大值，即 2^63 - 1) 。
* `cond` — 作为事件链条件或描述事件链的数据。[UInt8](../../sql-reference/data-types/int-uint.md)。

**参数**

* `window` — 滑动窗口的长度，即第一个条件与最后一个条件之间的时间间隔。`window` 的单位取决于 `timestamp` 本身，可能会有所不同。其判定条件由表达式 `timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window` 确定。
* `mode` — 可选参数。可设置一个或多个 mode。
  * `'strict_deduplication'` — 如果事件序列中同一条件重复成立，则这类重复事件会中断后续处理。注意：如果同一事件同时满足多个条件，结果可能不符合预期。
  * `'strict_order'` — 不允许其他事件插入。例如，在 `A->B->D->C` 这种情况下，会在 `D` 处停止查找 `A->B->C`，最大事件级别为 2。
  * `'strict_increase'` — 仅对时间戳严格递增的事件应用条件。
  * `'strict_once'` — 即使某个事件多次满足条件，在事件链中也只计数一次。
  * `'allow_reentry'` — 忽略违反严格顺序的事件。例如，在 A-&gt;A-&gt;B-&gt;C 这种情况下，会忽略多余的 A，从而找到 A-&gt;B-&gt;C，最大事件级别为 3。

**返回值**

滑动时间窗口内，事件链中连续触发的条件最大数量。
会分析所选数据中的所有事件链。

类型：`Integer`。

**示例**

判断设定的一段时间是否足以让用户在网店中挑选一部手机并购买两次。

设置以下事件链：

1. 用户登录其商店账户 (`eventID = 1003`) 。
2. 用户搜索手机 (`eventID = 1007, product = 'phone'`) 。
3. 用户下单 (`eventID = 1009`) 。
4. 用户再次下单 (`eventID = 1010`) 。

输入表：

```text
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

找出用户 `user_id` 在 2019 年 1 月至 2 月期间的一个周期内，能够推进到事件链的哪一步。

```sql title="Query"
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
ORDER BY level ASC;
```

```text title="Response"
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

**allow&#95;reentry 模式示例**

本示例说明了 `allow_reentry` 模式如何与用户重入规则配合使用：

```sql
-- Sample data: user visits checkout -> product detail -> checkout again -> payment
-- Without allow_reentry: stops at level 2 (product detail page)
-- With allow_reentry: reaches level 4 (payment completion)

SELECT
    level,
    count() AS users
FROM
(
    SELECT
        user_id,
        windowFunnel(3600, 'strict_order', 'allow_reentry')(
            timestamp,
            action = 'begin_checkout',      -- Step 1: Begin checkout
            action = 'view_product_detail', -- Step 2: View product detail  
            action = 'begin_checkout',      -- Step 3: Begin checkout again (reentry)
            action = 'complete_payment'     -- Step 4: Complete payment
        ) AS level
    FROM user_events
    WHERE event_date = today()
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

<div id="retention">
  ## retention
</div>

该函数接受 1 到 32 个 `UInt8` 类型的条件作为参数，用于表示某个事件是否满足相应条件。
任何条件都可以作为参数指定 (如 [WHERE](/zh/sql-reference/statements/select/where) 中所示) 。

除第一个条件外，其余条件按两两方式应用：如果第一个和第二个条件都为 true，则第二个的结果为 true；如果第一个和第三个条件都为 true，则第三个的结果为 true；依此类推。

**语法**

```sql
retention(cond1, cond2, ..., cond32);
```

**参数**

* `cond` — 返回 `UInt8` 结果 (1 或 0) 的 expression。

**返回值**

由 1 或 0 组成的数组。

* 1 — 该事件满足条件。
* 0 — 该事件不满足条件。

类型：`UInt8`。

**示例**

下面通过一个计算 `retention` 函数的示例来确定站点流量。

**1.** 创建一个表来演示该示例。

```sql title="Query"
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

输入表：

```sql title="Query"
SELECT * FROM retention_test
```

```text title="Response"
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

**2.** 使用 `retention` 函数按唯一 ID `uid` 对用户分组。

```sql title="Query"
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

```text title="Response"
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

**3.** 计算每天的网站访问总次数。

```sql title="Query"
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

```text title="Response"
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

其中：

* `r1` - 在 2020-01-01 这一天访问该网站的独立访客数量 (`cond1` 条件) 。
* `r2` - 在 2020-01-01 到 2020-01-02 之间某个特定时间段内访问该网站的独立访客数量 (`cond1` 和 `cond2` 条件) 。
* `r3` - 在 2020-01-01 和 2020-01-03 的某个特定时间段内访问该网站的独立访客数量 (`cond1` 和 `cond3` 条件) 。

<div id="uniquptonx">
  ## uniqUpTo(N)(x)
</div>

计算参数中不同值的数量，上限为指定的 `N`。如果不同参数值的数量大于 `N`，则此函数返回 `N` + 1；否则返回精确值。

建议在较小的 `N` 值下使用，通常不超过 10。`N` 的最大值为 100。

对于 aggregate function 的状态，此函数占用的内存量等于 1 + `N` * 单个值的字节大小。
处理字符串时，此函数会存储一个 8 字节的非加密哈希；对于字符串，计算结果是近似值。

例如，假设你有一个表，用于记录用户在你的网站上发起的每一次搜索查询。表中的每一行都表示一条搜索查询记录，各列包括用户 ID、搜索查询以及查询时间戳。你可以使用 `uniqUpTo` 生成一份报告，仅显示至少产生了 5 个唯一用户的关键字。

```sql
SELECT SearchPhrase
FROM SearchLog
GROUP BY SearchPhrase
HAVING uniqUpTo(4)(UserID) >= 5
```

`uniqUpTo(4)(UserID)` 会按每个 `SearchPhrase` 计算唯一 `UserID` 的数量，但最多只统计 4 个唯一值。如果某个 `SearchPhrase` 的唯一 `UserID` 超过 4 个，函数会返回 5 (4 + 1) 。随后，`HAVING` 子句会过滤掉唯一 `UserID` 数量小于 5 的 `SearchPhrase`。这样，你就会得到一个至少被 5 个唯一用户使用过的搜索关键词列表。

<div id="summapfiltered">
  ## sumMapFiltered
</div>

此函数的行为与 [sumMap](/zh/sql-reference/aggregate-functions/reference/summap) 相同，但额外接受一个作为参数的键数组，用于筛选。在处理高基数键时，这一点尤其有用。

**语法**

`sumMapFiltered(keys_to_keep)(keys, values)`

**参数**

* `keys_to_keep`: 用于筛选的键 [Array](../data-types/array.md)。
* `keys`: 键 [Array](../data-types/array.md)。
* `values`: 值 [Array](../data-types/array.md)。

**返回值**

* 返回一个由两个数组组成的元组：按排序顺序排列的键，以及对应键求和后的值。

**示例**

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt16, requests UInt64)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) FROM sum_map;
```

```response title="Response"
   ┌─sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests)─┐
1. │ ([1,4,8],[10,20,10])                                            │
   └─────────────────────────────────────────────────────────────────┘
```

<div id="summapfilteredwithoverflow">
  ## sumMapFilteredWithOverflow
</div>

此函数的行为与 [sumMap](/zh/sql-reference/aggregate-functions/reference/summap) 相同，不同之处在于它还接受一个用于过滤的键数组作为参数。在处理高基数键时，这一点尤其有用。它与 [sumMapFiltered](#summapfiltered) 函数的区别在于，它会以溢出方式进行求和——也就是说，求和结果的数据类型与参数的数据类型相同。

**语法**

`sumMapFilteredWithOverflow(keys_to_keep)(keys, values)`

**参数**

* `keys_to_keep`: 用于过滤的键的 [Array](../data-types/array.md)。
* `keys`: 键的 [Array](../data-types/array.md)。
* `values`: 值的 [Array](../data-types/array.md)。

**返回值**

* 返回一个包含两个数组的元组：按排序顺序排列的键，以及对应键的求和值。

**示例**

在此示例中，我们创建一个表 `sum_map`，向其中插入一些数据，然后同时使用 `sumMapFilteredWithOverflow`、`sumMapFiltered` 和 `toTypeName` 函数来比较结果。在创建的表中，`requests` 的类型为 `UInt8`，`sumMapFiltered` 为避免溢出，会将求和值的类型提升为 `UInt64`；而 `sumMapFilteredWithOverflow` 则将该类型保留为 `UInt8`，这不足以存储结果——也就是说，发生了溢出。

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt8, requests UInt8)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFilteredWithOverflow([1, 4, 8])(statusMap.status, statusMap.requests) as summap_overflow, toTypeName(summap_overflow) FROM sum_map;
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) as summap, toTypeName(summap) FROM sum_map;
```

```response title="Response"
   ┌─sum──────────────────┬─toTypeName(sum)───────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt8)) │
   └──────────────────────┴───────────────────────────────────┘
```

```response title="Response"
   ┌─summap───────────────┬─toTypeName(summap)─────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt64)) │
   └──────────────────────┴────────────────────────────────────┘
```

<div id="sequencenextnode">
  ## sequenceNextNode
</div>

返回事件链中下一个匹配事件的值。

*Experimental 函数，使用 `SET allow_experimental_funnel_functions = 1` 启用。*

**语法**

```sql
sequenceNextNode(direction, base)(timestamp, event_column, base_condition, event1, event2, event3, ...)
```

**参数**

* `direction` — 用于指定导航方向。
  * forward — 向前移动。
  * backward — 向后移动。

* `base` — 用于设置基准点。
  * head — 将基准点设为第一个事件。
  * tail — 将基准点设为最后一个事件。
  * first&#95;match — 将基准点设为第一个匹配到的 `event1`。
  * last&#95;match — 将基准点设为最后一个匹配到的 `event1`。

**参数列表**

* `timestamp` — 包含时间戳的列名。支持的数据类型：[Date](../../sql-reference/data-types/date.md)、[DateTime](/zh/sql-reference/data-types/datetime) 以及其他无符号整数类型。
* `event_column` — 包含要返回的下一个事件值的列名。支持的数据类型：[String](../../sql-reference/data-types/string.md) 和 [Nullable(String)](../../sql-reference/data-types/nullable.md)。
* `base_condition` — 基准点必须满足的条件。
* `event1`, `event2`, ... — 描述事件链的条件。[UInt8](../../sql-reference/data-types/int-uint.md)。

**返回值**

* `event_column[next_index]` — 如果匹配到该模式且存在下一个值。
* `NULL` - 如果未匹配到该模式，或下一个值不存在。

类型：[Nullable(String)](../../sql-reference/data-types/nullable.md)。

**示例**

当事件序列为 A-&gt;B-&gt;C-&gt;D-&gt;E，且你想知道 B-&gt;C 之后的事件 (即 D) 时，可以使用它。

用于查找 A-&gt;B 之后事件的查询语句：

```sql title="Query"
CREATE TABLE test_flow (
    dt DateTime,
    id int,
    page String)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow VALUES (1, 1, 'A') (2, 1, 'B') (3, 1, 'C') (4, 1, 'D') (5, 1, 'E');

SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'A', page = 'A', page = 'B') as next_flow FROM test_flow GROUP BY id;
```

```text title="Response"
┌─id─┬─next_flow─┐
│  1 │ C         │
└────┴───────────┘
```

**`forward` 和 `head` 的行为**

```sql
ALTER TABLE test_flow DELETE WHERE 1 = 1 settings mutations_sync = 1;

INSERT INTO test_flow VALUES (1, 1, 'Home') (2, 1, 'Gift') (3, 1, 'Exit');
INSERT INTO test_flow VALUES (1, 2, 'Home') (2, 2, 'Home') (3, 2, 'Gift') (4, 2, 'Basket');
INSERT INTO test_flow VALUES (1, 3, 'Gift') (2, 3, 'Home') (3, 3, 'Gift') (4, 3, 'Basket');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'Home', page = 'Home', page = 'Gift') FROM test_flow GROUP BY id;

                  dt   id   page
 1970-01-01 09:00:01    1   Home // Base point, Matched with Home
 1970-01-01 09:00:02    1   Gift // Matched with Gift
 1970-01-01 09:00:03    1   Exit // The result

 1970-01-01 09:00:01    2   Home // Base point, Matched with Home
 1970-01-01 09:00:02    2   Home // Unmatched with Gift
 1970-01-01 09:00:03    2   Gift
 1970-01-01 09:00:04    2   Basket

 1970-01-01 09:00:01    3   Gift // Base point, Unmatched with Home
 1970-01-01 09:00:02    3   Home
 1970-01-01 09:00:03    3   Gift
 1970-01-01 09:00:04    3   Basket
```

**`backward` 和 `tail` 的行为**

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, page = 'Basket', page = 'Basket', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift
1970-01-01 09:00:03    1   Exit // Base point, Unmatched with Basket

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Matched with Gift
1970-01-01 09:00:04    2   Basket // Base point, Matched with Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // Base point, Matched with Gift
1970-01-01 09:00:04    3   Basket // Base point, Matched with Basket
```

**`forward` 和 `first_match` 的行为**

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit // The result

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket  The result

1970-01-01 09:00:01    3   Gift // Base point
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit // Unmatched with Home

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket // Unmatched with Home

1970-01-01 09:00:01    3   Gift // Base point
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // The result
1970-01-01 09:00:04    3   Basket
```

**`backward` 和 `last_match` 的行为**

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // The result
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // Base point
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // Matched with Home, the result is null
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home // The result
1970-01-01 09:00:02    2   Home // Matched with Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift // The result
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // Base point
1970-01-01 09:00:04    3   Basket
```

**关于 `base_condition` 的行为**

```sql
CREATE TABLE test_flow_basecond
(
    `dt` DateTime,
    `id` int,
    `page` String,
    `ref` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow_basecond VALUES (1, 1, 'A', 'ref4') (2, 1, 'A', 'ref3') (3, 1, 'B', 'ref2') (4, 1, 'B', 'ref1');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, ref = 'ref1', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // The head can not be base point because the ref column of the head unmatched with 'ref1'.
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, ref = 'ref4', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1 // The tail can not be base point because the ref column of the tail unmatched with 'ref4'.
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, ref = 'ref3', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // This row can not be base point because the ref column unmatched with 'ref3'.
 1970-01-01 09:00:02    1   A      ref3 // Base point
 1970-01-01 09:00:03    1   B      ref2 // The result
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, ref = 'ref2', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3 // The result
 1970-01-01 09:00:03    1   B      ref2 // Base point
 1970-01-01 09:00:04    1   B      ref1 // This row can not be base point because the ref column unmatched with 'ref2'.
```