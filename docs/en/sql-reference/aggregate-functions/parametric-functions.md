---
toc_priority: 38
toc_title: Parametric
---

# Parametric Aggregate Functions {#aggregate_functions_parametric}

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## histogram {#histogram}

Calculates an adaptive histogram. It doesn’t guarantee precise results.

``` sql
histogram(number_of_bins)(values)
```

The functions uses [A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). The borders of histogram bins are adjusted as new data enters a function. In common case, the widths of bins are not equal.

**Parameters**

`number_of_bins` — Upper limit for the number of bins in the histogram. The function automatically calculates the number of bins. It tries to reach the specified number of bins, but if it fails, it uses fewer bins.
`values` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in input values.

**Returned values**

-   [Array](../../sql-reference/data-types/array.md) of [Tuples](../../sql-reference/data-types/tuple.md) of the following format:

        ```
        [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
        ```

        - `lower` — Lower bound of the bin.
        - `upper` — Upper bound of the bin.
        - `height` — Calculated height of the bin.

**Example**

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

You can visualize a histogram with the [bar](../../sql-reference/functions/other-functions.md#function-bar) function, for example:

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

In this case, you should remember that you don’t know the histogram bin borders.

## sequenceMatch(pattern)(timestamp, cond1, cond2, …) {#function-sequencematch}

Checks whether the sequence contains an event chain that matches the pattern.

``` sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

!!! warning "Warning"
    Events that occur at the same second may lay in the sequence in an undefined order affecting the result.

**Parameters**

-   `pattern` — Pattern string. See [Pattern syntax](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` and `DateTime`. You can also use any of the supported [UInt](../../sql-reference/data-types/int-uint.md) data types.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. You can pass up to 32 condition arguments. The function takes only the events described in these conditions into account. If the sequence contains data that isn’t described in a condition, the function skips them.

**Returned values**

-   1, if the pattern is matched.
-   0, if the pattern isn’t matched.

Type: `UInt8`.

<a name="sequence-function-pattern-syntax"></a>
**Pattern syntax**

-   `(?N)` — Matches the condition argument at position `N`. Conditions are numbered in the `[1, 32]` range. For example, `(?1)` matches the argument passed to the `cond1` parameter.

-   `.*` — Matches any number of events. You don’t need conditional arguments to match this element of the pattern.

-   `(?t operator value)` — Sets the time in seconds that should separate two events. For example, pattern `(?1)(?t>1800)(?2)` matches events that occur more than 1800 seconds from each other. An arbitrary number of any events can lay between these events. You can use the `>=`, `>`, `<`, `<=` operators.

**Examples**

Consider data in the `t` table:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

Perform the query:

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

The function found the event chain where number 2 follows number 1. It skipped number 3 between them, because the number is not described as an event. If we want to take this number into account when searching for the event chain given in the example, we should make a condition for it.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

In this case, the function couldn’t find the event chain matching the pattern, because the event for number 3 occured between 1 and 2. If in the same case we checked the condition for number 4, the sequence would match the pattern.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**See Also**

-   [sequenceCount](#function-sequencecount)

## sequenceCount(pattern)(time, cond1, cond2, …) {#function-sequencecount}

Counts the number of event chains that matched the pattern. The function searches event chains that don’t overlap. It starts to search for the next chain after the current chain is matched.

!!! warning "Warning"
    Events that occur at the same second may lay in the sequence in an undefined order affecting the result.

``` sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**Parameters**

-   `pattern` — Pattern string. See [Pattern syntax](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` and `DateTime`. You can also use any of the supported [UInt](../../sql-reference/data-types/int-uint.md) data types.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. You can pass up to 32 condition arguments. The function takes only the events described in these conditions into account. If the sequence contains data that isn’t described in a condition, the function skips them.

**Returned values**

-   Number of non-overlapping event chains that are matched.

Type: `UInt64`.

**Example**

Consider data in the `t` table:

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

Count how many times the number 2 occurs after the number 1 with any amount of other numbers between them:

``` sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

**See Also**

-   [sequenceMatch](#function-sequencematch)

## windowFunnel {#windowfunnel}

Searches for event chains in a sliding time window and calculates the maximum number of events that occurred from the chain.

The function works according to the algorithm:

-   The function searches for data that triggers the first condition in the chain and sets the event counter to 1. This is the moment when the sliding window starts.

-   If events from the chain occur sequentially within the window, the counter is incremented. If the sequence of events is disrupted, the counter isn’t incremented.

-   If the data has multiple event chains at varying points of completion, the function will only output the size of the longest chain.

**Syntax**

``` sql
windowFunnel(window, [mode])(timestamp, cond1, cond2, ..., condN)
```

**Parameters**

-   `window` — Length of the sliding window in seconds.
-   `mode` - It is an optional argument.
    -   `'strict'` - When the `'strict'` is set, the windowFunnel() applies conditions only for the unique values.
-   `timestamp` — Name of the column containing the timestamp. Data types supported: [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) and other unsigned integer types (note that even though timestamp supports the `UInt64` type, it’s value can’t exceed the Int64 maximum, which is 2^63 - 1).
-   `cond` — Conditions or data describing the chain of events. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

The maximum number of consecutive triggered conditions from the chain within the sliding time window.
All the chains in the selection are analyzed.

Type: `Integer`.

**Example**

Determine if a set period of time is enough for the user to select a phone and purchase it twice in the online store.

Set the following chain of events:

1.  The user logged in to their account on the store (`eventID = 1003`).
2.  The user searches for a phone (`eventID = 1007, product = 'phone'`).
3.  The user placed an order (`eventID = 1009`).
4.  The user made the order again (`eventID = 1010`).

Input table:

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

Find out how far the user `user_id` could get through the chain in a period in January-February of 2019.

Query:

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

Result:

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

## retention {#retention}

The function takes as arguments a set of conditions from 1 to 32 arguments of type `UInt8` that indicate whether a certain condition was met for the event.
Any condition can be specified as an argument (as in [WHERE](../../sql-reference/statements/select/where.md#select-where)).

The conditions, except the first, apply in pairs: the result of the second will be true if the first and second are true, of the third if the first and third are true, etc.

**Syntax**

``` sql
retention(cond1, cond2, ..., cond32);
```

**Parameters**

-   `cond` — an expression that returns a `UInt8` result (1 or 0).

**Returned value**

The array of 1 or 0.

-   1 — condition was met for the event.
-   0 — condition wasn’t met for the event.

Type: `UInt8`.

**Example**

Let’s consider an example of calculating the `retention` function to determine site traffic.

**1.** Сreate a table to illustrate an example.

``` sql
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

Input table:

Query:

``` sql
SELECT * FROM retention_test
```

Result:

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

**2.** Group users by unique ID `uid` using the `retention` function.

Query:

``` sql
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

Result:

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

**3.** Calculate the total number of site visits per day.

Query:

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

Result:

``` text
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

Where:

-   `r1`- the number of unique visitors who visited the site during 2020-01-01 (the `cond1` condition).
-   `r2`- the number of unique visitors who visited the site during a specific time period between 2020-01-01 and 2020-01-02 (`cond1` and `cond2` conditions).
-   `r3`- the number of unique visitors who visited the site during a specific time period between 2020-01-01 and 2020-01-03 (`cond1` and `cond3` conditions).

## uniqUpTo(N)(x) {#uniquptonx}

Calculates the number of different argument values ​​if it is less than or equal to N. If the number of different argument values is greater than N, it returns N + 1.

Recommended for use with small Ns, up to 10. The maximum value of N is 100.

For the state of an aggregate function, it uses the amount of memory equal to 1 + N \* the size of one value of bytes.
For strings, it stores a non-cryptographic hash of 8 bytes. That is, the calculation is approximated for strings.

The function also works for several arguments.

It works as fast as possible, except for cases when a large N value is used and the number of unique values is slightly less than N.

Usage example:

``` text
Problem: Generate a report that shows only keywords that produced at least 5 unique users.
Solution: Write in the GROUP BY query SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5
```

[Original article](https://clickhouse.tech/docs/en/query_language/agg_functions/parametric_functions/) <!--hide-->

## sumMapFiltered(keys\_to\_keep)(keys, values) {#summapfilteredkeys-to-keepkeys-values}

Same behavior as [sumMap](../../sql-reference/aggregate-functions/reference/summap.md#agg_functions-summap) except that an array of keys is passed as a parameter. This can be especially useful when working with a high cardinality of keys.
