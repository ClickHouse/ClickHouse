---
slug: /en/sql-reference/aggregate-functions/parametric-functions
sidebar_position: 38
sidebar_label: Parametric
---

# Parametric Aggregate Functions

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## histogram

Calculates an adaptive histogram. It does not guarantee precise results.

``` sql
histogram(number_of_bins)(values)
```

The functions uses [A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). The borders of histogram bins are adjusted as new data enters a function. In common case, the widths of bins are not equal.

**Arguments**

`values` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in input values.

**Parameters**

`number_of_bins` — Upper limit for the number of bins in the histogram. The function automatically calculates the number of bins. It tries to reach the specified number of bins, but if it fails, it uses fewer bins.

**Returned values**

- [Array](../../sql-reference/data-types/array.md) of [Tuples](../../sql-reference/data-types/tuple.md) of the following format:

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

In this case, you should remember that you do not know the histogram bin borders.

## sequenceMatch

Checks whether the sequence contains an event chain that matches the pattern.

**Syntax**

``` sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

:::note
Events that occur at the same second may lay in the sequence in an undefined order affecting the result.
:::

**Arguments**

- `timestamp` — Column considered to contain time data. Typical data types are `Date` and `DateTime`. You can also use any of the supported [UInt](../../sql-reference/data-types/int-uint.md) data types.

- `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. You can pass up to 32 condition arguments. The function takes only the events described in these conditions into account. If the sequence contains data that isn’t described in a condition, the function skips them.

**Parameters**

- `pattern` — Pattern string. See [Pattern syntax](#pattern-syntax).

**Returned values**

- 1, if the pattern is matched.
- 0, if the pattern isn’t matched.

Type: `UInt8`.

#### Pattern syntax

- `(?N)` — Matches the condition argument at position `N`. Conditions are numbered in the `[1, 32]` range. For example, `(?1)` matches the argument passed to the `cond1` parameter.

- `.*` — Matches any number of events. You do not need conditional arguments to match this element of the pattern.

- `(?t operator value)` — Sets the time in seconds that should separate two events. For example, pattern `(?1)(?t>1800)(?2)` matches events that occur more than 1800 seconds from each other. An arbitrary number of any events can lay between these events. You can use the `>=`, `>`, `<`, `<=`, `==` operators.

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

In this case, the function couldn’t find the event chain matching the pattern, because the event for number 3 occurred between 1 and 2. If in the same case we checked the condition for number 4, the sequence would match the pattern.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**See Also**

- [sequenceCount](#sequencecount)

## sequenceCount

Counts the number of event chains that matched the pattern. The function searches event chains that do not overlap. It starts to search for the next chain after the current chain is matched.

:::note
Events that occur at the same second may lay in the sequence in an undefined order affecting the result.
:::

**Syntax**

``` sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**Arguments**

- `timestamp` — Column considered to contain time data. Typical data types are `Date` and `DateTime`. You can also use any of the supported [UInt](../../sql-reference/data-types/int-uint.md) data types.

- `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. You can pass up to 32 condition arguments. The function takes only the events described in these conditions into account. If the sequence contains data that isn’t described in a condition, the function skips them.

**Parameters**

- `pattern` — Pattern string. See [Pattern syntax](#pattern-syntax).

**Returned values**

- Number of non-overlapping event chains that are matched.

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

- [sequenceMatch](#sequencematch)

## windowFunnel

Searches for event chains in a sliding time window and calculates the maximum number of events that occurred from the chain.

The function works according to the algorithm:

- The function searches for data that triggers the first condition in the chain and sets the event counter to 1. This is the moment when the sliding window starts.

- If events from the chain occur sequentially within the window, the counter is incremented. If the sequence of events is disrupted, the counter isn’t incremented.

- If the data has multiple event chains at varying points of completion, the function will only output the size of the longest chain.

**Syntax**

``` sql
windowFunnel(window, [mode, [mode, ... ]])(timestamp, cond1, cond2, ..., condN)
```

**Arguments**

- `timestamp` — Name of the column containing the timestamp. Data types supported: [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) and other unsigned integer types (note that even though timestamp supports the `UInt64` type, it’s value can’t exceed the Int64 maximum, which is 2^63 - 1).
- `cond` — Conditions or data describing the chain of events. [UInt8](../../sql-reference/data-types/int-uint.md).

**Parameters**

- `window` — Length of the sliding window, it is the time interval between the first and the last condition. The unit of `window` depends on the `timestamp` itself and varies. Determined using the expression `timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window`.
- `mode` — It is an optional argument. One or more modes can be set.
    - `'strict_deduplication'` — If the same condition holds for the sequence of events, then such repeating event interrupts further processing. Note: it may work unexpectedly if several conditions hold for the same event.
    - `'strict_order'` — Don't allow interventions of other events. E.g. in the case of `A->B->D->C`, it stops finding `A->B->C` at the `D` and the max event level is 2.
    - `'strict_increase'` — Apply conditions only to events with strictly increasing timestamps.
    - `'strict_once'` — Count each event only once in the chain even if it meets the condition several times

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
ORDER BY level ASC;
```

Result:

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

## retention

The function takes as arguments a set of conditions from 1 to 32 arguments of type `UInt8` that indicate whether a certain condition was met for the event.
Any condition can be specified as an argument (as in [WHERE](../../sql-reference/statements/select/where.md#select-where)).

The conditions, except the first, apply in pairs: the result of the second will be true if the first and second are true, of the third if the first and third are true, etc.

**Syntax**

``` sql
retention(cond1, cond2, ..., cond32);
```

**Arguments**

- `cond` — An expression that returns a `UInt8` result (1 or 0).

**Returned value**

The array of 1 or 0.

- 1 — Condition was met for the event.
- 0 — Condition wasn’t met for the event.

Type: `UInt8`.

**Example**

Let’s consider an example of calculating the `retention` function to determine site traffic.

**1.** Create a table to illustrate an example.

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

- `r1`- the number of unique visitors who visited the site during 2020-01-01 (the `cond1` condition).
- `r2`- the number of unique visitors who visited the site during a specific time period between 2020-01-01 and 2020-01-02 (`cond1` and `cond2` conditions).
- `r3`- the number of unique visitors who visited the site during a specific time period on 2020-01-01 and 2020-01-03 (`cond1` and `cond3` conditions).

## uniqUpTo(N)(x)

Calculates the number of different values of the argument up to a specified limit, `N`. If the number of different argument values is greater than `N`, this function returns `N` + 1, otherwise it calculates the exact value.

Recommended for use with small `N`s, up to 10. The maximum value of `N` is 100.

For the state of an aggregate function, this function uses the amount of memory equal to 1 + `N` \* the size of one value of bytes.
When dealing with strings, this function stores a non-cryptographic hash of 8 bytes; the calculation is approximated for strings.

For example, if you had a table that logs every search query made by users on your website. Each row in the table represents a single search query, with columns for the user ID, the search query, and the timestamp of the query. You can use `uniqUpTo` to generate a report that shows only the keywords that produced at least 5 unique users.

```sql
SELECT SearchPhrase
FROM SearchLog
GROUP BY SearchPhrase
HAVING uniqUpTo(4)(UserID) >= 5
```

`uniqUpTo(4)(UserID)` calculates the number of unique `UserID` values for each `SearchPhrase`, but it only counts up to 4 unique values. If there are more than 4 unique `UserID` values for a `SearchPhrase`, the function returns 5 (4 + 1). The `HAVING` clause then filters out the `SearchPhrase` values for which the number of unique `UserID` values is less than 5. This will give you a list of search keywords that were used by at least 5 unique users.

## sumMapFiltered

This function behaves the same as [sumMap](../../sql-reference/aggregate-functions/reference/summap.md#agg_functions-summap) except that it also accepts an array of keys to filter with as a parameter. This can be especially useful when working with a high cardinality of keys.

**Syntax**

`sumMapFiltered(keys_to_keep)(keys, values)`

**Parameters**

- `keys_to_keep`: [Array](../data-types/array.md) of keys to filter with.
- `keys`: [Array](../data-types/array.md) of keys.
- `values`: [Array](../data-types/array.md) of values.

**Returned Value**

- Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

**Example**

Query:

```sql
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

```sql
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) FROM sum_map;
```

Result:

```response
   ┌─sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests)─┐
1. │ ([1,4,8],[10,20,10])                                            │
   └─────────────────────────────────────────────────────────────────┘
```

## sumMapFilteredWithOverflow

This function behaves the same as [sumMap](../../sql-reference/aggregate-functions/reference/summap.md#agg_functions-summap) except that it also accepts an array of keys to filter with as a parameter. This can be especially useful when working with a high cardinality of keys. It differs from the [sumMapFiltered](#summapfiltered) function in that it does summation with overflow - i.e. returns the same data type for the summation as the argument data type.

**Syntax**

`sumMapFilteredWithOverflow(keys_to_keep)(keys, values)`

**Parameters**

- `keys_to_keep`: [Array](../data-types/array.md) of keys to filter with.
- `keys`: [Array](../data-types/array.md) of keys.
- `values`: [Array](../data-types/array.md) of values.

**Returned Value**

- Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

**Example**

In this example we create a table `sum_map`, insert some data into it and then use both `sumMapFilteredWithOverflow` and `sumMapFiltered` and the `toTypeName` function for comparison of the result. Where `requests` was of type `UInt8` in the created table, `sumMapFiltered` has promoted the type of the summed values to `UInt64` to avoid overflow whereas `sumMapFilteredWithOverflow` has kept the type as `UInt8` which is not large enough to store the result - i.e. overflow has occurred.

Query:

```sql
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

```sql
SELECT sumMapFilteredWithOverflow([1, 4, 8])(statusMap.status, statusMap.requests) as summap_overflow, toTypeName(summap_overflow) FROM sum_map;
```

```sql
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) as summap, toTypeName(summap) FROM sum_map;
```

Result:

```response
   ┌─sum──────────────────┬─toTypeName(sum)───────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt8)) │
   └──────────────────────┴───────────────────────────────────┘
```

```response
   ┌─summap───────────────┬─toTypeName(summap)─────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt64)) │
   └──────────────────────┴────────────────────────────────────┘
```

## sequenceNextNode

Returns a value of the next event that matched an event chain.

_Experimental function, `SET allow_experimental_funnel_functions = 1` to enable it._

**Syntax**

``` sql
sequenceNextNode(direction, base)(timestamp, event_column, base_condition, event1, event2, event3, ...)
```

**Parameters**

- `direction` — Used to navigate to directions.
    - forward — Moving forward.
    - backward — Moving backward.

- `base` — Used to set the base point.
    - head — Set the base point to the first event.
    - tail — Set the base point to the last event.
    - first_match — Set the base point to the first matched `event1`.
    - last_match — Set the base point to the last matched `event1`.

**Arguments**

- `timestamp` — Name of the column containing the timestamp. Data types supported: [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) and other unsigned integer types.
- `event_column` — Name of the column containing the value of the next event to be returned. Data types supported: [String](../../sql-reference/data-types/string.md) and [Nullable(String)](../../sql-reference/data-types/nullable.md).
- `base_condition` — Condition that the base point must fulfill.
- `event1`, `event2`, ... — Conditions describing the chain of events. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned values**

- `event_column[next_index]` — If the pattern is matched and next value exists.
- `NULL` - If the pattern isn’t matched or next value doesn't exist.

Type: [Nullable(String)](../../sql-reference/data-types/nullable.md).

**Example**

It can be used when events are A->B->C->D->E and you want to know the event following B->C, which is D.

The query statement searching the event following A->B:

``` sql
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

Result:

``` text
┌─id─┬─next_flow─┐
│  1 │ C         │
└────┴───────────┘
```

**Behavior for `forward` and `head`**

``` sql
ALTER TABLE test_flow DELETE WHERE 1 = 1 settings mutations_sync = 1;

INSERT INTO test_flow VALUES (1, 1, 'Home') (2, 1, 'Gift') (3, 1, 'Exit');
INSERT INTO test_flow VALUES (1, 2, 'Home') (2, 2, 'Home') (3, 2, 'Gift') (4, 2, 'Basket');
INSERT INTO test_flow VALUES (1, 3, 'Gift') (2, 3, 'Home') (3, 3, 'Gift') (4, 3, 'Basket');
```

``` sql
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

**Behavior for `backward` and `tail`**

``` sql
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


**Behavior for `forward` and `first_match`**

``` sql
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

``` sql
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


**Behavior for `backward` and `last_match`**

``` sql
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

``` sql
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


**Behavior for `base_condition`**

``` sql
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

``` sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, ref = 'ref1', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // The head can not be base point because the ref column of the head unmatched with 'ref1'.
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1
 ```

``` sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, ref = 'ref4', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1 // The tail can not be base point because the ref column of the tail unmatched with 'ref4'.
```

``` sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, ref = 'ref3', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // This row can not be base point because the ref column unmatched with 'ref3'.
 1970-01-01 09:00:02    1   A      ref3 // Base point
 1970-01-01 09:00:03    1   B      ref2 // The result
 1970-01-01 09:00:04    1   B      ref1
```

``` sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, ref = 'ref2', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3 // The result
 1970-01-01 09:00:03    1   B      ref2 // Base point
 1970-01-01 09:00:04    1   B      ref1 // This row can not be base point because the ref column unmatched with 'ref2'.
```
