# Parametric aggregate functions {#aggregate_functions_parametric}

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## histogram

Calculates an adaptive histogram. It doesn't guarantee precise results.

```sql
histogram(number_of_bins)(values)
```
 
The functions uses [A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). The borders of histogram bins are adjusted as new data enters a function. In common case, the widths of bins are not equal.

**Parameters**

`number_of_bins` — Upper limit for the number of bins in the histogram. The function automatically calculates the number of bins. It tries to reach the specified number of bins, but if it fails, it uses fewer bins.
`values` — [Expression](../syntax.md#syntax-expressions) resulting in input values.

**Returned values**

- [Array](../../data_types/array.md) of [Tuples](../../data_types/tuple.md) of the following format:

    ```
    [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
    ```

    - `lower` — Lower bound of the bin.
    - `upper` — Upper bound of the bin.
    - `height` — Calculated height of the bin.

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

You can visualize a histogram with the [bar](../functions/other_functions.md#function-bar) function, for example:

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

In this case, you should remember that you don't know the histogram bin borders.

## sequenceMatch(pattern)(timestamp, cond1, cond2, ...) {#function-sequencematch}

Checks whether the sequence contains the event chain that matches the pattern.

```sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

!!! warning "Warning"
    Events that occur at the same second may lay in the sequence in an undefined order affecting the result.


**Parameters**

- `pattern` — Pattern string. See [Pattern syntax](#sequence-function-pattern-syntax).

- `timestamp` — Column that considered to contain time data. Typical data types are `Date`, and `DateTime`. You can use also any of the supported [UInt](../../data_types/int_uint.md) data types.

- `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. You can pass up to 32 condition arguments. The function takes into account only the events described in these conditions. If the sequence contains data that are not described with conditions the function skips them.


**Returned values**

- 1, if the pattern is matched.
- 0, if the pattern isn't matched.


Type: `UInt8`.


<a name="sequence-function-pattern-syntax"></a>
**Pattern syntax**

- `(?N)` — Matches the condition argument at the position `N`. Conditions are numbered in the `[1, 32]` range. For example, `(?1)` matches the argument passed to the `cond1` parameter.

- `.*` — Matches any number of any events. You don't need the conditional arguments to match this element of the pattern.

- `(?t operator value)` — Sets the time in seconds that should separate two events. For example, pattern `(?1)(?t>1800)(?2)` matches events that distanced from each other for more than 1800 seconds. An arbitrary number of any events can lay between these events. You can use the `>=`, `>`, `<`, `<=` operators.

**Examples**

Consider data in the `t` table:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

Perform the query:

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```
```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

The function has found the event chain where number 2 follows number 1. It skipped number 3 between them, because the number is not described as an event. If we want to take this number into account when searching for the event chain, showed in the example, we should make a condition for it.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```
```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

In this case the function couldn't find the event chain matching the pattern, because there is the event for number 3 occured between 1 and 2. If in the same case we checked the condition for number 4, the sequence would match the pattern.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```
```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```


**See Also**

- [sequenceCount](#function-sequencecount)


## sequenceCount(pattern)(time, cond1, cond2, ...) {#function-sequencecount}

Counts the number of event chains that matched the pattern. The function searches event chains that not overlap. It starts to search for the next chain after the current chain is matched.

!!! warning "Warning"
    Events that occur at the same second may lay in the sequence in an undefined order affecting the result.

```sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**Parameters**

- `pattern` — Pattern string. See [Pattern syntax](#sequence-function-pattern-syntax).

- `timestamp` — Column that considered to contain time data. Typical data types are `Date`, and `DateTime`. You can also use any of the supported [UInt](../../data_types/int_uint.md) data types.

- `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. You can pass up to 32 condition arguments. The function takes into account only the events described in these conditions. If the sequence contains data that are not described with conditions the function skips them.


**Returned values**

- Number of non-overlapping event chains that are matched

Type: `UInt64`.


**Example**

Consider data in the `t` table:

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

Count how many times the number 2 occurs after the number 1 with any amount of other numbers between them:

```sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```
```text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

**See Also**

- [sequenceMatch](#function-sequencematch)


## windowFunnel(window)(timestamp, cond1, cond2, cond3, ...)

Searches for event chains in a sliding time window and calculates the maximum number of events that occurred from the chain.

```
windowFunnel(window)(timestamp, cond1, cond2, cond3, ...)
```

**Parameters:**

- `window` — Length of the sliding window in seconds.
- `timestamp` — Name of the column containing the timestamp. Data type support: `Date`,`DateTime`, and other unsigned integer types (note that though timestamp support `UInt64` type, there is a limitation it's value can't overflow maximum of Int64, which is 2^63 - 1).
- `cond1`, `cond2`... — Conditions or data describing the chain of events. Data type: `UInt8`. Values can be 0 or 1.

**Algorithm**

- The function searches for data that triggers the first condition in the chain and sets the event counter to 1. This is the moment when the sliding window starts.
- If events from the chain occur sequentially within the window, the counter is incremented. If the sequence of events is disrupted, the counter isn't incremented.
- If the data has multiple event chains at varying points of completion, the function will only output the size of the longest chain.

**Returned value**

- Integer. The maximum number of consecutive triggered conditions from the chain within the sliding time window. All the chains in the selection are analyzed.

**Example**

Determine if one hour is enough for the user to select a phone and purchase it in the online store.

Set the following chain of events:

1. The user logged in to their account on the store (`eventID=1001`).
2. The user searches for a phone (`eventID = 1003, product = 'phone'`).
3. The user placed an order (`eventID = 1009`).

To find out how far the user `user_id` could get through the chain in an hour in January of 2017, make the query:

```sql
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(3600)(timestamp, eventID = 1001, eventID = 1003 AND product = 'phone', eventID = 1009) AS level
    FROM trend_event
    WHERE (event_date >= '2017-01-01') AND (event_date <= '2017-01-31')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level
```

Simply, the level value could only be 0, 1, 2, 3, it means the maxium event action stage that one user could reach.

## retention(cond1, cond2, ...)

Retention refers to the ability of a company or product to retain its customers over some specified periods.

`cond1`, `cond2` ... is from one to 32 arguments of type UInt8 that indicate whether a certain condition was met for the event

Example:

Consider you are doing a website analytics, intend to calculate the retention of customers

This could be easily calculate by `retention`

```sql
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2018-08-10', date = '2018-08-11', date = '2018-08-12') AS r
    FROM events
    WHERE date IN ('2018-08-10', '2018-08-11', '2018-08-12')
    GROUP BY uid
)
```

Simply, `r1` means the number of unique visitors who met the `cond1` condition, `r2` means the number of unique visitors who met `cond1` and `cond2` conditions, `r3` means the number of unique visitors who met `cond1` and `cond3` conditions.


## uniqUpTo(N)(x)

Calculates the number of different argument values ​​if it is less than or equal to N. If the number of different argument values is greater than N, it returns N + 1.

Recommended for use with small Ns, up to 10. The maximum value of N is 100.

For the state of an aggregate function, it uses the amount of memory equal to 1 + N \* the size of one value of bytes.
For strings, it stores a non-cryptographic hash of 8 bytes. That is, the calculation is approximated for strings.

The function also works for several arguments.

It works as fast as possible, except for cases when a large N value is used and the number of unique values is slightly less than N.

Usage example:

```text
Problem: Generate a report that shows only keywords that produced at least 5 unique users.
Solution: Write in the GROUP BY query SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5
```

[Original article](https://clickhouse.yandex/docs/en/query_language/agg_functions/parametric_functions/) <!--hide-->


## sumMapFiltered(keys_to_keep)(keys, values)

Same behavior as [sumMap](reference.md#agg_functions-summap) except that an array of keys is passed as a parameter. This can be especially useful when working with a high cardinality of keys.
