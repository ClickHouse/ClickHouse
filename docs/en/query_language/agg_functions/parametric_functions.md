<a name="aggregate_functions_parametric"></a>

# Parametric aggregate functions

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## sequenceMatch(pattern)(time, cond1, cond2, ...)

Pattern matching for event chains.

`pattern` is a string containing a pattern to match. The pattern is similar to a regular expression.

`time` is the time of the event with the DateTime type.

`cond1`, `cond2` ... is from one to 32 arguments of type UInt8 that indicate whether a certain condition was met for the event.

The function collects a sequence of events in RAM. Then it checks whether this sequence matches the pattern.
It returns UInt8: 0 if the pattern isn't matched, or 1 if it matches.

Example: `sequenceMatch ('(?1).*(?2)')(EventTime, URL LIKE '%company%', URL LIKE '%cart%')`

- whether there was a chain of events in which a pageview with 'company' in the address occurred earlier than a pageview with 'cart' in the address.

This is a singular example. You could write it using other aggregate functions:

```text
minIf(EventTime, URL LIKE '%company%') < maxIf(EventTime, URL LIKE '%cart%').
```

However, there is no such solution for more complex situations.

Pattern syntax:

`(?1)` refers to the condition (any number can be used in place of 1).

`.*` is any number of any events.

`(?t>=1800)` is a time condition.

Any quantity of any type of events is allowed over the specified time.

Instead of `>=`,  the following operators can be used:`<`, `>`, `<=`.

Any number may be specified in place of 1800.

Events that occur during the same second can be put in the chain in any order. This may affect the result of the function.

## sequenceCount(pattern)(time, cond1, cond2, ...)

Works the same way as the sequenceMatch function, but instead of returning whether there is an event chain, it returns UInt64 with the number of event chains found.
Chains are searched for without overlapping. In other words, the next chain can start only after the end of the previous one.

## windowFunnel(window)(timestamp, cond1, cond2, cond3, ...)

Window funnel matching for event chains, calculates the max event level in a sliding window.

`window` is the timestamp window value, such as 3600.

`timestamp` is the time of the event with the DateTime type or UInt32 type.

`cond1`, `cond2` ... is from one to 32 arguments of type UInt8 that indicate whether a certain condition was met for the event

Example: 

Consider you are doing a website analytics, intend to find out the user counts clicked login button (event = 1001), then the user counts followed by searched the phones( event = 1003 and product = 'phone'), then the user counts followed by made an order (event = 1009). And all event chains must be in a 3600 seconds sliding window. 

This could be easily calculate by `windowFunnel`

```
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(3600)(timestamp, event_id = 1001, event_id = 1003 AND product = 'phone', event_id = 1009) AS level
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

```
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

