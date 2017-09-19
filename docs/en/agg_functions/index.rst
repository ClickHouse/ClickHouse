Aggregate functions
===================

count()
-------
Counts the number of rows. Accepts zero arguments and returns UInt64.
The syntax ``COUNT(DISTINCT x)`` is not supported. The separate ``uniq`` aggregate function exists for this purpose.

A ``SELECT count() FROM table`` query is not optimized, because the number of entries in the table is not stored separately. It will select some small column from the table and count the number of values in it.

any(x)
------
Selects the first encountered value.
The query can be executed in any order and even in a different order each time, so the result of this function is indeterminate.
To get a determinate result, you can use the ``min`` or ``max`` function instead of ``any``.

In some cases, you can rely on the order of execution. This applies to cases when ``SELECT`` comes from a subquery that uses ``ORDER BY``.

When a SELECT query has the GROUP BY clause or at least one aggregate function, ClickHouse (in contrast to for example MySQL) requires that all expressions in the ``SELECT``, ``HAVING`` and ``ORDER BY`` clauses be calculated from keys or from aggregate functions. That is, each column selected from the table must be used either in keys, or inside aggregate functions. To get behavior like in MySQL, you can put the other columns in the ``any`` aggregate function.

anyLast(x)
----------
Selects the last value encountered.
The result is just as indeterminate as for the 'any' function.

min(x)
------
Calculates the minimum.

max(x)
------
Calculates the maximum

argMin(arg, val)
----------------
Calculates the 'arg' value for a minimal 'val' value. If there are several different values of 'arg' for minimal values of 'val', the first of these values encountered is output.

argMax(arg, val)
----------------
Calculates the 'arg' value for a maximum 'val' value. If there are several different values of 'arg' for maximum values of 'val', the first of these values encountered is output.

sum(x)
------
Calculates the sum.
Only works for numbers.

sumMap(key, value)
------
Performs summation of array 'value' by corresponding keys of array 'key'.
Number of elements in 'key' and 'value' arrays should be the same for each row, on which summation is being performed.
Returns a tuple of two arrays - sorted keys and values, summed up by corresponding keys.

Example:

.. code-block:: sql

CREATE TABLE sum_map(
	date Date,
	timeslot DateTime,
	statusMap Nested(
		status UInt16,
		requests UInt64
	)
) ENGINE = Log;
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests)
FROM sum_map
GROUP BY timeslot

.. code-block:: text

┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │
└─────────────────────┴──────────────────────────────────────────────┘

avg(x)
------
Calculates the average.
Only works for numbers.
The result is always Float64.

uniq(x)
-------
Calculates the approximate number of different values of the argument. Works for numbers, strings, dates, and dates with times.

Uses an adaptive sampling algorithm: for the calculation state, it uses a sample of element hash values with a size up to 65535.
Compared with the widely known `HyperLogLog <https://en.wikipedia.org/wiki/HyperLogLog>`_ algorithm, this algorithm is less effective in terms of accuracy and memory consumption (even up to proportionality), but it is adaptive. This means that with fairly high accuracy, it consumes less memory during simultaneous computation of cardinality for a large number of data sets whose cardinality has power law distribution (i.e. in cases when most of the data sets are small). This algorithm is also very accurate for data sets with small cardinality (up to 65536) and very efficient on CPU (when computing not too many of these functions, using ``uniq`` is almost as fast as using other aggregate functions).

There is no compensation for the bias of an estimate, so for large data sets the results are systematically deflated. This function is normally used for computing the number of unique visitors in Yandex.Metrica, so this bias does not play a role.

The result is deterministic (it does not depend on the order of query execution).

uniqCombined(x)
---------------
Approximately computes the number of different values ​​of the argument. Works for numbers, strings, dates, date-with-time, for several arguments and arguments-tuples.

A combination of three algorithms is used: an array, a hash table and `HyperLogLog <https://en.wikipedia.org/wiki/HyperLogLog>`_ with an error correction table. The memory consumption is several times smaller than the ``uniq`` function, and the accuracy is several times higher. The speed of operation is slightly lower than that of the ``uniq`` function, but sometimes it can be even higher - in the case of distributed requests, in which a large number of aggregation states are transmitted over the network. The maximum state size is 96 KiB (HyperLogLog of 217 6-bit cells).

The result is deterministic (it does not depend on the order of query execution).

The ``uniqCombined`` function is a good default choice for calculating the number of different values.

uniqHLL12(x)
------------
Uses the `HyperLogLog <https://en.wikipedia.org/wiki/HyperLogLog>`_ algorithm to approximate the number of different values of the argument. It uses 212 5-bit cells. The size of the state is slightly more than 2.5 KB.

The result is deterministic (it does not depend on the order of query execution).

In most cases, use the 'uniq' function. You should only use this function if you understand its advantages well.

uniqExact(x)
------------
Calculates the number of different values of the argument, exactly.
There is no reason to fear approximations, so it's better to use the ``uniq`` function.
You should use the ``uniqExact`` function if you definitely need an exact result.

The ``uniqExact`` function uses more memory than the ``uniq`` function, because the size of the state has unbounded growth as the number of different values increases.

groupArray(x), groupArray(max_size)(x)
--------------------------------------
Creates an array of argument values.
Values can be added to the array in any (indeterminate) order.

The second version (with ``max_size`` parameter) limits the size of resulting array to ``max_size`` elements.
For example, ``groupArray(1)(x)`` is equivalent to ``[any(x)]``.

In some cases, you can rely on the order of execution. This applies to cases when ``SELECT`` comes from a subquery that uses ``ORDER BY``.

groupUniqArray(x)
-----------------
Creates an array from different argument values. Memory consumption is the same as for the ``uniqExact`` function.

quantile(level)(x)
------------------
Approximates the 'level' quantile. 'level' is a constant, a floating-point number from 0 to 1. We recommend using a 'level' value in the range of 0.01..0.99.
Don't use a 'level' value equal to 0 or 1 - use the 'min' and 'max' functions for these cases.

The algorithm is the same as for the ``median`` function. Actually, ``quantile`` and ``median`` are internally the same function. You can use the ``quantile`` function without parameters - in this case, it calculates the median, and you can use the ``median`` function with parameters - in this case, it calculates the quantile of the set level.

When using multiple ``quantile` and ``median`` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the ``quantiles`` function.

quantileDeterministic(level)(x, determinator)
---------------------------------------------
Calculates the quantile of 'level' using the same algorithm as the ``medianDeterministic`` function.


quantileTiming(level)(x)
------------------------
Calculates the quantile of 'level' using the same algorithm as the ``medianTiming`` function.

quantileTimingWeighted(level)(x, weight)
----------------------------------------
Calculates the quantile of 'level' using the same algorithm as the ``medianTimingWeighted`` function.

quantileExact(level)(x)
-----------------------
Computes the level quantile exactly. To do this, all transferred values are added to an array, which is then partially sorted. Therefore, the function consumes O(n) memory, where n is the number of transferred values. However, for a small number of values, the function is very effective.

quantileExactWeighted(level)(x, weight)
---------------------------------------
Computes the level quantile exactly. In this case, each value is taken into account with the weight weight - as if it is present weight once. The arguments of the function can be considered as histograms, where the value "x" corresponds to the "column" of the histogram of the height weight, and the function itself can be considered as the summation of histograms.

The algorithm is a hash table. Because of this, in case the transmitted values ​​are often repeated, the function consumes less RAM than the quantileExact. You can use this function instead of quantileExact, specifying the number 1 as the weight.

quantileTDigest(level)(x)
-------------------------
Computes the level quantile approximately, using the `t-digest <https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf>`_ algorithm. The maximum error is 1%. The memory consumption per state is proportional to the logarithm of the number of transmitted values.

The performance of the function is below quantile, quantileTiming. By the ratio of state size and accuracy, the function is significantly better than quantile.

The result depends on the order in which the query is executed, and is nondeterministic.

median
------
Approximates the median. Also see the similar ``quantile`` function.
Works for numbers, dates, and dates with times.
For numbers it returns Float64, for dates - a date, and for dates with times - a date with time.

Uses `reservoir sampling <https://en.wikipedia.org/wiki/Reservoir_sampling>`_ with a reservoir size up to 8192.
If necessary, the result is output with linear approximation from the two neighboring values.
This algorithm proved to be more practical than another well-known algorithm - QDigest.

The result depends on the order of running the query, and is nondeterministic.

quantiles(level1, level2, ...)(x)
---------------------------------
Approximates quantiles of all specified levels.
The result is an array containing the corresponding number of values.

varSamp(x)
----------
Calculates the amount ``Σ((x - x̅)2) / (n - 1)``, where 'n' is the sample size and 'x̅' is the average value of 'x'.

It represents an unbiased estimate of the variance of a random variable, if the values passed to the function are a sample of this random amount.

Returns Float64. If n <= 1, it returns +∞.

varPop(x)
---------
Calculates the amount ``Σ((x - x̅)2) / n``, where 'n' is the sample size and 'x̅' is the average value of 'x'.

In other words, dispersion for a set of values. Returns Float64.

stddevSamp(x)
-------------
The result is equal to the square root of ``varSamp(x)``.


stddevPop(x)
------------
The result is equal to the square root of ``varPop(x)``.


covarSamp(x, y)
---------------
Calculates the value of ``Σ((x - x̅)(y - y̅)) / (n - 1)``.

Returns Float64. If n <= 1, it returns +∞.

covarPop(x, y)
--------------
Calculates the value of ``Σ((x - x̅)(y - y̅)) / n``.

corr(x, y)
----------
Calculates the Pearson correlation coefficient: ``Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)2) * Σ((y - y̅)2))``.

Parametric aggregate functions
==============================
Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters - constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

sequenceMatch(pattern)(time, cond1, cond2, ...)
-----------------------------------------------
Pattern matching for event chains.

'pattern' is a string containing a pattern to match. The pattern is similar to a regular expression.
'time' is the event time of the DateTime type.
'cond1, cond2 ...' are from one to 32 arguments of the UInt8 type that indicate whether an event condition was met.

The function collects a sequence of events in RAM. Then it checks whether this sequence matches the pattern.
It returns UInt8 - 0 if the pattern isn't matched, or 1 if it matches.

Example: ``sequenceMatch('(?1).*(?2)')(EventTime, URL LIKE '%company%', URL LIKE '%cart%')``
- whether there was a chain of events in which pages with the address in company were visited earlier than pages with the address in cart.

This is a simple example. You could write it using other aggregate functions:

.. code-block:: sql

    minIf(EventTime, URL LIKE '%company%') < maxIf(EventTime, URL LIKE '%cart%').

However, there is no such solution for more complex situations.

Pattern syntax:
``(?1)`` - Reference to a condition (any number in place of 1).
``.*`` - Any number of events.
``(?t>=1800)`` - Time condition.
Any quantity of any type of events is allowed over the specified time.
The operators <, >, <= may be used instead of  >=.
Any number may be specified in place of 1800.

Events that occur during the same second may be put in the chain in any order. This may affect the result of the function.

sequenceCount(pattern)(time, cond1, cond2, ...)
-----------------------------------------------
Similar to the sequenceMatch function, but it does not return the fact that there is a chain of events, and UInt64 is the number of strings found.
Chains are searched without overlapping. That is, the following chain can start only after the end of the previous one.

uniqUpTo(N)(x)
--------------
Calculates the number of different argument values, if it is less than or equal to N.
If the number of different argument values is greater than N, it returns N + 1.

Recommended for use with small Ns, up to 10. The maximum N value is 100.

For the state of an aggregate function, it uses the amount of memory equal to 1 + N * the size of one value of bytes.
For strings, it stores a non-cryptographic hash of 8 bytes. That is, the calculation is approximated for strings.

It works as fast as possible, except for cases when a large N value is used and the number of unique values is slightly less than N.

Usage example:
Problem: Generate a report that shows only keywords that produced at least 5 unique users.
Solution: Write in the query ``GROUP BY SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5``

Aggregate function combinators
==============================
The name of an aggregate function can have a suffix appended to it. This changes the way the aggregate function works.
There are ``If`` and ``Array`` combinators. See the sections below.

If combinator. Conditional aggregate functions
----------------------------------------------
The suffix ``-If`` can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument - a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

Examples: ``sumIf(column, cond)``, ``countIf(cond)``, ``avgIf(x, cond)``, ``quantilesTimingIf(level1, level2)(x, cond)``, ``argMinIf(arg, val, cond)`` and so on.

You can use aggregate functions to calculate aggregates for multiple conditions at once, without using subqueries and JOINs.
For example, in Yandex.Metrica, we use conditional aggregate functions for implementing segment comparison functionality.

Array combinator. Aggregate functions for array arguments
---------------------------------------------------------
The -Array suffix can be appended to any aggregate function. In this case, the aggregate function takes arguments of the 'Array(T)' type (arrays) instead of 'T' type arguments. If the aggregate function accepts multiple arguments, this must be arrays of equal lengths. When processing arrays, the aggregate function works like the original aggregate function across all array elements.

Example 1: ``sumArray(arr)`` - Totals all the elements of all 'arr' arrays. In this example, it could have been written more simply: sum(arraySum(arr)).

Example 2: ``uniqArray(arr)`` - Count the number of unique elements in all 'arr' arrays. This could be done an easier way: ``uniq(arrayJoin(arr))``, but it's not always possible to add 'arrayJoin' to a query.

The ``-If`` and ``-Array`` combinators can be used together. However, 'Array' must come first, then 'If'. 
Examples: ``uniqArrayIf(arr, cond)``,  ``quantilesTimingArrayIf(level1, level2)(arr, cond)``. Due to this order, the 'cond' argument can't be an array.

State combinator
----------------
If this combinator is used, the aggregate function returns intermediate aggregation state (for example, in the case of the ``uniqCombined`` function, a HyperLogLog structure for calculating the number of unique values), which has type of ``AggregateFunction(...)`` and can be used for further processing or can be saved to a table for subsequent pre-aggregation - see the sections "AggregatingMergeTree" and "functions for working with intermediate aggregation states".

Merge combinator
----------------
In the case of using this combinator, the aggregate function will take as an argument the intermediate state of an aggregation, pre-aggregate (combine together) these states, and return the finished/complete value.

MergeState combinator
---------------------
Merges the intermediate aggregation states, similar to the -Merge combinator, but returns a non-complete value, but an intermediate aggregation state, similar to the -State combinator.
