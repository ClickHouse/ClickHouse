---
toc_folder_title: Functions
toc_priority: 32
toc_title: Introduction
---

# Functions {#functions}

There are at least\* two types of functions - regular functions (they are just called “functions”) and aggregate functions. These are completely different concepts. Regular functions work as if they are applied to each row separately (for each row, the result of the function doesn’t depend on the other rows). Aggregate functions accumulate a set of values from various rows (i.e. they depend on the entire set of rows).

In this section we discuss regular functions. For aggregate functions, see the section “Aggregate functions”.

\* - There is a third type of function that the ‘arrayJoin’ function belongs to; table functions can also be mentioned separately.\*

## Strong Typing {#strong-typing}

In contrast to standard SQL, ClickHouse has strong typing. In other words, it doesn’t make implicit conversions between types. Each function works for a specific set of types. This means that sometimes you need to use type conversion functions.

## Common Subexpression Elimination {#common-subexpression-elimination}

All expressions in a query that have the same AST (the same record or same result of syntactic parsing) are considered to have identical values. Such expressions are concatenated and executed once. Identical subqueries are also eliminated this way.

## Types of Results {#types-of-results}

All functions return a single return as the result (not several values, and not zero values). The type of result is usually defined only by the types of arguments, not by the values. Exceptions are the tupleElement function (the a.N operator), and the toFixedString function.

## Constants {#constants}

For simplicity, certain functions can only work with constants for some arguments. For example, the right argument of the LIKE operator must be a constant.
Almost all functions return a constant for constant arguments. The exception is functions that generate random numbers.
The ‘now’ function returns different values for queries that were run at different times, but the result is considered a constant, since constancy is only important within a single query.
A constant expression is also considered a constant (for example, the right half of the LIKE operator can be constructed from multiple constants).

Functions can be implemented in different ways for constant and non-constant arguments (different code is executed). But the results for a constant and for a true column containing only the same value should match each other.

## NULL Processing {#null-processing}

Functions have the following behaviors:

-   If at least one of the arguments of the function is `NULL`, the function result is also `NULL`.
-   Special behavior that is specified individually in the description of each function. In the ClickHouse source code, these functions have `UseDefaultImplementationForNulls=false`.

## Constancy {#constancy}

Functions can’t change the values of their arguments – any changes are returned as the result. Thus, the result of calculating separate functions does not depend on the order in which the functions are written in the query.

## Error Handling {#error-handling}

Some functions might throw an exception if the data is invalid. In this case, the query is canceled and an error text is returned to the client. For distributed processing, when an exception occurs on one of the servers, the other servers also attempt to abort the query.

## Evaluation of Argument Expressions {#evaluation-of-argument-expressions}

In almost all programming languages, one of the arguments might not be evaluated for certain operators. This is usually the operators `&&`, `||`, and `?:`.
But in ClickHouse, arguments of functions (operators) are always evaluated. This is because entire parts of columns are evaluated at once, instead of calculating each row separately.

## Performing Functions for Distributed Query Processing {#performing-functions-for-distributed-query-processing}

For distributed query processing, as many stages of query processing as possible are performed on remote servers, and the rest of the stages (merging intermediate results and everything after that) are performed on the requestor server.

This means that functions can be performed on different servers.
For example, in the query `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

-   if a `distributed_table` has at least two shards, the functions ‘g’ and ‘h’ are performed on remote servers, and the function ‘f’ is performed on the requestor server.
-   if a `distributed_table` has only one shard, all the ‘f’, ‘g’, and ‘h’ functions are performed on this shard’s server.

The result of a function usually doesn’t depend on which server it is performed on. However, sometimes this is important.
For example, functions that work with dictionaries use the dictionary that exists on the server they are running on.
Another example is the `hostName` function, which returns the name of the server it is running on in order to make `GROUP BY` by servers in a `SELECT` query.

If a function in a query is performed on the requestor server, but you need to perform it on remote servers, you can wrap it in an ‘any’ aggregate function or add it to a key in `GROUP BY`.

[Original article](https://clickhouse.tech/docs/en/query_language/functions/) <!--hide-->
