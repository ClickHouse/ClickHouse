---
description: 'Documentation for the AggregateFunction data type in ClickHouse, which
  stores intermediate states of aggregate functions'
keywords: ['AggregateFunction', 'Type']
sidebar_label: 'AggregateFunction'
sidebar_position: 46
sidebar_label: 'AggregateFunction'
slug: /sql-reference/data-types/aggregatefunction
title: 'AggregateFunction Type'
---

# AggregateFunction Type

## Description {#description}

All [Aggregate functions](/sql-reference/aggregate-functions) in ClickHouse have
an implementation-specific intermediate state that can be serialized to an
`AggregateFunction` data type and stored in a table. This is usually done by 
means of a [materialized view](../../sql-reference/statements/create/view.md).

There are two aggregate function [combinators](/sql-reference/aggregate-functions/combinators)
commonly used with the `AggregateFunction` type:

- The [`-State`](/sql-reference/aggregate-functions/combinators#-state) aggregate function combinator, which when appended to an aggregate
  function name, produces `AggregateFunction` intermediate states.
- The [`-Merge`](/sql-reference/aggregate-functions/combinators#-merge) aggregate
  function combinator, which is used to get the final result of an aggregation 
  from the intermediate states.

## Syntax {#syntax}

```sql
AggregateFunction(aggregate_function_name, types_of_arguments...)
```

**Parameters**

- `aggregate_function_name` - The name of an aggregate function. If the function 
   is parametric, then its parameters should be specified too.
- `types_of_arguments` - The types of the aggregate function arguments.

for example:

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

## Usage {#usage}

### Data Insertion {#data-insertion}

To insert data into a table with columns of type `AggregateFunction`, you can 
use `INSERT SELECT` with aggregate functions and the
[`-State`](/sql-reference/aggregate-functions/combinators#-state) aggregate 
function combinator.

For example, to insert into columns of type `AggregateFunction(uniq, UInt64)` and
`AggregateFunction(quantiles(0.5, 0.9), UInt64)` you would use the following 
aggregate functions with combinators.

```sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

In contrast to functions `uniq` and `quantiles`, `uniqState` and `quantilesState`
(with `-State` combinator appended) return the state, rather than the final value.
In other words, they return a value of `AggregateFunction` type.

In the results of the `SELECT` query, values of type `AggregateFunction` have 
implementation-specific binary representations for all of the ClickHouse output
formats. 

If you dump data into, for example, the `TabSeparated` format with a `SELECT` 
query, then this dump can be loaded back using the `INSERT` query.

### Data Selection {#data-selection}

When selecting data from `AggregatingMergeTree` table, use the `GROUP BY` clause
and the same aggregate functions as for when you inserted the data, but use the 
[`-Merge`](/sql-reference/aggregate-functions/combinators#-merge) combinator.

An aggregate function with the `-Merge` combinator appended to it takes a set of 
states, combines them, and returns the result of the complete data aggregation.

For example, the following two queries return the same result:

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Usage Example {#usage-example}

See [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) engine description.

## Related Content {#related-content}

- Blog: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
- [MergeState](/sql-reference/aggregate-functions/combinators#-mergestate)
  combinator.
- [State](/sql-reference/aggregate-functions/combinators#-state) combinator.
