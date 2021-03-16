---
toc_priority: 60
toc_title: IN Operator
---

# Functions for Implementing the IN Operator {#functions-for-implementing-the-in-operator}

## in, notIn, globalIn, globalNotIn {#in-functions}

See the section [IN operators](../../sql-reference/operators/in.md#select-in-operators).

## tuple(x, y, …), operator (x, y, …) {#tuplex-y-operator-x-y}

A function that allows grouping multiple columns.
For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can’t be written to a table.

## tupleElement(tuple, n), operator x.N {#tupleelementtuple-n-operator-x-n}

A function that allows getting a column from a tuple.
‘N’ is the column index, starting from 1. N must be a constant. ‘N’ must be a constant. ‘N’ must be a strict postive integer no greater than the size of the tuple.
There is no cost to execute the function.

[Original article](https://clickhouse.tech/docs/en/query_language/functions/in_functions/) <!--hide-->
