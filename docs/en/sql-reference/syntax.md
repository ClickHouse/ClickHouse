---
description: 'Documentation for Syntax'
displayed_sidebar: 'sqlreference'
sidebar_label: 'Syntax'
sidebar_position: 2
slug: /sql-reference/syntax
title: 'Syntax'
doc_type: 'reference'
---

In this section, we will take a look at ClickHouse's SQL syntax. 
ClickHouse uses a syntax based on SQL but offers a number of extensions and optimizations.

## Query Parsing {#query-parsing}

There are two types of parsers in ClickHouse:
- _A full SQL parser_ (a recursive descent parser).
- _A data format parser_ (a fast stream parser).

The full SQL parser is used in all cases except for the `INSERT` query, which uses both parsers. 

Let's examine the query below:

```sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

As mentioned already, the `INSERT` query makes use of both parsers. 
The `INSERT INTO t VALUES` fragment is parsed by the full parser, 
and the data `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` is parsed by the data format parser, or fast stream parser.

<details>
<summary>Turning on the full parser</summary>

You can also turn on the full parser for the data 
by using the [`input_format_values_interpret_expressions`](../operations/settings/settings-formats.md#input_format_values_interpret_expressions) setting. 

When the aforementioned setting is set to `1`, 
ClickHouse first tries to parse values with the fast stream parser. 
If it fails, ClickHouse tries to use the full parser for the data, treating it like an SQL [expression](#expressions).
</details>

The data can have any format. 
When a query is received, the server calculates no more than [max_query_size](../operations/settings/settings.md#max_query_size) bytes of the request in RAM 
(by default, 1 MB), and the rest is stream parsed.
This is to allow for avoiding issues with large `INSERT` queries, which is the recommended way to insert your data in ClickHouse.

When using the [`Values`](/interfaces/formats/Values) format in an `INSERT` query, 
it may appear that data is parsed the same as for expressions in a `SELECT` query however this is not the case. 
The `Values` format is much more limited.

The rest of this section covers the full parser. 

:::note
For more information about format parsers, see the [Formats](../interfaces/formats.md) section.
:::

## Spaces {#spaces}

- There may be any number of space symbols between syntactical constructions (including the beginning and end of a query). 
- Space symbols include the space, tab, line feed, CR, and form feed.

## Comments {#comments}

ClickHouse supports both SQL-style and C-style comments:

- SQL-style comments begin with `--`, `#!` or `# ` and continue to the end of the line. A space after `--` and `#!` can be omitted.
- C-style comments span from `/*` to `*/` and can be multiline. Spaces are not required either.

## Keywords {#keywords}

Keywords in ClickHouse can be either _case-sensitive_ or _case-insensitive_ depending on the context.

Keywords are **case-insensitive** when they correspond to:

- SQL standard. For example, `SELECT`, `select` and `SeLeCt` are all valid.
- Implementation in some popular DBMS (MySQL or Postgres). For example, `DateTime` is the same as `datetime`.

:::note
You can check whether a data type name is case-sensitive in the [system.data_type_families](/operations/system-tables/data_type_families) table.
:::

In contrast to standard SQL, all other keywords (including functions names) are **case-sensitive**.

Furthermore, Keywords are not reserved. 
They are treated as such only in the corresponding context. 
If you use [identifiers](#identifiers) with the same name as the keywords, enclose them into double-quotes or backticks. 

For example, the following query is valid if the table `table_name` has a column with the name `"FROM"`:

```sql
SELECT "FROM" FROM table_name
```

## Identifiers {#identifiers}

Identifiers are:

- Cluster, database, table, partition, and column names.
- [Functions](#functions).
- [Data types](../sql-reference/data-types/index.md).
- [Expression aliases](#expression-aliases).

Identifiers can be quoted or non-quoted, although the latter is preferred.

Non-quoted identifiers must match the regex `^[a-zA-Z_][0-9a-zA-Z_]*$` and cannot be equal to [keywords](#keywords).
See the table below for examples of valid and invalid identifiers:

| Valid identifiers                              | Invalid identifiers                    |
|------------------------------------------------|----------------------------------------|
| `xyz`, `_internal`, `Id_with_underscores_123_` | `1x`, `tom@gmail.com`, `äußerst_schön` |

If you want to use identifiers the same as keywords or you want to use other symbols in identifiers, quote it using double quotes or backticks, for example, `"id"`, `` `id` ``.

:::note
The same rules that apply for escaping in quoted identifiers also apply for string literals. See [String](#string) for more details.
:::

## Literals {#literals}

In ClickHouse, a literal is a value which is directly represented in a query.
In other words it is a fixed value which does not change during query execution.

Literals can be:
- [String](#string)
- [Numeric](#numeric)
- [Compound](#compound)
- [`NULL`](#null)
- [Heredocs](#heredoc) (custom string literals)

We take a look at each of these in more detail in the sections below.

### String {#string}

String literals must be enclosed in single quotes. Double quotes are not supported.

Escaping works by either:

- using a preceding single quote where the single-quote character `'` (and only this character) can be escaped as `''`, or
- using the preceding backslash with the following supported escape sequences listed in the table below.

:::note
The backslash loses its special meaning i.e. it is interpreted literally should it precede characters other than the ones listed below.
:::

| Supported Escape                    | Description                                                             |
|-------------------------------------|-------------------------------------------------------------------------|
| `\xHH`                              | 8-bit character specification followed by any number of hex digits (H). | 
| `\N`                                | reserved, does nothing (eg `SELECT 'a\Nb'` returns `ab`)                |
| `\a`                                | alert                                                                   |
| `\b`                                | backspace                                                               |
| `\e`                                | escape character                                                        |
| `\f`                                | form feed                                                               |
| `\n`                                | line feed                                                               |
| `\r`                                | carriage return                                                         |
| `\t`                                | horizontal tab                                                          |
| `\v`                                | vertical tab                                                            |
| `\0`                                | null character                                                          |
| `\\`                                | backslash                                                               |
| `\'` (or ` '' `)                    | single quote                                                            |
| `\"`                                | double quote                                                            |
| `` ` ``                             | backtick                                                                |
| `\/`                                | forward slash                                                           |
| `\=`                                | equal sign                                                              |
| ASCII control characters (c &lt;= 31). |                                                                      |

:::note
In string literals, you need to escape at least `'` and `\` using escape codes `\'` (or: `''`) and `\\`.
:::

### Numeric {#numeric}

Numeric literals are parsed as follows:

- If the literal is prefixed with a minus sign `-`, the token is skipped and the result is negated after parsing.
- The numeric literal is first parsed as a 64-bit unsigned integer, using the [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) function.
  - If the value is prefixed with `0b` or `0x`/`0X`, the number is parsed as binary or hexadecimal, respectively.
  - If the value is negative and the absolute magnitude is greater than 2<sup>63</sup>, an error is returned.
- If unsuccessful, the value is next parsed as a floating-point number using the [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) function.
- Otherwise, an error is returned.

Literal values are cast to the smallest type that the value fits in.
For example:
- `1` is parsed as `UInt8`
- `256` is parsed as `UInt16`. 

:::note Important
Integer values wider than 64-bit (`UInt128`, `Int128`, `UInt256`, `Int256`) must be cast to a larger type to parse properly:

```sql
-170141183460469231731687303715884105728::Int128
340282366920938463463374607431768211455::UInt128
-57896044618658097711785492504343953926634992332820282019728792003956564819968::Int256
115792089237316195423570985008687907853269984665640564039457584007913129639935::UInt256
```

This bypasses the above algorithm and parses the integer with a routine that supports arbitrary precision.

Otherwise, the literal will be parsed as a floating-point number and thus subject to loss of precision due to truncation.
:::

For more information, see [Data types](../sql-reference/data-types/index.md).

Underscores `_` inside numeric literals are ignored and can be used for better readability.

The following Numeric literals are supported:

| Numeric Literal                           | Examples                                        |
|-------------------------------------------|-------------------------------------------------|
| **Integers**                              | `1`, `10_000_000`, `18446744073709551615`, `01` |
| **Decimals**                              | `0.1`                                           |
| **Exponential notation**                  | `1e100`, `-1e-100`                              |
| **Floating point numbers**                | `123.456`, `inf`, `nan`                         |
| **Hex**                                   | `0xc0fe`                                        |
| **SQL Standard compatible hex string**    | `x'c0fe'`                                       |
| **Binary**                                | `0b1101`                                        |
| **SQL Standard compatible binary string** | `b'1101'`                                       |

:::note
Octal literals are not supported to avoid accidental errors in interpretation.
:::

### Compound {#compound}

Arrays are constructed with square brackets `[1, 2, 3]`. Tuples are constructed with round brackets `(1, 'Hello, world!', 2)`.
Technically these are not literals, but expressions with the array creation operator and the tuple creation operator, respectively.
An array must consist of at least one item, and a tuple must have at least two items.

:::note
There is a separate case when tuples appear in the `IN` clause of a `SELECT` query. 
Query results can include tuples, but tuples cannot be saved to a database (except for tables using the [Memory](../engines/table-engines/special/memory.md) engine).
:::

### NULL {#null}

`NULL` is used to indicate that a value is missing. 
To store `NULL` in a table field, it must be of the [Nullable](../sql-reference/data-types/nullable.md) type.

:::note
The following should be noted for `NULL`:

- Depending on the data format (input or output), `NULL` may have a different representation. For more information, see [data formats](/interfaces/formats).
- `NULL` processing is nuanced. For example, if at least one of the arguments of a comparison operation is `NULL`, the result of this operation is also `NULL`. The same is true for multiplication, addition, and other operations. We recommend to read the documentation for each operation.
- In queries, you can check `NULL` using the [`IS NULL`](/sql-reference/functions/functions-for-nulls#isNull) and [`IS NOT NULL`](/sql-reference/functions/functions-for-nulls#isNotNull) operators and the related functions `isNull` and `isNotNull`.
:::

### Heredoc {#heredoc}

A [heredoc](https://en.wikipedia.org/wiki/Here_document) is a way to define a string (often multiline), while maintaining the original formatting. 
A heredoc is defined as a custom string literal, placed between two `$` symbols.

For example:

```sql
SELECT $heredoc$SHOW CREATE VIEW my_view$heredoc$;

┌─'SHOW CREATE VIEW my_view'─┐
│ SHOW CREATE VIEW my_view   │
└────────────────────────────┘
```

:::note
- A value between two heredocs is processed "as-is".
:::

:::tip
- You can use a heredoc to embed snippets of SQL, HTML, or XML code, etc.
:::

## Defining and Using Query Parameters {#defining-and-using-query-parameters}

Query parameters allow you to write generic queries that contain abstract placeholders instead of concrete identifiers. 
When a query with query parameters is executed, 
all placeholders are resolved and replaced by the actual query parameter values.

There are two ways to define a query parameter:

- `SET param_<name>=<value>`
- `--param_<name>='<value>'`

When using the second variant, it is passed as an argument to `clickhouse-client` on the command line where:
- `<name>` is the name of the query parameter.
- `<value>` is its value.

A query parameter can be referenced in a query using `{<name>: <datatype>}`, where `<name>` is the query parameter name and `<datatype>` is the datatype it is converted to.

<details>
<summary>Example with SET command</summary>

For example, the following SQL defines parameters named `a`, `b`, `c` and `d` - each with a different data type:

```sql
SET param_a = 13;
SET param_b = 'str';
SET param_c = '2022-08-04 18:30:53';
SET param_d = {'10': [11, 12], '13': [14, 15]};

SELECT
   {a: UInt32},
   {b: String},
   {c: DateTime},
   {d: Map(String, Array(UInt8))};

13    str    2022-08-04 18:30:53    {'10':[11,12],'13':[14,15]}
```
</details>

<details>
<summary>Example with clickhouse-client</summary>

If you are using `clickhouse-client`, the parameters are specified as `--param_name=value`. For example, the following parameter has the name `message` and it is retrieved as a `String`:

```bash
clickhouse-client --param_message='hello' --query="SELECT {message: String}"

hello
```

If the query parameter represents the name of a database, table, function or other identifier, use `Identifier` for its type. For example, the following query returns rows from a table named `uk_price_paid`:

```sql
SET param_mytablename = "uk_price_paid";
SELECT * FROM {mytablename:Identifier};
```
</details>

:::note
Query parameters are not general text substitutions which can be used in arbitrary places in arbitrary SQL queries. 
They are primarily designed to work in `SELECT` statements in place of identifiers or literals.
:::

## Functions {#functions}

Function calls are written like an identifier with a list of arguments (possibly empty) in round brackets. 
In contrast to standard SQL, the brackets are required, even for an empty argument list. 
For example: 

```sql
now()
```

There are also:
- [Regular functions](/sql-reference/functions/overview).
- [Aggregate functions](/sql-reference/aggregate-functions).

Some aggregate functions can contain two lists of arguments in brackets. For example: 

```sql
quantile (0.9)(x) 
```

These aggregate functions are called "parametric" functions, 
and the arguments in the first list are called "parameters".

:::note
The syntax of aggregate functions without parameters is the same as for regular functions.
:::

## Operators {#operators}

Operators are converted to their corresponding functions during query parsing, taking their priority and associativity into account.

For example, the expression 

```text
1 + 2 * 3 + 4
```

is transformed to 

```text
plus(plus(1, multiply(2, 3)), 4)`
```

## Data Types and Database Table Engines {#data-types-and-database-table-engines}

Data types and table engines in the `CREATE` query are written the same way as identifiers or functions. 
In other words, they may or may not contain an argument list in brackets. 

For more information, see the sections:
- [Data types](/sql-reference/data-types/index.md)
- [Table engines](/engines/table-engines/index.md)
- [CREATE](/sql-reference/statements/create/index.md).

## Expressions {#expressions}

An expression can be any of the following:
- a function
- an identifier
- a literal
- the application of an operator
- an expression in brackets
- a subquery
- an asterisk

It can also contain an [alias](#expression-aliases).

A list of expressions is one or more expressions separated by commas.
Functions and operators, in turn, can have expressions as arguments.

A constant expression is an expression whose result is known during query analysis, i.e. before execution.
For example, expressions over literals are constant expressions.

## Expression Aliases {#expression-aliases}

An alias is a user-defined name for an [expression](#expressions) in a query.

```sql
expr AS alias
```

The parts of the syntax above are explained below.

| Part of syntax | Description                                                                                                                                      | Example                                                                 | Notes                                                                                                                                                |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AS`           | The keyword for defining aliases. You can define the alias for a table name or a column name in a `SELECT` clause without using the `AS` keyword.| `SELECT table_name_alias.column_name FROM table_name table_name_alias`. | In the [CAST](/sql-reference/functions/type-conversion-functions#cast) function, the `AS` keyword has another meaning. See the description of the function. |
| `expr`         | Any expression supported by ClickHouse.                                                                                                          | `SELECT column_name * 2 AS double FROM some_table`                      |                                                                                                                                                      |
| `alias`        | Name for `expr`. Aliases should comply with the [identifiers](#identifiers) syntax.                                                                       | `SELECT "table t".column_name FROM table_name AS "table t"`.            |                                                                                                                                                      |

### Notes on Usage {#notes-on-usage}

- Aliases are global for a query or subquery, and you can define an alias in any part of a query for any expression. For example:

```sql
SELECT (1 AS n) + 2, n`.
```

- Aliases are not visible in subqueries and between subqueries. For example, while executing the following query ClickHouse generates the exception `Unknown identifier: num`:

```sql
`SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a`
```

- If an alias is defined for the result columns in the `SELECT` clause of a subquery, these columns are visible in the outer query. For example:

```sql
SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.
```

- Be careful with aliases that are the same as column or table names. Let's consider the following example:

```sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog();

SELECT
    argMax(a, b),
    sum(b) AS b
FROM t;

Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

In the preceding example, we declared table `t` with column `b`. 
Then, when selecting data, we defined the `sum(b) AS b` alias. 
As aliases are global, 
ClickHouse substituted the literal `b` in the expression `argMax(a, b)` with the expression `sum(b)`. 
This substitution caused the exception.

:::note
You can change this default behavior by setting [prefer_column_name_to_alias](/operations/settings/settings#prefer_column_name_to_alias) to `1`.
:::

## Asterisk {#asterisk}

In a `SELECT` query, an asterisk can replace the expression. 
For more information, see the section [SELECT](/sql-reference/statements/select/index.md#asterisk).
