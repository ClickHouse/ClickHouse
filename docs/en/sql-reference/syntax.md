---
slug: /en/sql-reference/syntax
sidebar_position: 2
sidebar_label: Syntax
---

# Syntax

There are two types of parsers in the system: the full SQL parser (a recursive descent parser), and the data format parser (a fast stream parser).
In all cases except the `INSERT` query, only the full SQL parser is used.
The `INSERT` query uses both parsers:

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

The `INSERT INTO t VALUES` fragment is parsed by the full parser, and the data `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` is parsed by the fast stream parser. You can also turn on the full parser for the data by using the [input_format_values_interpret_expressions](../operations/settings/settings-formats.md#input_format_values_interpret_expressions) setting. When `input_format_values_interpret_expressions = 1`, ClickHouse first tries to parse values with the fast stream parser. If it fails, ClickHouse tries to use the full parser for the data, treating it like an SQL [expression](#expressions).

Data can have any format. When a query is received, the server calculates no more than [max_query_size](../operations/settings/settings.md#max_query_size) bytes of the request in RAM (by default, 1 MB), and the rest is stream parsed.
It allows for avoiding issues with large `INSERT` queries.

When using the `Values` format in an `INSERT` query, it may seem that data is parsed the same as expressions in a `SELECT` query, but this is not true. The `Values` format is much more limited.

The rest of this article covers the full parser. For more information about format parsers, see the [Formats](../interfaces/formats.md) section.

## Spaces

There may be any number of space symbols between syntactical constructions (including the beginning and end of a query). Space symbols include the space, tab, line feed, CR, and form feed.

## Comments

ClickHouse supports either SQL-style and C-style comments:

- SQL-style comments start with `--`, `#!` or `# ` and continue to the end of the line, a space after `--` and `#!` can be omitted.
- C-style are from `/*` to `*/`and can be multiline, spaces are not required either.

## Keywords

Keywords are case-insensitive when they correspond to:

- SQL standard. For example, `SELECT`, `select` and `SeLeCt` are all valid.
- Implementation in some popular DBMS (MySQL or Postgres). For example, `DateTime` is the same as `datetime`.

You can check whether a data type name is case-sensitive in the [system.data_type_families](../operations/system-tables/data_type_families.md#system_tables-data_type_families) table.

In contrast to standard SQL, all other keywords (including functions names) are **case-sensitive**.

Keywords are not reserved; they are treated as such only in the corresponding context. If you use [identifiers](#identifiers) with the same name as the keywords, enclose them into double-quotes or backticks. For example, the query `SELECT "FROM" FROM table_name` is valid if the table `table_name` has column with the name `"FROM"`.

## Identifiers

Identifiers are:

- Cluster, database, table, partition, and column names.
- Functions.
- Data types.
- [Expression aliases](#expression-aliases).

Identifiers can be quoted or non-quoted. The latter is preferred.

Non-quoted identifiers must match the regex `^[a-zA-Z_][0-9a-zA-Z_]*$` and can not be equal to [keywords](#keywords). Examples: `x`, `_1`, `X_y__Z123_`.

If you want to use identifiers the same as keywords or you want to use other symbols in identifiers, quote it using double quotes or backticks, for example, `"id"`, `` `id` ``.

## Literals

There are numeric, string, compound, and `NULL` literals.

### Numeric

Numeric literals are parsed as follows:

- First, as a 64-bit signed number, using the [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) function.
- If unsuccessful, as a 64-bit unsigned number, using the [strtoll](https://en.cppreference.com/w/cpp/string/byte/strtol) function.
- If unsuccessful, as a floating-point number using the [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) function.
- Otherwise, it returns an error.

Literal values are cast to the smallest type that the value fits in.
For example, 1 is parsed as `UInt8`, but 256 is parsed as `UInt16`. For more information, see [Data types](../sql-reference/data-types/index.md).
Underscores `_` inside numeric literals are ignored and can be used for better readability.

The following Numeric literals are supported:

**Integers** – `1`, `10_000_000`, `18446744073709551615`, `01`
**Decimals** – `0.1`
**Exponential notation** - `1e100`, `-1e-100`
**Floating point numbers** – `123.456`, `inf`, `nan`

**Hex** – `0xc0fe`
**SQL Standard compatible hex string** – `x'c0fe'`

**Binary** – `0b1101`
**SQL Standard compatible binary string** - `b'1101'`

Octal literals are not supported to avoid accidental errors in interpretation.

### String

String literals must be enclosed in single quotes, double quotes are not supported.
Escaping works either

- using a preceding single quote where the single-quote character `'` (and only this character) can be escaped as `''`, or
- using a preceding backslash with the following supported escape sequences: `\\`, `\'`, `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`. The backslash loses its special meaning, i.e. will be interpreted literally, if it precedes characters different than the listed ones.

In string literals, you need to escape at least `'` and `\` using escape codes `\'` (or: `''`) and `\\`.

### Compound

Arrays are constructed with square brackets `[1, 2, 3]`. Tuples are constructed with round brackets `(1, 'Hello, world!', 2)`.
Technically these are not literals, but expressions with the array creation operator and the tuple creation operator, respectively.
An array must consist of at least one item, and a tuple must have at least two items.
There’s a separate case when tuples appear in the `IN` clause of a `SELECT` query. Query results can include tuples, but tuples can’t be saved to a database (except of tables with [Memory](../engines/table-engines/special/memory.md) engine).

### NULL

Indicates that the value is missing.

In order to store `NULL` in a table field, it must be of the [Nullable](../sql-reference/data-types/nullable.md) type.

Depending on the data format (input or output), `NULL` may have a different representation. For more information, see the documentation for [data formats](../interfaces/formats.md#formats).

There are many nuances to processing `NULL`. For example, if at least one of the arguments of a comparison operation is `NULL`, the result of this operation is also `NULL`. The same is true for multiplication, addition, and other operations. For more information, read the documentation for each operation.

In queries, you can check `NULL` using the [IS NULL](../sql-reference/operators/index.md#is-null) and [IS NOT NULL](../sql-reference/operators/index.md#is-not-null) operators and the related functions `isNull` and `isNotNull`.

### Heredoc

A [heredoc](https://en.wikipedia.org/wiki/Here_document) is a way to define a string (often multiline), while maintaining the original formatting. A heredoc is defined as a custom string literal, placed between two `$` symbols, for example `$heredoc$`. A value between two heredocs is processed "as-is".

You can use a heredoc to embed snippets of SQL, HTML, or XML code, etc.

**Example**

Query:

```sql
SELECT $smth$SHOW CREATE VIEW my_view$smth$;
```

Result:

```text
┌─'SHOW CREATE VIEW my_view'─┐
│ SHOW CREATE VIEW my_view   │
└────────────────────────────┘
```

## Defining and Using Query Parameters

Query parameters allow you to write generic queries that contain abstract placeholders instead of concrete identifiers. When a query with query parameters is executed, all placeholders are resolved and replaced by the actual query parameter values.

There are two way to define a query parameter:

- use the `SET param_<name>=<value>` command
- use `--param_<name>='<value>'` as an argument to `clickhouse-client` on the command line. `<name>` is the name of the query parameter and `<value>` is its value

A query parameter can be referenced in a query using `{<name>: <datatype>}`, where `<name>` is the query parameter name and `<datatype>` is the datatype it is converted to.

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
```

Result:

```response
13	str	2022-08-04 18:30:53	{'10':[11,12],'13':[14,15]}
```

If you are using `clickhouse-client`, the parameters are specified as `--param_name=value`. For example, the following parameter has the name `message` and it is retrieved as a `String`:

```bash
clickhouse-client --param_message='hello' --query="SELECT {message: String}"
```

Result:

```response
hello
```

If the query parameter represents the name of a database, table, function or other identifier, use `Identifier` for its type. For example, the following query returns rows from a table named `uk_price_paid`:

```sql
SET param_mytablename = "uk_price_paid";
SELECT * FROM {mytablename:Identifier};
```

:::note
Query parameters are not general text substitutions which can be used in arbitrary places in arbitrary SQL queries. They are primarily designed to work in `SELECT` statements in place of identifiers or literals.
:::

## Functions

Function calls are written like an identifier with a list of arguments (possibly empty) in round brackets. In contrast to standard SQL, the brackets are required, even for an empty argument list. Example: `now()`.
There are regular and aggregate functions (see the section [Aggregate functions](/docs/en/sql-reference/aggregate-functions/index.md)). Some aggregate functions can contain two lists of arguments in brackets. Example: `quantile (0.9) (x)`. These aggregate functions are called “parametric” functions, and the arguments in the first list are called “parameters”. The syntax of aggregate functions without parameters is the same as for regular functions.

## Operators

Operators are converted to their corresponding functions during query parsing, taking their priority and associativity into account.
For example, the expression `1 + 2 * 3 + 4` is transformed to `plus(plus(1, multiply(2, 3)), 4)`.

## Data Types and Database Table Engines

Data types and table engines in the `CREATE` query are written the same way as identifiers or functions. In other words, they may or may not contain an argument list in brackets. For more information, see the sections [Data types](/docs/en/sql-reference/data-types/index.md), [Table engines](/docs/en/engines/table-engines/index.md), and [CREATE](/docs/en/sql-reference/statements/create/index.md).

## Expression Aliases

An alias is a user-defined name for expression in a query.

``` sql
expr AS alias
```

- `AS` — The keyword for defining aliases. You can define the alias for a table name or a column name in a `SELECT` clause without using the `AS` keyword.

    For example, `SELECT table_name_alias.column_name FROM table_name table_name_alias`.

    In the [CAST](./functions/type-conversion-functions.md#castx-t) function, the `AS` keyword has another meaning. See the description of the function.

- `expr` — Any expression supported by ClickHouse.

    For example, `SELECT column_name * 2 AS double FROM some_table`.

- `alias` — Name for `expr`. Aliases should comply with the [identifiers](#identifiers) syntax.

    For example, `SELECT "table t".column_name FROM table_name AS "table t"`.

### Notes on Usage

Aliases are global for a query or subquery, and you can define an alias in any part of a query for any expression. For example, `SELECT (1 AS n) + 2, n`.

Aliases are not visible in subqueries and between subqueries. For example, while executing the query `SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a` ClickHouse generates the exception `Unknown identifier: num`.

If an alias is defined for the result columns in the `SELECT` clause of a subquery, these columns are visible in the outer query. For example, `SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.

Be careful with aliases that are the same as column or table names. Let’s consider the following example:

``` sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog()
```

``` sql
SELECT
    argMax(a, b),
    sum(b) AS b
FROM t
```

``` text
Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

In this example, we declared table `t` with column `b`. Then, when selecting data, we defined the `sum(b) AS b` alias. As aliases are global, ClickHouse substituted the literal `b` in the expression `argMax(a, b)` with the expression `sum(b)`. This substitution caused the exception. You can change this default behavior by setting [prefer_column_name_to_alias](../operations/settings/settings.md#prefer-column-name-to-alias) to `1`.

## Asterisk

In a `SELECT` query, an asterisk can replace the expression. For more information, see the section [SELECT](/docs/en/sql-reference/statements/select/index.md#asterisk).

## Expressions

An expression is a function, identifier, literal, application of an operator, expression in brackets, subquery, or asterisk. It can also contain an alias.
A list of expressions is one or more expressions separated by commas.
Functions and operators, in turn, can have expressions as arguments.
