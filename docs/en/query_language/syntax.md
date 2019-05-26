# Syntax

There are two types of parsers in the system: the full SQL parser (a recursive descent parser), and the data format parser (a fast stream parser).
In all cases except the `INSERT` query, only the full SQL parser is used.
The `INSERT` query uses both parsers:

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

The `INSERT INTO t VALUES` fragment is parsed by the full parser, and the data `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` is parsed by the fast stream parser. You can also turn on the full parser for the data by using the [input_format_values_interpret_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) setting. When `input_format_values_interpret_expressions = 1`, ClickHouse first tries to parse values with the fast stream parser. If it fails, ClickHouse tries to use the full parser for the data, treating it like an SQL [expression](#syntax-expressions).

Data can have any format. When a query is received, the server calculates no more than [max_query_size](../operations/settings/settings.md#settings-max_query_size) bytes of the request in RAM (by default, 1 MB), and the rest is stream parsed.
This means the system doesn't have problems with large `INSERT` queries, like MySQL does.

When using the `Values` format in an `INSERT` query, it may seem that data is parsed the same as expressions in a `SELECT` query, but this is not true. The `Values` format is much more limited.

Next we will cover the full parser. For more information about format parsers, see the [Formats](../interfaces/formats.md) section.

## Spaces

There may be any number of space symbols between syntactical constructions (including the beginning and end of a query). Space symbols include the space, tab, line feed, CR, and form feed.

## Comments

SQL-style and C-style comments are supported.
SQL-style comments: from `--` to the end of the line. The space after `--` can be omitted.
Comments in C-style: from `/*` to `*/`. These comments can be multiline. Spaces are not required here, either.

## Keywords {#syntax-keywords}

Keywords (such as `SELECT`) are not case-sensitive. Everything else (column names, functions, and so on), in contrast to standard SQL, is case-sensitive.

Keywords are not reserved (they are just parsed as keywords in the corresponding context). If you use [identifiers](#syntax-identifiers) the same as the keywords, enclose them into quotes. For example, the query `SELECT "FROM" FROM table_name` is valid if the table `table_name` has column with the name `"FROM"`.

## Identifiers {#syntax-identifiers}

Identifiers are:

- Cluster, database, table, partition and column names.
- Functions.
- Data types.
- [Expression aliases](#syntax-expression_aliases).

Identifiers can be quoted or non-quoted. It is recommended to use non-quoted identifiers.

Non-quoted identifiers must match the regex `^[a-zA-Z_][0-9a-zA-Z_]*$` and can not be equal to [keywords](#syntax-keywords).  Examples: `x, _1, X_y__Z123_.`

If you want to use identifiers the same as keywords or you want to use other symbols in identifiers, quote it using double quotes or backticks, for example, `"id"`, `` `id` ``.

## Literals

There are: numeric, string, compound and `NULL` literals.


### Numeric

A numeric literal tries to be parsed:

- First as a 64-bit signed number, using the [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) function.
- If unsuccessful, as a 64-bit unsigned number, using the [strtoll](https://en.cppreference.com/w/cpp/string/byte/strtol) function.
- If unsuccessful, as a floating-point number using the [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) function.
- Otherwise, an error is returned.

The corresponding value will have the smallest type that the value fits in.
For example, 1 is parsed as `UInt8`, but 256 is parsed as `UInt16`. For more information, see [Data types](../data_types/index.md).

Examples: `1`, `18446744073709551615`, `0xDEADBEEF`, `01`, `0.1`, `1e100`, `-1e-100`, `inf`, `nan`.


### String

Only string literals in single quotes are supported. The enclosed characters can be backslash-escaped. The following escape sequences have a corresponding special value: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`. In all other cases, escape sequences in the format `\c`, where `c` is any character, are converted to `c`. This means that you can use the sequences `\'`and`\\`. The value will have the [String](../data_types/string.md) type.

The minimum set of characters that you need to escape in string literals: `'` and `\`. Single quote can be escaped with the single quote, literals `'It\'s'` and `'It''s'` are equal.

### Compound

Constructions are supported for arrays: `[1, 2, 3]` and tuples: `(1, 'Hello, world!', 2)`..
Actually, these are not literals, but expressions with the array creation operator and the tuple creation operator, respectively.
An array must consist of at least one item, and a tuple must have at least two items.
Tuples have a special purpose for use in the `IN` clause of a `SELECT` query. Tuples can be obtained as the result of a query, but they can't be saved to a database (with the exception of [Memory](../operations/table_engines/memory.md) tables).

### NULL {#null-literal}

Indicates that the value is missing.

In order to store `NULL` in a table field, it must be of the [Nullable](../data_types/nullable.md) type.

Depending on the data format (input or output), `NULL` may have a different representation. For more information, see the documentation for [data formats](../interfaces/formats.md#formats).

There are many nuances to processing `NULL`. For example, if at least one of the arguments of a comparison operation is `NULL`, the result of this operation will also be `NULL`. The same is true for multiplication, addition, and other operations. For more information, read the documentation for each operation.

In queries, you can check `NULL` using the [IS NULL](operators.md#operator-is-null) and [IS NOT NULL](operators.md) operators and the related functions `isNull` and `isNotNull`.

## Functions

Functions are written like an identifier with a list of arguments (possibly empty) in brackets. In contrast to standard SQL, the brackets are required, even for an empty arguments list. Example: `now()`.
There are regular and aggregate functions (see the section "Aggregate functions"). Some aggregate functions can contain two lists of arguments in brackets. Example: ` quantile (0.9) (x)`. These aggregate functions are called "parametric" functions, and the arguments in the first list are called "parameters". The syntax of aggregate functions without parameters is the same as for regular functions.

## Operators

Operators are converted to their corresponding functions during query parsing, taking their priority and associativity into account.
For example, the expression `1 + 2 * 3 + 4` is transformed to `plus(plus(1, multiply(2, 3)), 4)`.


## Data Types and Database Table Engines

Data types and table engines in the `CREATE` query are written the same way as identifiers or functions. In other words, they may or may not contain an arguments list in brackets. For more information, see the sections "Data types," "Table engines," and "CREATE".

## Expression Aliases {#syntax-expression_aliases}

An alias is a user-defined name for an expression in a query.

```
expr AS alias
```

- `AS` — The keyword for defining aliases. You can define the alias for a table name or a column name in a `SELECT` clause without using the `AS` keyword.

    For example, `SELECT table_name_alias.column_name FROM table_name table_name_alias`.

    In the [CAST](functions/type_conversion_functions.md#type_conversion_function-cast) function, the `AS` keyword has another meaning. See the description of the function.

- `expr` — Any expression supported by ClickHouse.

    For example, `SELECT column_name * 2 AS double FROM some_table`.

- `alias` — Name for `expr`. Aliases should comply with the [identifiers](#syntax-identifiers) syntax.

    For example, `SELECT "table t".column_name FROM table_name AS "table t"`.

### Notes on Usage

Aliases are global for a query or subquery and you can define an alias in any part of a query for any expression. For example, `SELECT (1 AS n) + 2, n`.

Aliases are not visible in subqueries and between subqueries. For example, while executing the query `SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a` ClickHouse generates the exception `Unknown identifier: num`.

If an alias is defined for the result columns in the `SELECT` clause of a subquery, these columns are visible in the outer query. For example, `SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.

Be careful with aliases that are the same as column or table names. Let's consider the following example:

```
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog()
```

```
SELECT
    argMax(a, b),
    sum(b) AS b
FROM t

Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

In this example, we declared table `t` with column `b`. Then, when selecting data, we defined the `sum(b) AS b` alias. As aliases are global, ClickHouse substituted the literal `b` in the expression `argMax(a, b)` with the expression `sum(b)`. This substitution caused the exception.

## Asterisk

In a `SELECT` query, an asterisk can replace the expression. For more information, see the section "SELECT".

## Expressions {#syntax-expressions}

An expression is a function, identifier, literal, application of an operator, expression in brackets, subquery, or asterisk. It can also contain an alias.
A list of expressions is one or more expressions separated by commas.
Functions and operators, in turn, can have expressions as arguments.


[Original article](https://clickhouse.yandex/docs/en/query_language/syntax/) <!--hide-->
