# Syntax

There are two types of parsers in the system: the full SQL parser (a recursive descent parser), and the data format parser (a fast stream parser).
In all cases except the INSERT query, only the full SQL parser is used.
The INSERT query uses both parsers:

```sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

The `INSERT INTO t VALUES` fragment is parsed by the full parser, and the data `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` is parsed by the fast stream parser.
Data can have any format. When a query is received, the server calculates no more than `max_query_size` bytes of the request in RAM (by default, 1 MB), and the rest is stream parsed.
This means the system doesn't have problems with large INSERT queries, like MySQL does.

When using the Values format in an INSERT query, it may seem that data is parsed the same as expressions in a SELECT query, but this is not true. The Values format is much more limited.

Next we will cover the full parser. For more information about format parsers, see the section "Formats".

## Spaces

There may be any number of space symbols between syntactical constructions (including the beginning and end of a query). Space symbols include the space, tab, line feed, CR, and form feed.

## Comments

SQL-style and C-style comments are supported.
SQL-style comments: from `--` to the end of the line. The space after `--` can be omitted.
Comments in C-style: from `/*`  to `*/`. These comments can be multiline. Spaces are not required here, either.

## Keywords

Keywords (such as `SELECT`) are not case-sensitive. Everything else (column names, functions, and so on), in contrast to standard SQL, is case-sensitive. Keywords are not reserved (they are just parsed as keywords in the corresponding context).

## Identifiers

Identifiers (column names, functions, and data types) can be quoted or non-quoted.
Non-quoted identifiers start with a Latin letter or underscore, and continue with a Latin letter, underscore, or number. In other words, they must match the regex `^[a-zA-Z_][0-9a-zA-Z_]*$`. Examples: `x, _1, X_y__Z123_.`

Quoted identifiers are placed in reversed quotation marks ` `id` ` (the same as in MySQL), and can indicate any set of bytes (non-empty). In addition, symbols (for example, the reverse quotation mark) inside this type of identifier can be backslash-escaped. Escaping rules are the same as for string literals (see below).
We recommend using identifiers that do not need to be quoted.

## Literals

### Numeric

A numeric literal tries to be parsed:

- First as a 64-bit signed number, using the `strtoull` function.
- If unsuccessful, as a 64-bit unsigned number, using the `strtoll` function.
- If unsuccessful, as a floating-point number using the `strtod` function.
- Otherwise, an error is returned.

The corresponding value will have the smallest type that the value fits in.
For example, 1 is parsed as `UInt8`, and 256 is parsed as `UInt16`. For more information, see the section [Data types](../data_types/index.md#data_types).

Examples: `1`, `18446744073709551615`, `0xDEADBEEF`, `01`, `0.1`, `1e100`, `-1e-100`, `inf`, `nan`.

### String

Only string literals in single quotes are supported. The enclosed characters can be backslash-escaped. The following escape sequences have a corresponding special value: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`. In all other cases, escape sequences in the format `\c`, where "c" is any character, are converted to "c". This means that you can use the sequences `\'`and`\\`. The value will be of type [String](../data_types/string.md#data_types-string).

The minimum set of characters that you need to escape in string literals: `'` and `\`.

### Compound

Constructions are supported for arrays: `[1, 2, 3]` and tuples: `(1, 'Hello, world!', 2)`..
Actually, these are not literals, but expressions with the array creation operator and the tuple creation operator, respectively.
For more information, see the section "Operators2".
An array must consist of at least one item, and a tuple must have at least two items.
Tuples have a special purpose for use in the IN clause of a SELECT query. Tuples can be obtained as the result of a query, but they can't be saved to a database (with the exception of Memory-type tables).

<a name="null-literal"></a>

### NULL

Indicates that the value is missing.

In order to store `NULL` in a table field, it must be of the [Nullable](../data_types/nullable.md#data_type-nullable) type.

Depending on the data format (input or output), `NULL` may have a different representation. For more information, see the documentation for [data formats](../interfaces/formats.md#formats).

There are many nuances to processing `NULL`. For example, if at least one of the arguments of a comparison operation is `NULL`, the result of this operation will also be `NULL`. The same is true for multiplication, addition, and other operations. For more information, read the documentation for each operation.

In queries, you can check `NULL` using the [IS NULL](operators.md#operator-is-null) and [IS NOT NULL](operators.md#operator-is-not-null) operators and the related functions `isNull` and `isNotNull`.

## Functions

Functions are written like an identifier with a list of arguments (possibly empty) in brackets. In contrast to standard SQL, the brackets are required, even for an empty arguments list. Example: `now()`.
There are regular and aggregate functions (see the section "Aggregate functions"). Some aggregate functions can contain two lists of arguments in brackets. Example: ` quantile (0.9) (x)`. These aggregate functions are called "parametric" functions, and the arguments in the first list are called "parameters". The syntax of aggregate functions without parameters is the same as for regular functions.

## Operators

Operators are converted to their corresponding functions during query parsing, taking their priority and associativity into account.
For example, the expression `1 + 2 * 3 + 4` is transformed to `plus(plus(1, multiply(2, 3)), 4)`.
For more information, see the section "Operators" below.

## Data Types and Database Table Engines

Data types and table engines in the `CREATE` query are written the same way as identifiers or functions. In other words, they may or may not contain an arguments list in brackets. For more information, see the sections "Data types," "Table engines," and "CREATE".

## Synonyms

In the SELECT query, expressions can specify synonyms using the AS keyword. Any expression is placed to the left of AS. The identifier name for the synonym is placed to the right of AS. As opposed to standard SQL, synonyms are not only declared on the top level of expressions:

```sql
SELECT (1 AS n) + 2, n
```

In contrast to standard SQL, synonyms can be used in all parts of a query, not just `SELECT`.

## Asterisk

In a `SELECT` query, an asterisk can replace the expression. For more information, see the section "SELECT".

## Expressions

An expression is a function, identifier, literal, application of an operator, expression in brackets, subquery, or asterisk. It can also contain a synonym.
A list of expressions is one or more expressions separated by commas.
Functions and operators, in turn, can have expressions as arguments.
