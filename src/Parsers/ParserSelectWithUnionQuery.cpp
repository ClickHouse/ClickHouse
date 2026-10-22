#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserPipeOperators.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


namespace DB
{

bool ParserSelectWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list_node;
    ParserUnionList parser;

    if (!parser.parse(pos, list_node, expected))
        return false;

    /// NOTE: We can't simply flatten inner union query now, since we may have different union mode in query,
    /// so flatten may change it's semantics. For example:
    /// flatten `SELECT 1 UNION (SELECT 1 UNION ALL SELECT 1)` -> `SELECT 1 UNION SELECT 1 UNION ALL SELECT 1`

    /// If we got only one child which is ASTSelectWithUnionQuery, just lift it up
    auto & expr_list = list_node->as<ASTExpressionList &>();
    if (expr_list.children.size() == 1 && expr_list.children.at(0)->as<ASTSelectWithUnionQuery>())
    {
        node = std::move(expr_list.children.at(0));
    }
    else
    {
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();

        node = select_with_union_query;
        select_with_union_query->list_of_selects = list_node;
        select_with_union_query->children.push_back(select_with_union_query->list_of_selects);
        select_with_union_query->list_of_modes = parser.getUnionModes();
    }

    /// The query can be followed by a chain of pipe operators, e.g.: FROM t |> WHERE x |> LIMIT 1.
    if (pos->type == TokenType::PipeOperator)
        return parsePipeOperators(pos, node, expected);

    return true;
}

}

namespace DB
{

void registerStatementUnion(StatementFactory & factory)
{
    factory.registerStatement("UNION",
    {
        .description = R"DOCS_MD(
You can use `UNION` with explicitly specifying `UNION ALL` or `UNION DISTINCT`.

If you don't specify `ALL` or `DISTINCT`, it will depend on the `union_default_mode` setting. The difference between `UNION ALL` and `UNION DISTINCT` is that `UNION DISTINCT` will do a distinct transform for union result, it is equivalent to `SELECT DISTINCT` from a subquery containing `UNION ALL`.

You can use `UNION` to combine any number of `SELECT` queries by extending their results. Example:

```sql title="Query"
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Result columns are matched by their index (order inside `SELECT`). If column names do not match, names for the final result are taken from the first query.

Type casting is performed for unions. For example, if two queries being combined have the same field with non-`Nullable` and `Nullable` types from a compatible type, the resulting `UNION` has a `Nullable` type field.

Queries that are parts of `UNION` can be enclosed in `()`. [ORDER BY](/reference/statements/select/order-by) and [LIMIT](/reference/statements/select/limit) are applied to separate queries, not to the final result. If you need to apply a conversion to the final result, you can put all the queries with `UNION` in a subquery in the [FROM](/reference/statements/select/from) clause.

If you use `UNION` without explicitly specifying `UNION ALL` or `UNION DISTINCT`, you can specify the union mode using the [union_default_mode](/reference/settings/session-settings/other#union_default_mode) setting. The setting values can be `ALL`, `DISTINCT` or an empty string. However, if you use `UNION` with `union_default_mode` setting to empty string, it will throw an exception. The following examples demonstrate the results of queries with different values setting.

```sql title="Query"
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

```sql title="Query"
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

Queries that are parts of `UNION/UNION ALL/UNION DISTINCT` can be run simultaneously, and their results can be mixed together.

**See Also**

- [insert_null_as_default](/reference/settings/session-settings/insert#insert_null_as_default) setting.
- [union_default_mode](/reference/settings/session-settings/other#union_default_mode) setting.
)DOCS_MD",
        .syntax = R"(
SELECT ... UNION [ALL | DISTINCT] SELECT ... [UNION [ALL | DISTINCT] SELECT ...]
)",
        .parent = "SELECT",
        .related = {"SELECT", "INTERSECT", "EXCEPT", "DISTINCT", "JOIN"},
    });

    factory.registerStatement("INTERSECT",
    {
        .description = R"DOCS_MD(
The `INTERSECT` clause returns only those rows that result from both the first and the second queries. The queries must match the number of columns, order, and type. The result of `INTERSECT` can contain duplicate rows.

Multiple `INTERSECT` statements are executed left to right if parentheses are not specified. The `INTERSECT` operator has a higher priority than the `UNION` and `EXCEPT` clauses.

```sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

INTERSECT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```
The condition could be any expression based on your requirements.

## Examples {#examples}

Here is a simple example that intersects the numbers 1 to 10 with the numbers 3 to 8:

```sql title="Query"
SELECT number FROM numbers(1,10) INTERSECT SELECT number FROM numbers(3,8);
```

```response title="Response"
┌─number─┐
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
└────────┘
```

`INTERSECT` is useful if you have two tables that share a common column (or columns). You can intersect the results of two queries, as long as the results contain the same columns. For example, suppose we have a few million rows of historical cryptocurrency data that contains trade prices and volume:

```sql title="Query"
CREATE TABLE crypto_prices
(
    trade_date Date,
    crypto_name String,
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

INSERT INTO crypto_prices
   SELECT *
   FROM s3(
    'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_prices.csv',
    'CSVWithNames'
);

SELECT * FROM crypto_prices
WHERE crypto_name = 'Bitcoin'
ORDER BY trade_date DESC
LIMIT 10;
```

```response title="Response"
┌─trade_date─┬─crypto_name─┬──────volume─┬────price─┬───market_cap─┬──change_1_day─┐
│ 2020-11-02 │ Bitcoin     │ 30771456000 │ 13550.49 │ 251119860000 │  -0.013585099 │
│ 2020-11-01 │ Bitcoin     │ 24453857000 │ 13737.11 │ 254569760000 │ -0.0031840964 │
│ 2020-10-31 │ Bitcoin     │ 30306464000 │ 13780.99 │ 255372070000 │   0.017308505 │
│ 2020-10-30 │ Bitcoin     │ 30581486000 │ 13546.52 │ 251018150000 │   0.008084608 │
│ 2020-10-29 │ Bitcoin     │ 56499500000 │ 13437.88 │ 248995320000 │   0.012552661 │
│ 2020-10-28 │ Bitcoin     │ 35867320000 │ 13271.29 │ 245899820000 │   -0.02804481 │
│ 2020-10-27 │ Bitcoin     │ 33749879000 │ 13654.22 │ 252985950000 │    0.04427984 │
│ 2020-10-26 │ Bitcoin     │ 29461459000 │ 13075.25 │ 242251000000 │  0.0033826586 │
│ 2020-10-25 │ Bitcoin     │ 24406921000 │ 13031.17 │ 241425220000 │ -0.0058658565 │
│ 2020-10-24 │ Bitcoin     │ 24542319000 │ 13108.06 │ 242839880000 │   0.013650347 │
└────────────┴─────────────┴─────────────┴──────────┴──────────────┴───────────────┘
```

Now suppose we have a table named `holdings` that contains a list of cryptocurrencies that we own, along with the number of coins:

```sql title="Query"
CREATE TABLE holdings
(
    crypto_name String,
    quantity UInt64
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name);

INSERT INTO holdings VALUES
   ('Bitcoin', 1000),
   ('Bitcoin', 200),
   ('Ethereum', 250),
   ('Ethereum', 5000),
   ('DOGEFI', 10);
   ('Bitcoin Diamond', 5000);
```

We can use `INTERSECT` to answer questions like **"Which coins do we own that have traded at a price greater than $100?"**:

```sql title="Query"
SELECT crypto_name FROM holdings
INTERSECT
SELECT crypto_name FROM crypto_prices
WHERE price > 100
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Bitcoin     │
│ Ethereum    │
│ Ethereum    │
└─────────────┘
```

This means at some point in time, Bitcoin and Ethereum traded above $100, and DOGEFI and Bitcoin Diamond have never traded above $100 (at least using the data we have here in this example).

## INTERSECT DISTINCT {#intersect-distinct}

Notice in the previous query we had multiple Bitcoin and Ethereum holdings that traded above $100. It might be nice to remove duplicate rows (since they only repeat what we already know). You can add `DISTINCT` to `INTERSECT` to eliminate duplicate rows from the result:

```sql title="Query"
SELECT crypto_name FROM holdings
INTERSECT DISTINCT
SELECT crypto_name FROM crypto_prices
WHERE price > 100;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Ethereum    │
└─────────────┘
```

**See Also**

- [UNION](/reference/statements/select/union)
- [EXCEPT](/reference/statements/select/except)
)DOCS_MD",
        .syntax = R"(
SELECT column1 [, column2] FROM table1 [WHERE condition]
INTERSECT [ALL | DISTINCT]
SELECT column1 [, column2] FROM table2 [WHERE condition]
)",
        .parent = "SELECT",
        .related = {"SELECT", "UNION", "EXCEPT", "IN"},
    });

    factory.registerStatement("EXCEPT",
    {
        .description = R"DOCS_MD(
> The `EXCEPT` clause returns only those rows that result from the first query without the second.

- Both queries must have the same number of columns in the same order and data type.
- The result of `EXCEPT` can contain duplicate rows. Use `EXCEPT DISTINCT` if this is not desirable.
- Multiple `EXCEPT` statements are executed from left to right if parentheses are not specified.
- The `EXCEPT` operator has the same priority as the `UNION` clause and lower priority than the `INTERSECT` clause.

## Syntax {#syntax}

```sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

EXCEPT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]
```
The condition could be any expression based on your requirements.

Additionally, `EXCEPT()` can be used to exclude columns from a result in the same table, as is possible with BigQuery (Google Cloud), using the following syntax:

```sql
SELECT column1 [, column2 ] EXCEPT (column3 [, column4])
FROM table1
[WHERE condition]
```

## Examples {#examples}

The examples in this section demonstrate usage of the `EXCEPT` clause.

### Filtering Numbers Using the `EXCEPT` Clause {#filtering-numbers-using-the-except-clause}

Here is a simple example that returns the numbers 1 to 10 that are _not_ a part of the numbers 3 to 8:

```sql title="Query"
SELECT number
FROM numbers(1, 10)
EXCEPT
SELECT number
FROM numbers(3, 6)
```

```response title="Response"
┌─number─┐
│      1 │
│      2 │
│      9 │
│     10 │
└────────┘
```

### Excluding Specific Columns Using `EXCEPT()` {#excluding-specific-columns-using-except}

`EXCEPT()` can be used to quickly exclude columns from a result. For instance if we want to select all columns from a table, except a few select columns as shown in the example below:

```sql title="Query"
SHOW COLUMNS IN system.settings

SELECT * EXCEPT (default, alias_for, readonly, description)
FROM system.settings
LIMIT 5
```

```response title="Response"
    ┌─field───────┬─type─────────────────────────────────────────────────────────────────────┬─null─┬─key─┬─default─┬─extra─┐
 1. │ alias_for   │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 2. │ changed     │ UInt8                                                                    │ NO   │     │ ᴺᵁᴸᴸ    │       │
 3. │ default     │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 4. │ description │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 5. │ is_obsolete │ UInt8                                                                    │ NO   │     │ ᴺᵁᴸᴸ    │       │
 6. │ max         │ Nullable(String)                                                         │ YES  │     │ ᴺᵁᴸᴸ    │       │
 7. │ min         │ Nullable(String)                                                         │ YES  │     │ ᴺᵁᴸᴸ    │       │
 8. │ name        │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 9. │ readonly    │ UInt8                                                                    │ NO   │     │ ᴺᵁᴸᴸ    │       │
10. │ tier        │ Enum8('Production' = 0, 'Obsolete' = 4, 'Experimental' = 8, 'Beta' = 12) │ NO   │     │ ᴺᵁᴸᴸ    │       │
11. │ type        │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
12. │ value       │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
    └─────────────┴──────────────────────────────────────────────────────────────────────────┴──────┴─────┴─────────┴───────┘

   ┌─name────────────────────┬─value──────┬─changed─┬─min──┬─max──┬─type────┬─is_obsolete─┬─tier───────┐
1. │ dialect                 │ clickhouse │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ Dialect │           0 │ Production │
2. │ min_compress_block_size │ 65536      │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
3. │ max_compress_block_size │ 1048576    │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
4. │ max_block_size          │ 65409      │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
5. │ max_insert_block_size   │ 1048449    │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
   └─────────────────────────┴────────────┴─────────┴──────┴──────┴─────────┴─────────────┴────────────┘
```

### Using `EXCEPT` and `INTERSECT` with Cryptocurrency Data {#using-except-and-intersect-with-cryptocurrency-data}

`EXCEPT` and `INTERSECT` can often be used interchangeably with different Boolean logic, and they are both useful if you have two tables that share a common column (or columns).
For example, suppose we have a few million rows of historical cryptocurrency data that contains trade prices and volume:

```sql title="Query"
CREATE TABLE crypto_prices
(
    trade_date Date,
    crypto_name String,
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

INSERT INTO crypto_prices
   SELECT *
   FROM s3(
    'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_prices.csv',
    'CSVWithNames'
);

SELECT * FROM crypto_prices
WHERE crypto_name = 'Bitcoin'
ORDER BY trade_date DESC
LIMIT 10;
```

```response title="Response"
┌─trade_date─┬─crypto_name─┬──────volume─┬────price─┬───market_cap─┬──change_1_day─┐
│ 2020-11-02 │ Bitcoin     │ 30771456000 │ 13550.49 │ 251119860000 │  -0.013585099 │
│ 2020-11-01 │ Bitcoin     │ 24453857000 │ 13737.11 │ 254569760000 │ -0.0031840964 │
│ 2020-10-31 │ Bitcoin     │ 30306464000 │ 13780.99 │ 255372070000 │   0.017308505 │
│ 2020-10-30 │ Bitcoin     │ 30581486000 │ 13546.52 │ 251018150000 │   0.008084608 │
│ 2020-10-29 │ Bitcoin     │ 56499500000 │ 13437.88 │ 248995320000 │   0.012552661 │
│ 2020-10-28 │ Bitcoin     │ 35867320000 │ 13271.29 │ 245899820000 │   -0.02804481 │
│ 2020-10-27 │ Bitcoin     │ 33749879000 │ 13654.22 │ 252985950000 │    0.04427984 │
│ 2020-10-26 │ Bitcoin     │ 29461459000 │ 13075.25 │ 242251000000 │  0.0033826586 │
│ 2020-10-25 │ Bitcoin     │ 24406921000 │ 13031.17 │ 241425220000 │ -0.0058658565 │
│ 2020-10-24 │ Bitcoin     │ 24542319000 │ 13108.06 │ 242839880000 │   0.013650347 │
└────────────┴─────────────┴─────────────┴──────────┴──────────────┴───────────────┘
```

Now suppose we have a table named `holdings` that contains a list of cryptocurrencies that we own, along with the number of coins:

```sql title="Query"
CREATE TABLE holdings
(
    crypto_name String,
    quantity UInt64
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name);

INSERT INTO holdings VALUES
   ('Bitcoin', 1000),
   ('Bitcoin', 200),
   ('Ethereum', 250),
   ('Ethereum', 5000),
   ('DOGEFI', 10),
   ('Bitcoin Diamond', 5000);
```

We can use `EXCEPT` to answer a question like **"Which coins do we own have never traded below $10?"**:

```sql title="Query"
SELECT crypto_name FROM holdings
EXCEPT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Bitcoin     │
└─────────────┘
```

This means of the four cryptocurrencies we own, only Bitcoin has never dropped below $10 (based on the limited data we have here in this example).

### Using `EXCEPT DISTINCT` {#using-except-distinct}

Notice in the previous query we had multiple Bitcoin holdings in the result. You can add `DISTINCT` to `EXCEPT` to eliminate duplicate rows from the result:

```sql title="Query"
SELECT crypto_name FROM holdings
EXCEPT DISTINCT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
└─────────────┘
```

**See Also**

- [UNION](/reference/statements/select/union)
- [INTERSECT](/reference/statements/select/intersect)
)DOCS_MD",
        .syntax = R"(
SELECT column1 [, column2] FROM table1 [WHERE condition]
EXCEPT [ALL | DISTINCT]
SELECT column1 [, column2] FROM table2 [WHERE condition]
)",
        .parent = "SELECT",
        .related = {"SELECT", "UNION", "INTERSECT", "EXCEPT modifier"},
    });
}

}
