#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/System/StorageSystemPrimes.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
}

namespace
{

/** primes(limit)
  * primes(offset, length)
  * primes(offset, length, step)
  *
  * offset/length/step are in prime-index space:
  *   offset: how many primes to skip (0-based)
  *   length: how many primes to return
  *   step: take every step-th prime (1 means every prime)
  *
  * In text, after skipping `offset` primes, take every `step`-th prime until `limit` primes are taken.
  */
class TableFunctionPrimes : public ITableFunction
{
public:
    static constexpr auto name = "primes";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return ""; }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
};

ColumnsDescription TableFunctionPrimes::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    return ColumnsDescription{{{"prime", std::make_shared<DataTypeUInt64>()}}};
}

UInt64 TableFunctionPrimes::evaluateArgument(ContextPtr context, ASTPtr & argument) const
{
    const auto & [field, type] = evaluateConstantExpression(argument, context);

    if (!isNativeNumber(type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} expression, must be numeric type", type->getName());

    Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The value {} is not representable as UInt64",
            applyVisitor(FieldVisitorToString(), field));

    return converted.safeGet<UInt64>();
}

StoragePtr TableFunctionPrimes::executeImpl(
    const ASTPtr & ast_function,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool /*is_insert_query*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments ? function->arguments->children : ASTs{};

        if (arguments.size() >= 4)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' cannot have more than three params", getName());

        if (!arguments.empty())
        {
            UInt64 offset = arguments.size() >= 2 ? evaluateArgument(context, arguments[0]) : 0;
            UInt64 length = arguments.size() >= 2 ? evaluateArgument(context, arguments[1]) : evaluateArgument(context, arguments[0]);
            UInt64 step = arguments.size() == 3 ? evaluateArgument(context, arguments[2]) : 1;

            if (!step)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function {} requires step to be a positive number", getName());

            auto res = std::make_shared<StorageSystemPrimes>(
                StorageID(getDatabaseName(), table_name), std::string{"prime"}, length, offset, step);

            res->startup();
            return res;
        }

        auto res = std::make_shared<StorageSystemPrimes>(StorageID(getDatabaseName(), table_name), std::string{"prime"});

        res->startup();
        return res;
    }

    throw Exception(
        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Table function '{}' requires 'limit' or 'offset, length', or 'offset, length, step'.",
        getName());
}

}

void registerTableFunctionPrimes(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPrimes>(
        {.description = R"DOCS_MD(
- `primes()` – Returns an infinite table with a single `prime` column (UInt64) that contains prime numbers in ascending order, starting from 2. Use `LIMIT` (and optionally `OFFSET`) to restrict the number of rows.

- `primes(N)` – Returns a table with the single `prime` column (UInt64) that contains the first `N` prime numbers, starting from 2.

- `primes(N, M)` – Returns a table with the single `prime` column (UInt64) that contains `M` prime numbers starting at the `N`-th prime (0-based).

- `primes(N, M, S)` – Returns a table with the single `prime` column (UInt64) that contains `M` prime numbers starting from the `N`-th prime (0-based) with step `S` by prime index. The returned primes correspond to indices `N, N + S, N + 2S, ..., N + (M - 1)S`. `S` must be `>= 1`.

This is similar to the [`system.primes`](/reference/system-tables/primes) system table.

The following queries are equivalent:

```sql
SELECT * FROM primes(10);
SELECT * FROM primes(0, 10);
SELECT * FROM primes() LIMIT 10;
SELECT * FROM system.primes LIMIT 10;
SELECT * FROM system.primes WHERE prime IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
```

The following queries are also equivalent:

```sql
SELECT * FROM primes(10, 10);
SELECT * FROM primes() LIMIT 10 OFFSET 10;
SELECT * FROM system.primes LIMIT 10 OFFSET 10;
```

### Examples {#examples}

The first 10 primes.
```sql
SELECT * FROM primes(10);
```

```response
  ┌─prime─┐
  │     2 │
  │     3 │
  │     5 │
  │     7 │
  │    11 │
  │    13 │
  │    17 │
  │    19 │
  │    23 │
  │    29 │
  └───────┘
```

The first prime greater than 1e15.
```sql
SELECT prime FROM primes() WHERE prime > 1e15 LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```

Solve a modular constraint over primes in a very large range: find the first prime `p >= 10^15` such that `p` modulo `65537` equals `1`.
```sql
SELECT prime
FROM primes()
WHERE prime >= 1e15
  AND prime % 65537 = 1
LIMIT 1;
```

```response
 ┌────────────prime─┐
 │ 1000000001218399 │ -- 1.00 quadrillion
 └──────────────────┘
```

The first 7 Mersenne primes.
```sql
SELECT prime
FROM primes()
WHERE bitAnd(prime, prime + 1) = 0
LIMIT 7;
```

```response
  ┌──prime─┐
  │      3 │
  │      7 │
  │     31 │
  │    127 │
  │   8191 │
  │ 131071 │
  │ 524287 │
  └────────┘
```

### Notes {#notes}
- The fastest forms are the plain range and point-filter queries that use the default step (`1`), for example, `primes(N)` or `primes() LIMIT N`. These forms use an optimized prime generator to compute very large primes efficiently.
- For unbounded sources (`primes()` / `system.primes`), simple value filters such as `prime BETWEEN ...`, `prime IN (...)`, or `prime = ...` can be applied during generation to restrict the searched value ranges. For example, the following query executes almost instantly:
```sql
SELECT sum(prime)
FROM primes()
WHERE prime BETWEEN 1e6 AND 1e6 + 100
   OR prime BETWEEN 1e12 AND 1e12 + 100
   OR prime BETWEEN 1e15 AND 1e15 + 100
   OR prime IN (9999999967, 9999999971, 9999999973)
   OR prime = 1000000000000037;
```

```response
  ┌───────sum(prime)─┐
  │ 2004010006000641 │ -- 2.00 quadrillion
  └──────────────────┘

1 row in set. Elapsed: 0.090 sec. 
```
- This value-range optimization does not apply to bounded table functions (`primes(N)`, `primes(offset, count[, step])`) with `WHERE`, because those variants define a finite table by prime index and the filter must be evaluated after generating that table to preserve semantics.
- Using a non-zero offset and/or step greater than 1 (`primes(offset, count)` / `primes(offset, count, step)`) may be slower because additional primes may need to be generated and skipped internally. If you don't need an offset or step, omit them.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = true}
    );
}

}
