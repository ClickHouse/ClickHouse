#include <optional>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>
#include <base/types.h>
#include <TableFunctions/registerTableFunctions.h>


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

/* numbers(limit), numbers_mt(limit)
 * - the same as SELECT number FROM system.numbers LIMIT limit.
 * Used for testing purposes, as a simple example of table function.
 */
template <bool multithreaded>
class TableFunctionNumbers : public ITableFunction
{
public:
    static constexpr auto name = multithreaded ? "numbers_mt" : "numbers";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;
    const char * getStorageEngineName() const override
    {
        /// Technically it's SystemNumbers but it doesn't register itself
        return "";
    }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
};

template <bool multithreaded>
ColumnsDescription TableFunctionNumbers<multithreaded>::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    /// NOTE: https://bugs.llvm.org/show_bug.cgi?id=47418
    return ColumnsDescription{{{"number", std::make_shared<DataTypeUInt64>()}}};
}

template <bool multithreaded>
StoragePtr TableFunctionNumbers<multithreaded>::executeImpl(
    const ASTPtr & ast_function,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool /*is_insert_query*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

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

            auto res = std::make_shared<StorageSystemNumbers>(
                StorageID(getDatabaseName(), table_name), multithreaded, std::string{"number"}, UInt128(length), offset, step);
            res->startup();
            return res;
        }

        auto res = std::make_shared<StorageSystemNumbers>(StorageID(getDatabaseName(), table_name), multithreaded, std::string{"number"});
        res->startup();
        return res;
    }
    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'limit' or 'offset, limit'.", getName());
}

template <bool multithreaded>
UInt64 TableFunctionNumbers<multithreaded>::evaluateArgument(ContextPtr context, ASTPtr & argument) const
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

}

void registerTableFunctionNumbers(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNumbers<true>>(
        {
            .description = R"(The same as `numbers`, but generates the values using multiple threads (according to the `max_threads` setting), so the order of the rows is not deterministic. Returns a table with a single `number` column of type `UInt64`. When called with no arguments, it produces an unbounded stream of integers starting from 0.)",
            .syntax = "numbers_mt() | numbers_mt(N) | numbers_mt(N, M) | numbers_mt(N, M, S)",
            .arguments = {
                {"N", "When used as `numbers_mt(N)`: the number of integers to return (`0` to `N - 1`). When used as `numbers_mt(N, M[, S])`: the starting value (offset).", {"UInt64"}},
                {"M", "The width of the value range `[N, N + M)`. Values are emitted from this range starting at `N` and stepping by `S`, so `numbers_mt(N, M)` returns `N` to `N + M - 1` and `numbers_mt(N, M, S)` returns fewer values when `S > 1` (only for `numbers_mt(N, M)` and `numbers_mt(N, M, S)`).", {"UInt64"}},
                {"S", "The step between successive values (`S >= 1`), only for `numbers_mt(N, M, S)`.", {"UInt64"}},
            },
            .returned_value = {"A table with a single `number` column of type `UInt64`.", {"UInt64"}},
            .examples = {
                {"The integers from 0 to 9, in an unspecified order", "SELECT * FROM numbers_mt(10) ORDER BY number;", ""},
                {"Count rows using multiple threads", "SELECT count() FROM numbers_mt(1000000000);", ""},
                {"Limit an infinite stream", "SELECT * FROM numbers_mt() LIMIT 10;", ""},
            },
            .introduced_in = {1, 1},
            .category = FunctionDocumentation::Category::TableFunction,
        },
        {.allow_readonly = true});

    factory.registerFunction<TableFunctionNumbers<false>>(
        {.description = R"DOCS_MD(
- `numbers()` – Returns an infinite table with a single `number` column (UInt64) that contains integers in ascending order, starting from 0. Use `LIMIT` (and optionally `OFFSET`) to restrict the number of rows.

- `numbers(N)` – Returns a table with a single `number` column (UInt64) that contains integers from 0 to `N - 1`.

- `numbers(N, M)` – Returns a table with a single `number` column (UInt64) that contains `M` integers from `N` to `N + M - 1`.

- `numbers(N, M, S)` – Returns a table with a single `number` column (UInt64) that contains values in `[N, N + M)` with step `S` (about `M / S` rows, rounded up). `S` must be `>= 1`.

This is similar to the [`system.numbers`](/reference/system-tables/numbers) system table. It can be used for testing and generating successive values.

The following queries are equivalent:

```sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM numbers() LIMIT 10;
SELECT * FROM system.numbers LIMIT 10;
SELECT * FROM system.numbers WHERE number BETWEEN 0 AND 9;
SELECT * FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
```

The following queries are also equivalent:

```sql
SELECT * FROM numbers(10, 10);
SELECT * FROM numbers() LIMIT 10 OFFSET 10;
SELECT * FROM system.numbers LIMIT 10 OFFSET 10;
```

The following queries are also equivalent:

```sql
SELECT number * 2 FROM numbers(10);
SELECT (number - 10) * 2 FROM numbers(10, 10);
SELECT * FROM numbers(0, 20, 2);
```

### Examples {#examples}

The first 10 numbers.
```sql
SELECT * FROM numbers(10);
```

```response
 ┌─number─┐
 │      0 │
 │      1 │
 │      2 │
 │      3 │
 │      4 │
 │      5 │
 │      6 │
 │      7 │
 │      8 │
 │      9 │
 └────────┘
```

Generate a sequence of dates from 2010-01-01 to 2010-12-31.

```sql
SELECT toDate('2010-01-01') + number AS d FROM numbers(365);
```

Find the first `UInt64` `>= 10^15` whose `sipHash64(number)` has 20 trailing zero bits.

```sql
SELECT number
FROM numbers()
WHERE number >= 1e15
  AND bitAnd(sipHash64(number), 0xFFFFF) = 0
LIMIT 1;
```

```response
 ┌───────────number─┐
 │ 1000000000056095 │ -- 1.00 quadrillion
 └──────────────────┘
```

### Notes {#notes}
- For performance reasons, if you know how many rows you need, prefer bounded forms (`numbers(N)`, `numbers(N, M[, S])`) over unbounded `numbers()` / `system.numbers`.
- For parallel generation, use `numbers_mt(...)` or the [`system.numbers_mt`](/reference/system-tables/numbers_mt) table. Note that results may be returned in any order.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = true});
}

}
