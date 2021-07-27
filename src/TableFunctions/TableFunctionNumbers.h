#pragma once

#include <TableFunctions/ITableFunction.h>
#include <common/types.h>


namespace DB
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
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "SystemNumbers"; }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
};

namespace NumbersDoc
{
const char * doc = R"(
`numbers(N)` – Returns a table with the single ‘number’ column (UInt64) that contains integers from 0 to N-1.
`numbers(N, M)` - Returns a table with the single ‘number’ column (UInt64) that contains integers from N to (N + M - 1).

Similar to the `system.numbers` table, it can be used for testing and generating successive values, `numbers(N, M)` more efficient than `system.numbers`.

The following queries are equivalent:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
```

Examples:

``` sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```
)";

}

}
