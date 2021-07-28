#pragma once

#include <TableFunctions/ITableFunction.h>
#include <common/types.h>


namespace DB
{

/* zeros(limit), zeros_mt(limit)
 * - the same as SELECT zero FROM system.zeros LIMIT limit.
 * Used for testing purposes, as a simple example of table function.
 */
template <bool multithreaded>
class TableFunctionZeros : public ITableFunction
{
public:
    static constexpr auto name = multithreaded ? "zeros_mt" : "zeros";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "SystemZeros"; }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
};

namespace ZerosDoc
{
const char * doc = R"(
Works in the pretty same ways as `numbers`. Generated column is not constant, but already materialized. It is generated once and reused if possible.

Works about 3 times faster than `numbers`.

```sql
ELECT count() FROM system.zeros_mt

↓ Progress: 1.28 trillion rows, 1.28 TB (75.03 billion rows/s., 75.03 GB/s.) Cancelling query.
↙ Progress: 1.29 trillion rows, 1.29 TB (75.03 billion rows/s., 75.03 GB/s.) Ok.


SELECT count()
FROM system.numbers_mt

↙ Progress: 247.64 billion rows, 1.98 TB (24.07 billion rows/s., 192.53 GB/s.) Cancelling query.
← Progress: 250.05 billion rows, 2.00 TB (24.06 billion rows/s., 192.48 GB/s.) Ok.
```

```sql
SELECT sum(zero) FROM system.zeros_mt

→ Progress: 399.09 billion rows, 399.09 GB (36.70 billion rows/s., 36.70 GB/s.) Cancelling query.
↘ Progress: 402.78 billion rows, 402.78 GB (36.69 billion rows/s., 36.69 GB/s.) Ok.


SELECT sum(number) FROM system.numbers_mt

→ Progress: 267.31 billion rows, 2.14 TB (11.79 billion rows/s., 94.35 GB/s.) Cancelling query.
↘ Progress: 268.61 billion rows, 2.15 TB (11.80 billion rows/s., 94.39 GB/s.) Ok.
```
)";
}

namespace ZerosMTDoc
{
const char * doc = R"(
Same as zeros but *multithreaded*.
)";
}

}
