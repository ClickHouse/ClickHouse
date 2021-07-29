#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Core/Types.h>

namespace DB
{

/* null(structure) - creates a temporary null storage
 *
 * Used for testing purposes, for convenience writing tests and demos.
 */
class TableFunctionNull : public ITableFunction
{
public:
    static constexpr auto name = "null";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Null"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    String structure;
};

namespace NullDoc
{
const char * doc = R"(
Creates a temporary table of the specified structure with the [Null](../../engines/table-engines/special/null.md) table engine. According to the `Null`-engine properties, the table data is ignored and the table itself is immediately dropped right after the query execution. The function is used for the convenience of test writing and demonstrations.

**Syntax** 

``` sql
null('structure')
```

**Parameter** 

-   `structure` — A list of columns and column types. [String](../../sql-reference/data-types/string.md).

**Returned value**

A temporary `Null`-engine table with the specified structure.

**Example**

Query with the `null` function:

``` sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```
can replace three queries:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

See also: 

-   [Null table engine](../../engines/table-engines/special/null.md)

[Original article](https://clickhouse.tech/docs/en/sql-reference/table-functions/null/) <!--hide-->

)";

}

}
