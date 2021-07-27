#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <common/types.h>

namespace DB
{

/* view(query)
 * Turning subquery into a table.
 * Useful for passing subquery around.
 */
class TableFunctionView : public ITableFunction
{
public:
    static constexpr auto name = "view";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "View"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    ASTCreateQuery create;
};

namespace ViewDoc
{
const char * doc = R"(
Turns a subquery into a table. The function implements views (see [CREATE VIEW](https://clickhouse.tech/docs/en/sql-reference/statements/create/view/#create-view)). The resulting table does not store data, but only stores the specified `SELECT` query. When reading from the table, ClickHouse executes the query and deletes all unnecessary columns from the result.

**Syntax**

``` sql
view(subquery)
```

**Arguments**

-   `subquery` — `SELECT` query.

**Returned value**

-   A table.

**Example**

Input table:

``` text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

Query:

``` sql
SELECT * FROM view(SELECT name FROM months);
```

Result:

``` text
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

You can use the `view` function as a parameter of the [remote](https://clickhouse.tech/docs/en/sql-reference/table-functions/remote/#remote-remotesecure) and [cluster](https://clickhouse.tech/docs/en/sql-reference/table-functions/cluster/#cluster-clusterallreplicas) table functions:

``` sql
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

``` sql
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

**See Also**

-   [View Table Engine](https://clickhouse.tech/docs/en/engines/table-engines/special/view/)

[Original article](https://clickhouse.tech/docs/en/sql-reference/table-functions/view/) <!--hide-->

)";

}


}
