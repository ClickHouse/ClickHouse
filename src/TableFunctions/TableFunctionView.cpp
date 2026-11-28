#include <Core/Settings.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/StorageView.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionView.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{
namespace Setting
{
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


const ASTSelectWithUnionQuery & TableFunctionView::getSelectQuery() const
{
    return *create.select;
}

VectorWithMemoryTracking<size_t> TableFunctionView::skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const
{
    return {0};
}

void TableFunctionView::parseArguments(const ASTPtr & ast_function, ContextPtr /*context*/)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (function)
    {
        if (auto * select = function->tryGetQueryArgument())
        {
            create.set(create.select, select->clone());
            return;
        }
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires a query argument.", getName());
}

ColumnsDescription TableFunctionView::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    chassert(create.select);
    chassert(create.children.size() == 1);
    chassert(create.children[0]->as<ASTSelectWithUnionQuery>());

    SharedHeader sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(create.children[0], context);

    return ColumnsDescription(sample_block->getNamesAndTypesList());
}

StoragePtr TableFunctionView::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    /// A table created with `CREATE TABLE ... AS view(...)` passes its columns, and then the storage is created on the first
    /// access, with the global context (see `ITableFunction::execute`). Analyzing the query here would do it with the global
    /// access rights, which evaluates constant expressions, such as `dictGet`, that the user may have no access to, and
    /// discloses their values, for example in an error. `StorageView::read` analyzes the query with the context of each query
    /// that reads the table, and converts its result to these columns.
    const bool has_cached_columns = !cached_columns.empty();
    auto columns = has_cached_columns ? std::move(cached_columns) : getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageView>(StorageID(getDatabaseName(), table_name), create, columns, "");
    /// The query result is converted to these columns, as `StorageTableFunctionProxy` did for the columns of the query.
    if (has_cached_columns)
    {
        res->setConvertsQueryResultToColumns();
    }
    res->startup();
    return res;
}

void registerTableFunctionView(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionView>({.description = R"DOCS_MD(
Turns a subquery into a table. The function implements views (see [CREATE VIEW](/reference/statements/create/view)). The resulting table does not store data, but only stores the specified `SELECT` query. When reading from the table, ClickHouse executes the query and deletes all unnecessary columns from the result.

## Syntax {#syntax}

```sql
view(subquery)
```

## Arguments {#arguments}

- `subquery` — `SELECT` query.

## Returned value {#returned-value}

- A table.

## Examples {#examples}

Input table:

```text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

```sql title="Query"
SELECT * FROM view(SELECT name FROM months);
```

```text title="Response"
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

You can use the `view` function as a parameter of the [remote](/reference/functions/table-functions/remote) and [cluster](/reference/functions/table-functions/cluster) table functions:

```sql title="Query"
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

```sql title="Query"
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

## Related {#related}

- [View Table Engine](/reference/engines/table-engines/special/view)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, SecretArgumentsSpec{}, {.allow_readonly = true});
}

}
