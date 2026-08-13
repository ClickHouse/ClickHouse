#include <TableFunctions/TableFunctionFuzzQuery.h>

#include <DataTypes/DataTypeString.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionFuzzQuery::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments", getName());

    auto args = args_func.at(0)->children;
    configuration = StorageFuzzQuery::getConfiguration(args, context);
}

StoragePtr TableFunctionFuzzQuery::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    ColumnsDescription columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageFuzzQuery>(
        StorageID(getDatabaseName(), table_name),
        columns,
        /* comment */ String{},
        configuration);
    res->startup();
    return res;
}

void registerTableFunctionFuzzQuery(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFuzzQuery>(
         {.description = R"DOCS_MD(
Perturbs the given query string with random variations.

## Syntax {#syntax}

```sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

## Arguments {#arguments}

| Argument           | Description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| `query`            | (String) - The source query to perform the fuzzing on.                      |
| `max_query_length` | (UInt64) - A maximum length the query can get during the fuzzing process. |
| `random_seed`      | (UInt64) - A random seed for producing stable results.                      |

## Returned value {#returned_value}

A table object with a single column containing perturbed query strings.

## Usage Example {#usage-example}

```sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```response
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = true});
}

}
