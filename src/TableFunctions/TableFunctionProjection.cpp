#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/StorageFromMergeTreeProjection.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

class TableFunctionMergeTreeProjection : public ITableFunction
{
public:
    static constexpr auto name = "mergeTreeProjection";
    std::string getName() const override { return name; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override
    {
        /// Technically it's MergeTreeProjection but it doesn't register itself
        return "";
    }

    StorageID source_table_id{StorageID::createEmpty()};
    String projection_name;
};

void TableFunctionMergeTreeProjection::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function ({}) must have arguments", quoteString(getName()));

    ASTs & args = args_func.at(0)->children;
    if (args.size() != 3)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have 3 arguments, got: {}", getName(), args.size());

    args[0] = evaluateConstantExpressionForDatabaseName(args[0], context);
    args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
    args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);

    auto database = checkAndGetLiteralArgument<String>(args[0], "database");
    auto table = checkAndGetLiteralArgument<String>(args[1], "table");
    source_table_id = StorageID{database, table};
    projection_name = checkAndGetLiteralArgument<String>(args[2], "projection");
}

ColumnsDescription TableFunctionMergeTreeProjection::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    auto source_table = DatabaseCatalog::instance().getTable(source_table_id, context);
    auto metadata_snapshot = source_table->getInMemoryMetadataPtr(context, false);

    if (!metadata_snapshot->getProjections().has(projection_name))
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "There is no projection {} in table {}",
            projection_name,
            source_table_id.getFullTableName());

    return metadata_snapshot->getProjections().get(projection_name).metadata->columns;
}

StoragePtr TableFunctionMergeTreeProjection::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool /* is_insert_query */) const
{
    auto source_table = DatabaseCatalog::instance().getTable(source_table_id, context);
    auto metadata_snapshot = source_table->getInMemoryMetadataPtr(context, false);
    ProjectionDescriptionRawPtr projection = &metadata_snapshot->getProjections().get(projection_name);

    StorageID storage_id(getDatabaseName(), table_name);
    auto res = std::make_shared<StorageFromMergeTreeProjection>(
        std::move(storage_id), std::move(source_table), metadata_snapshot, projection);

    res->startup();
    return res;
}

void registerTableFunctionMergeTreeProjection(TableFunctionFactory & factory);
void registerTableFunctionMergeTreeProjection(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMergeTreeProjection>(
        {.description = R"DOCS_MD(
Represents the contents of some projection in MergeTree tables. It can be used for introspection.

## Syntax {#syntax}

```sql
mergeTreeProjection(database, table, projection)
```

## Arguments {#arguments}

| Argument     | Description                                |
|--------------|--------------------------------------------|
| `database`   | The database name to read projection from. |
| `table`      | The table name to read projection from.    |
| `projection` | The projection to read from.               |

## Returned value {#returned_value}

A table object with columns provided by given projection.

## Usage Example {#usage-example}

```sql
CREATE TABLE test
(
    `user_id` UInt64,
    `item_id` UInt64,
    PROJECTION order_by_item_id
    (
        SELECT _part_offset
        ORDER BY item_id
    )
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO test SELECT number, 100 - number FROM numbers(5);
```

```sql
SELECT *, _part_offset FROM mergeTreeProjection(currentDatabase(), test, order_by_item_id);
```

```text
   ┌─item_id─┬─_parent_part_offset─┬─_part_offset─┐
1. │      96 │                   4 │            0 │
2. │      97 │                   3 │            1 │
3. │      98 │                   2 │            2 │
4. │      99 │                   1 │            3 │
5. │     100 │                   0 │            4 │
   └─────────┴─────────────────────┴──────────────┘
```

```sql
DESCRIBE mergeTreeProjection(currentDatabase(), test, order_by_item_id) SETTINGS describe_compact_output = 1;
```

```text
   ┌─name────────────────┬─type───┐
1. │ item_id             │ UInt64 │
2. │ _parent_part_offset │ UInt64 │
   └─────────────────────┴────────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = true}
    );
}

}
