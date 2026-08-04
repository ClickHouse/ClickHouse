#include <Storages/StorageMergeTreeCodecBlockCounts.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

class TableFunctionMergeTreeCodecBlockCounts : public ITableFunction
{
public:
    static constexpr auto name = "mergeTreeCodecBlockCounts";
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

    const char * getStorageEngineName() const override { return ""; }

    StorageID source_table_id{StorageID::createEmpty()};
};

void TableFunctionMergeTreeCodecBlockCounts::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function ({}) must have arguments.", quoteString(getName()));

    ASTs & args = args_func.at(0)->children;
    if (args.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' must have 2 arguments (database, table), got: {}",
            getName(),
            args.size());

    args[0] = evaluateConstantExpressionForDatabaseName(args[0], context);
    args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);

    auto database = checkAndGetLiteralArgument<String>(args[0], "database");
    auto table = checkAndGetLiteralArgument<String>(args[1], "table");

    source_table_id = StorageID{database, table};
}

ColumnsDescription TableFunctionMergeTreeCodecBlockCounts::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    auto source_table = DatabaseCatalog::instance().getTable(source_table_id, context);

    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Table function {} expected MergeTree table, got: {}", getName(), source_table->getName());

    auto codec_map = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>());

    ColumnsDescription columns;
    columns.add({"part_name", std::make_shared<DataTypeString>(), "Active data part the column belongs to."});
    columns.add({"column", std::make_shared<DataTypeString>(), "Column name."});
    columns.add(
        {"substream",
         std::make_shared<DataTypeString>(),
         "Physical stream of the column the counts are for. Matches `system.parts_columns.substreams`."});
    auto nullable_uint64 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    columns.add(
        {"data_compressed_bytes", nullable_uint64, "Size of compressed data in the substream, in bytes. NULL for `Compact` parts."});
    columns.add(
        {"data_uncompressed_bytes", nullable_uint64, "Size of uncompressed data in the substream, in bytes. NULL for `Compact` parts."});
    columns.add(
        {"codec_block_counts",
         codec_map,
         "The number of compressed blocks of this substream grouped by codec. Empty for `Compact` parts."});
    return columns;
}

StoragePtr TableFunctionMergeTreeCodecBlockCounts::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    auto source_table = DatabaseCatalog::instance().getTable(source_table_id, context);
    auto columns = getActualTableStructure(context, is_insert_query);

    StorageID storage_id(getDatabaseName(), table_name);
    auto res = std::make_shared<StorageMergeTreeCodecBlockCounts>(std::move(storage_id), std::move(source_table), std::move(columns));

    res->startup();
    return res;
}

void registerTableFunctionMergeTreeCodecBlockCounts(TableFunctionFactory & factory);
void registerTableFunctionMergeTreeCodecBlockCounts(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMergeTreeCodecBlockCounts>(
        {.description = R"DOCS_MD(
Reports, per (part, column, substream) of a MergeTree table, how many compressed blocks use each codec. This is how you observe adaptive codec selection (enabled by `allow_experimental_adaptive_codec_selection` setting), which can pick a codec per block for default-codec columns.

Selecting `codec_block_counts` reads `.bin` data files, not just metadata. The other columns are metadata-only.

Parts that do not record their substreams in `columns_substreams.txt` are not listed.

If a row policy applies to the table for the current user, reading `codec_block_counts` throws `ACCESS_DENIED`, because the counts would cover rows the policy hides. The other columns stay readable, `system.parts_columns` reports them regardless of row policies.

## Syntax {#syntax}

```sql
mergeTreeCodecBlockCounts(database, table)
```

## Arguments {#arguments}

| Argument   | Description                          |
|------------|--------------------------------------|
| `database` | The database name of the table.      |
| `table`    | The MergeTree table name.            |

## Returned value {#returned-value}

A table object with one row per (active part, column, substream) of the source table:

- `part_name` ([String](/reference/data-types/string)) — The active data part the column belongs to.
- `column` ([String](/reference/data-types/string)) — The column name.
- `substream` ([String](/reference/data-types/string)) — The physical stream of the column the counts are for. Matches `system.parts_columns.substreams`.
- `data_compressed_bytes` ([Nullable(UInt64)](/reference/data-types/nullable)) — Size of compressed data in the substream, in bytes. `NULL` for `Compact` parts.
- `data_uncompressed_bytes` ([Nullable(UInt64)](/reference/data-types/nullable)) — Size of uncompressed data in the substream, in bytes. `NULL` for `Compact` parts.
- `codec_block_counts` ([Map(String, UInt64)](/reference/data-types/map)) — The number of compressed blocks of this substream grouped by codec. Empty for `Compact` parts, whose columns share one data file and so have no per-stream codec attribution.

## Usage example {#usage-example}

```sql
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO mt SELECT number FROM numbers(100000);

SELECT column, substream, codec_block_counts
FROM mergeTreeCodecBlockCounts(currentDatabase(), mt);
```

```text
┌─column─┬─substream─┬─codec_block_counts─┐
│ a      │ a         │ {'LZ4':13}         │
└────────┴───────────┴────────────────────┘
```

Column-level totals are a `GROUP BY column` with [`sumMap`](/reference/functions/aggregate-functions/sumMap):

```sql
SELECT column, sumMap(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), mt)
GROUP BY column;
```

A `LowCardinality` column reports its dictionary and indexes streams separately:

```sql
CREATE TABLE mt_lc (s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO mt_lc SELECT toString(number % 1000) FROM numbers(1000000);

SELECT substream, codec_block_counts
FROM mergeTreeCodecBlockCounts(currentDatabase(), mt_lc)
WHERE column = 's'
ORDER BY substream;
```

```text
┌─substream─┬─codec_block_counts─┐
│ s         │ {'LZ4':31}         │
│ s.dict    │ {'LZ4':1}          │
└───────────┴────────────────────┘
```
)DOCS_MD",
         .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = true});
}

}
