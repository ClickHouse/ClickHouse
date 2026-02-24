#include <Storages/StorageMergeTreeTextIndex.h>
#include <TableFunctions/ITableFunction.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

class TableFunctionMergeTreeTextIndex : public ITableFunction
{
public:
    static constexpr auto name = "mergeTreeTextIndex";
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
        return "";
    }

    String source_database;
    String source_table;
    String source_index_name;
};

void TableFunctionMergeTreeTextIndex::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function ({}) must have arguments", quoteString(getName()));

    ASTs & args = args_func.at(0)->children;
    if (args.size() != 3)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' must have 3 arguments (database, table, index_name), got: {}", getName(), args.size());

    auto database_arg = evaluateConstantExpressionForDatabaseName(args[0], context);
    auto table_arg = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
    auto index_name_arg = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);

    source_database = checkAndGetLiteralArgument<String>(database_arg, "database");
    source_table = checkAndGetLiteralArgument<String>(table_arg, "table");
    source_index_name = checkAndGetLiteralArgument<String>(index_name_arg, "index_name");
}

static std::shared_ptr<DataTypeEnum8> getDictionaryCompressionType()
{
    DataTypeEnum8::Values values;
    values.emplace_back("raw", static_cast<Int8>(0));
    values.emplace_back("front_coded", static_cast<Int8>(1));
    return std::make_shared<DataTypeEnum8>(std::move(values));
}

ColumnsDescription TableFunctionMergeTreeTextIndex::getActualTableStructure(ContextPtr, bool /*is_insert_query*/) const
{
    return ColumnsDescription{{
        {"part_name", std::make_shared<DataTypeString>()},
        {"token", std::make_shared<DataTypeString>()},
        {"dictionary_compression", getDictionaryCompressionType()},
        {"cardinality", std::make_shared<DataTypeUInt64>()},
        {"num_posting_blocks", std::make_shared<DataTypeUInt64>()},
        {"has_embedded_postings", std::make_shared<DataTypeUInt8>()},
        {"has_raw_postings", std::make_shared<DataTypeUInt8>()},
        {"has_compressed_postings", std::make_shared<DataTypeUInt8>()}
    }};
}

StoragePtr TableFunctionMergeTreeTextIndex::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    auto source_table_ptr = DatabaseCatalog::instance().getTable(StorageID{source_database, source_table}, context);
    auto metadata_snapshot = source_table_ptr->getInMemoryMetadataPtr();
    const auto & index_desc = metadata_snapshot->getSecondaryIndices().getByName(source_index_name);

    if (index_desc.type != "text")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Got index '{}' of type '{}', expected 'text'",
            source_index_name, index_desc.type);

    auto text_index = MergeTreeIndexFactory::instance().get(index_desc);
    auto columns = getActualTableStructure(context, is_insert_query);
    StorageID storage_id(getDatabaseName(), table_name);

    auto res = std::make_shared<StorageMergeTreeTextIndex>(
        std::move(storage_id),
        std::move(source_table_ptr),
        std::move(text_index),
        std::move(columns));

    res->startup();
    return res;
}

void registerTableFunctionMergeTreeTextIndex(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMergeTreeTextIndex>(
    {
        .documentation =
        {
            .description = "Reading the dictionary of a text index from a MergeTree table. Returns tokens with their posting list metadata.",
            .examples = {{"mergeTreeTextIndex", "SELECT * FROM mergeTreeTextIndex(currentDatabase(), my_table, my_text_index)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = true,
    });
}

}
