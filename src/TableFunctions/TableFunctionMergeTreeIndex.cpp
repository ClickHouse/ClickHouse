#include <Storages/StorageMergeTreeIndex.h>
#include <TableFunctions/ITableFunction.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/NamedCollectionsHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

class TableFunctionMergeTreeIndex : public ITableFunction
{
public:
    static constexpr auto name = "mergeTreeIndex";
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

    const char * getStorageTypeName() const override { return "MergeTreeIndex"; }

    StorageID source_table_id{StorageID::createEmpty()};
    bool with_marks = false;
};

void TableFunctionMergeTreeIndex::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function ({}) must have arguments.", quoteString(getName()));

    ASTs & args = args_func.at(0)->children;
    if (args.size() < 2 || args.size() > 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' must have 2 or 3 arguments, got: {}", getName(), args.size());

    args[0] = evaluateConstantExpressionForDatabaseName(args[0], context);
    args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);

    auto database = checkAndGetLiteralArgument<String>(args[0], "database");
    auto table = checkAndGetLiteralArgument<String>(args[1], "table");

    if (args.size() == 3)
    {
        auto [key, value] = getKeyValueFromAST(args[2], context);
        if (key != "with_marks" || (value.getType() != Field::Types::Bool && value.getType() != Field::Types::UInt64))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function '{}' expects 'with_marks' flag as third argument", getName());

        if (value.getType() == Field::Types::Bool)
            with_marks = value.get<bool>();
        else
            with_marks = value.get<UInt64>();
    }

    source_table_id = StorageID{database, table};
}

static std::unordered_set<String> getAllPossibleStreamNames(
    const MergeTreeDataPartsVector & data_parts, const NameAndTypePair & column)
{
    std::unordered_set<String> all_streams;

    auto callback = [&](const auto & substream_path)
    {
        auto subcolumn_name = ISerialization::getSubcolumnNameForStream(substream_path);
        auto full_name = Nested::concatenateName(column.name, subcolumn_name);
        all_streams.insert(Nested::concatenateName(full_name, "mark"));
    };

    auto serialization = column.type->getDefaultSerialization();
    serialization->enumerateStreams(callback);

    if (!column.type->supportsSparseSerialization())
        return all_streams;

    for (const auto & part : data_parts)
    {
        serialization = part->getSerialization(column.name);
        if (serialization->getKind() == ISerialization::Kind::SPARSE)
        {
            serialization->enumerateStreams(callback);
            break;
        }
    }

    return all_streams;
}

ColumnsDescription TableFunctionMergeTreeIndex::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    auto source_table = DatabaseCatalog::instance().getTable(source_table_id, context);
    auto metadata_snapshot = source_table->getInMemoryMetadataPtr();

    ColumnsDescription columns;
    for (const auto & column : StorageMergeTreeIndex::virtuals_sample_block)
        columns.add({column.name, column.type});

    for (const auto & column : metadata_snapshot->getPrimaryKey().sample_block)
        columns.add({column.name, column.type});

    if (with_marks)
    {
        auto element_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
        auto mark_type = std::make_shared<DataTypeTuple>(
            DataTypes{element_type, element_type},
            Names{"offset_in_compressed_file", "offset_in_decompressed_block"});

        const auto * merge_tree = dynamic_cast<const MergeTreeData *>(source_table.get());
        if (!merge_tree)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function mergeTreeIndex expected MergeTree table, got: {}", source_table->getName());

        auto data_parts = merge_tree->getDataPartsVectorForInternalUsage();
        for (const auto & column : metadata_snapshot->getColumns().getAllPhysical())
        {
            auto all_streams = getAllPossibleStreamNames(data_parts, column);
            for (const auto & stream_name : all_streams)
                columns.add({stream_name, mark_type});
        }
    }

    return columns;
}

StoragePtr TableFunctionMergeTreeIndex::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    auto source_table = DatabaseCatalog::instance().getTable(source_table_id, context);
    auto columns = getActualTableStructure(context, is_insert_query);

    StorageID storage_id(getDatabaseName(), table_name);
    auto res = std::make_shared<StorageMergeTreeIndex>(std::move(storage_id), std::move(source_table), std::move(columns), with_marks);

    res->startup();
    return res;
}

void registerTableFunctionMergeTreeIndex(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMergeTreeIndex>(
        {.documentation
         = {.description = "KEK",
            .returned_value = "SHPEK"},
         .allow_readonly = true});
}

}
