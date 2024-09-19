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
#include <Common/escapeForFileName.h>

#include <boost/range/adaptor/map.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
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

    ASTs rest_args(args.begin() + 2, args.end());
    if (!rest_args.empty())
    {
        auto params = getParamsMapFromAST(rest_args, context);
        auto param = params.extract("with_marks");

        if (!param.empty())
        {
            auto & value = param.mapped();
            if (value.getType() != Field::Types::Bool && value.getType() != Field::Types::UInt64)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table function '{}' expected bool flag for 'with_marks' argument", getName());

            if (value.getType() == Field::Types::Bool)
                with_marks = value.safeGet<bool>();
            else
                with_marks = value.safeGet<UInt64>();
        }

        if (!params.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unexpected arguments '{}' for table function '{}'",
                fmt::join(params | boost::adaptors::map_keys, ","), getName());
        }
    }

    source_table_id = StorageID{database, table};
}

static NameSet getAllPossibleStreamNames(
    const NameAndTypePair & column,
    const MergeTreeDataPartsVector & data_parts)
{
    NameSet all_streams;

    /// Add the stream with the name of column
    /// because it may be abcent in serialization streams (e.g. for Tuple type)
    /// but in compact parts we write only marks for whole columns, not subsubcolumns.
    auto main_stream_name = escapeForFileName(column.name);
    all_streams.insert(Nested::concatenateName(main_stream_name, "mark"));

    auto callback = [&](const auto & substream_path)
    {
        auto stream_name = ISerialization::getFileNameForStream(column, substream_path);
        all_streams.insert(Nested::concatenateName(stream_name, "mark"));
    };

    auto serialization = IDataType::getSerialization(column);
    serialization->enumerateStreams(callback);

    if (!column.type->supportsSparseSerialization())
        return all_streams;

    /// If there is at least one part with sparse serialization
    /// add columns with marks of its substreams to the table.
    for (const auto & part : data_parts)
    {
        serialization = part->tryGetSerialization(column.name);
        if (serialization && serialization->getKind() == ISerialization::Kind::SPARSE)
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
        auto columns_list = Nested::convertToSubcolumns(metadata_snapshot->getColumns().getAllPhysical());

        for (const auto & column : columns_list)
        {
            auto all_streams = getAllPossibleStreamNames(column, data_parts);
            for (const auto & stream_name : all_streams)
            {
                /// There may be shared substreams of columns (e.g. for Nested type)
                if (!columns.has(stream_name))
                    columns.add({stream_name, mark_type});
            }
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
    {
        .documentation =
        {
            .description = "Represents the contents of index and marks files of MergeTree tables. It can be used for introspection",
            .examples = {{"mergeTreeIndex", "SELECT * FROM mergeTreeIndex(currentDatabase(), mt_table, with_marks = true)", ""}},
            .categories = {"Other"},
        },
        .allow_readonly = true,
    });
}

}
