#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/StorageMergeTreeAnalyzeIndexes.h>
#include <TableFunctions/ITableFunction.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/quoteString.h>
#include <Storages/MergeTree/VectorSearchUtils.h>
#include <Parsers/ASTLiteral.h>

namespace
{

const char * mergeTreeAnalyzeIndexFunctionName(bool resolve_by_uuid)
{
    if (resolve_by_uuid)
        return "mergeTreeAnalyzeIndexesUUID";
    else
        return "mergeTreeAnalyzeIndexes";
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

class TableFunctionMergeTreeAnalyzeIndexes : public ITableFunction
{
public:
    explicit TableFunctionMergeTreeAnalyzeIndexes(bool resolve_by_uuid_)
        : resolve_by_uuid(resolve_by_uuid_)
    {}

    std::string getName() const override { return mergeTreeAnalyzeIndexFunctionName(resolve_by_uuid); }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override
    {
        /// Technically it's MergeTreeAnalyzeIndexes but it doesn't register itself
        return "";
    }

    void parseArgumentsUUID(const ASTs & args_func, ContextPtr context);
    void parseArgumentsDatabaseTable(const ASTs & args_func, ContextPtr context);

    /// 2 features will benefit from distributed index load + analysis:
    /// a) vector search with large vector indexes
    /// b) top-k using only minmax index (e.g SELECT * FROM youtube ORDER BY dislike_count LIMIT 10)
    /// These 2 cannot be packaged in the 'predicate'
    void parseArgumentsForOptimizations(const ASTs & args, ContextPtr context);

    const bool resolve_by_uuid;
    StorageID source_table_id{StorageID::createEmpty()};
    String parts_regexp;
    ASTPtr predicate;
    OptionalVectorSearchParameters vector_search_parameters;
};

std::vector<size_t> TableFunctionMergeTreeAnalyzeIndexes::skipAnalysisForArguments(const QueryTreeNodePtr & /* query_node_table_function */, ContextPtr /* context */) const
{
    /// Filter should not be analyzed
    if (resolve_by_uuid)
        return {1};
    else
        return {2};
}

void TableFunctionMergeTreeAnalyzeIndexes::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function ({}) must have arguments.", quoteString(getName()));

    if (resolve_by_uuid)
        parseArgumentsUUID(args_func, context);
    else
        parseArgumentsDatabaseTable(args_func, context);
}

void TableFunctionMergeTreeAnalyzeIndexes::parseArgumentsUUID(const ASTs & args_func, ContextPtr context)
{
    ASTs & args = args_func.at(0)->children;
    /// clang-tidy suggest to use args.empty() over args.size() < 1, which looks wrong here, but OK, let's use empty()
    if (args.empty() || args.size() > 5)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' must have at from 1 to 3 or 5 arguments (UUID, condition[, parts_regexp], [, optimization, args_array]), got: {}", getName(), args.size());

    args[0] = evaluateConstantExpressionAsLiteral(args[0], context);
    auto uuid = parseFromString<UUID>(checkAndGetLiteralArgument<String>(args[0], "UUID"));

    if (args.size() > 1)
        predicate = args[1]->clone();

    if (args.size() > 2)
    {
        args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
        parts_regexp = checkAndGetLiteralArgument<String>(args[2], "parts_regexp");
    }

    if (args.size() > 3)
        parseArgumentsForOptimizations(args, context);

    source_table_id = StorageID{/*database=*/ "", /*table=*/ "", uuid};
}

void TableFunctionMergeTreeAnalyzeIndexes::parseArgumentsDatabaseTable(const ASTs & args_func, ContextPtr context)
{
    ASTs & args = args_func.at(0)->children;
    if (args.size() < 2 || args.size() > 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' must have at from 2 to 4 arguments (database, table, condition[, parts_regexp]), got: {}", getName(), args.size());

    args[0] = evaluateConstantExpressionForDatabaseName(args[0], context);
    auto database = checkAndGetLiteralArgument<String>(args[0], "database");

    args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
    auto table = checkAndGetLiteralArgument<String>(args[1], "table");

    if (args.size() > 2)
        predicate = args[2]->clone();

    if (args.size() > 3)
    {
        args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(args[3], context);
        parts_regexp = checkAndGetLiteralArgument<String>(args[3], "parts_regexp");
    }

    if (args.size() > 3)
        parseArgumentsForOptimizations(args, context);

    source_table_id = StorageID{database, table};
}

void TableFunctionMergeTreeAnalyzeIndexes::parseArgumentsForOptimizations(const ASTs & args, ContextPtr /*context*/)
{
    if (args.size() == 5)
    {
        auto optimization = checkAndGetLiteralArgument<String>(args[3], "extra_optimization");
        if (optimization == "vector_search_index_analysis")
        {
            auto cast_node = args[4]->children.at(0);
            auto vector_search_args = cast_node->children.at(0)->as<ASTLiteral>()->value.safeGet<Array>();
            if (vector_search_args.size() != 6)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "vector_search_index_analysis requires 6 arguments");

            Array field_array = vector_search_args[3].safeGet<Array>();
            std::vector<Float64> reference_vector;
            for (const auto & field_array_value : field_array)
            {
                Float64 float64 = field_array_value.safeGet<Float64>();
                reference_vector.push_back(float64);
            }

            vector_search_parameters = VectorSearchParameters{vector_search_args[0].safeGet<String>(), /// column
                vector_search_args[1].safeGet<String>(), /// distance function
                vector_search_args[2].safeGet<UInt64>(), /// limit
                reference_vector, /// search vector
                static_cast<bool>(vector_search_args[4].safeGet<bool>()), /// additional filters
                static_cast<bool>(vector_search_args[5].safeGet<bool>())}; /// return distances
        }
    }
}

ColumnsDescription TableFunctionMergeTreeAnalyzeIndexes::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    return ColumnsDescription(NamesAndTypesList({
        {"part_name", std::make_shared<DataTypeString>()},
        {"ranges", std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{
            std::make_shared<DataTypeUInt64>(), // begin
            std::make_shared<DataTypeUInt64>(), // end
        }))},
    }));
}

StoragePtr TableFunctionMergeTreeAnalyzeIndexes::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    StoragePtr source_table;
    if (source_table_id.hasUUID())
    {
        /// Note, there is no getByUUID() at the time of writing, hence using try*() methods.
        auto database_and_table = DatabaseCatalog::instance().tryGetByUUID(source_table_id.uuid);
        source_table = DatabaseCatalog::instance().tryGetByUUID(source_table_id.uuid).second;
        if (!source_table)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table with UUID {} does not exist", source_table_id.uuid);
    }
    else
        source_table = DatabaseCatalog::instance().getTable(source_table_id, context);

    auto columns = getActualTableStructure(context, is_insert_query);
    StorageID storage_id(getDatabaseName(), table_name);

    auto res = std::make_shared<StorageMergeTreeAnalyzeIndexes>(
        std::move(storage_id),
        std::move(source_table),
        std::move(columns),
        parts_regexp,
        predicate,
        vector_search_parameters);
    res->startup();
    return res;
}

void registerTableFunctionMergeTreeAnalyzeIndexes(TableFunctionFactory & factory)
{
    factory.registerFunction(mergeTreeAnalyzeIndexFunctionName(/*resolve_by_uuid=*/ false), TableFunctionFactoryData{
        []() { return std::make_shared<TableFunctionMergeTreeAnalyzeIndexes>(/* resolve_by_uuid_= */ false); },
        TableFunctionProperties{
            .documentation =
            {
                .description = "Internal function for index analysis",
                .examples = {{"mergeTreeAnalyzeIndexes", "SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), mt_table, predicate[, 'parts_regexp'])", ""}},
                .category = FunctionDocumentation::Category::TableFunction
            },
            .allow_readonly = true,
        }
    });

    factory.registerFunction(mergeTreeAnalyzeIndexFunctionName(/*resolve_by_uuid=*/ true), TableFunctionFactoryData{
        []() { return std::make_shared<TableFunctionMergeTreeAnalyzeIndexes>(/* resolve_by_uuid_= */ true); },
        TableFunctionProperties{
            .documentation =
            {
                .description = "Internal function for index analysis",
                .examples = {{"mergeTreeAnalyzeIndexes", "SELECT * FROM mergeTreeAnalyzeIndexesUUID('table_uuid', predicate[, 'parts_regexp'])", ""}},
                .category = FunctionDocumentation::Category::TableFunction
            },
            .allow_readonly = true,
        }
    });
}

}
