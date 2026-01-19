#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
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
    auto metadata_snapshot = source_table->getInMemoryMetadataPtr();

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
    auto metadata_snapshot = source_table->getInMemoryMetadataPtr();
    ProjectionDescriptionRawPtr projection = &metadata_snapshot->getProjections().get(projection_name);

    StorageID storage_id(getDatabaseName(), table_name);
    auto res = std::make_shared<StorageFromMergeTreeProjection>(
        std::move(storage_id), std::move(source_table), std::move(metadata_snapshot), projection);

    res->startup();
    return res;
}

void registerTableFunctionMergeTreeProjection(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMergeTreeProjection>(
    {
        .documentation =
        {
            .description = "Reading directly from MergeTree projection",
            .examples = {{"mergeTreeProjection", "SELECT * FROM mergeTreeProjection(currentDatabase(), mt_table, proj_name)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = true,
    });
}

}
