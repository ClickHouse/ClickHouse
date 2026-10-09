#include <Parsers/ASTIdentifier_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Storages/Elasticsearch/ElasticsearchSettings.h>
#include <Storages/Elasticsearch/StorageElasticsearch.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
class TableFunctionElasticsearch : public ITableFunction
{
public:
    static constexpr auto name = "elasticsearch";

    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "Elasticsearch"; }

    const String & getFunctionURI() const override { return configuration->url; }

    String getUsedNamedCollectionName() const override { return configuration ? configuration->named_collection_name : String{}; }

    ColumnsDescription getActualTableStructure(ContextPtr /*context*/, bool is_insert_query) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::shared_ptr<ElasticsearchConfiguration> configuration;
    ElasticsearchSettings settings_provided_in_call;
    ColumnsDescription structure_columns;
};

StoragePtr TableFunctionElasticsearch::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    auto columns = cached_columns.empty() ? getActualTableStructure(context, is_insert_query) : std::move(cached_columns);
    StorageElasticsearch::checkColumns(columns);
    auto es_settings_ptr = std::make_unique<ElasticsearchSettings>(settings_provided_in_call);
    auto storage = std::make_shared<StorageElasticsearch>(
        StorageID(getDatabaseName(), table_name), *configuration, columns, ConstraintsDescription(), String{}, std::move(es_settings_ptr));
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionElasticsearch::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    if (!structure_columns.empty())
        return structure_columns;
    return StorageElasticsearch::getDefaultColumns();
}

void TableFunctionElasticsearch::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments", getName());

    ASTs & args = func_args.arguments->children;

    for (auto it = args.begin(); it != args.end(); ++it)
    {
        const ASTSetQuery * settings_ast = (*it)->as<ASTSetQuery>();
        if (settings_ast)
        {
            settings_provided_in_call.loadFromQuery(*settings_ast);
            settings_provided_in_call.sanityCheck();
            args.erase(it);
            break;
        }
    }

    for (auto it = args.begin(); it != args.end(); ++it)
    {
        const ASTFunction * func_ast = (*it)->as<ASTFunction>();
        if (!func_ast || func_ast->name != "equals" || !func_ast->arguments || func_ast->arguments->children.size() != 2)
            continue;

        if (tryGetIdentifierName(func_ast->arguments->children[0]) != "structure")
            continue;

        auto structure = checkAndGetLiteralArgument<String>(func_ast->arguments->children[1], "structure");
        structure_columns = parseColumnsListFromString(structure, context);
        StorageElasticsearch::checkColumns(structure_columns);
        args.erase(it);
        break;
    }

    configuration = std::make_shared<ElasticsearchConfiguration>(ElasticsearchConfiguration::fromArguments(args, context));
}

}

void registerTableFunctionElasticsearch(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionElasticsearch>(
        {
            .description = R"DOCS_MD(Table function for elasticsearch)DOCS_MD",
            .category = FunctionDocumentation::Category::TableFunction
        },
        SecretArgumentsSpec{.positional_secret_slots = {3}, .secret_keys = {"password", "bearer_token", "api_key"}});
}

}
