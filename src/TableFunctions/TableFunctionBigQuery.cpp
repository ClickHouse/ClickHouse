#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Storages/BigQuery/StorageBigQuery.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class TableFunctionBigQuery : public ITableFunction
{
public:
    static constexpr auto name = "bigquery";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "BigQuery"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::shared_ptr<BigQueryConfiguration> configuration;
    /// The remote schema is fetched over the network, cache it within the query.
    mutable std::optional<ColumnsDescription> fetched_columns;
};

StoragePtr TableFunctionBigQuery::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    auto columns = cached_columns.empty() ? getActualTableStructure(context, is_insert_query) : std::move(cached_columns);
    auto storage = std::make_shared<StorageBigQuery>(
        StorageID(getDatabaseName(), table_name), *configuration, columns, ConstraintsDescription(), String{}, context);
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionBigQuery::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (!fetched_columns)
        fetched_columns = columnsDescriptionFromBigQuerySchema(StorageBigQuery::fetchTableSchema(*configuration, context));
    return *fetched_columns;
}

void TableFunctionBigQuery::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function 'bigquery' must have arguments");

    configuration = std::make_shared<BigQueryConfiguration>(BigQueryConfiguration::fromArguments(func_args.arguments->children, context));
}

}

void registerTableFunctionBigQuery(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionBigQuery>(
    {
        .description = "Allows reading from and writing to a table in Google BigQuery. "
                       "The table structure is inferred from the BigQuery table schema.",
        .examples = {
            {"Read a public dataset",
             "SELECT * FROM bigquery('bigquery-public-data', 'samples', 'shakespeare', '<access_token>') LIMIT 10", ""},
            {"Authenticate with a service account key",
             "SELECT * FROM bigquery('my-project', 'my_dataset', 'my_table', service_account_key = '<key file content>')", ""},
            {"Insert data",
             "INSERT INTO FUNCTION bigquery('my-project', 'my_dataset', 'my_table', '<access_token>') VALUES (1, 'one')", ""},
        },
        .category = FunctionDocumentation::Category::TableFunction
    });
}

}
