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
    /// Shared with the resulting storage so that an OAuth access token minted during schema inference
    /// (analysis) is reused during execution instead of being re-requested from the token endpoint.
    std::shared_ptr<BigQueryTokenProvider> token_provider;
    /// The remote schema is fetched over the network once during analysis. It is cached here and handed
    /// to the storage so that execution reuses the same schema snapshot (no second `tables.get`, and no
    /// mismatch if the BigQuery table changes between analysis and execution).
    mutable std::optional<BigQueryFields> fetched_fields;
};

StoragePtr TableFunctionBigQuery::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    std::optional<BigQueryFields> prefetched_fields;
    ColumnsDescription columns;
    if (cached_columns.empty())
    {
        columns = getActualTableStructure(context, is_insert_query);
        prefetched_fields = fetched_fields;
    }
    else
    {
        columns = std::move(cached_columns);
        /// A cache hit bypasses schema inference, so `fetched_fields` may be empty here; in that case the
        /// storage falls back to fetching the schema lazily on the first read or write.
        prefetched_fields = fetched_fields;
    }

    auto storage = std::make_shared<StorageBigQuery>(
        StorageID(getDatabaseName(), table_name), *configuration, columns, ConstraintsDescription(), String{}, context,
        token_provider, std::move(prefetched_fields));
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionBigQuery::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (!fetched_fields)
        fetched_fields = StorageBigQuery::fetchTableSchema(*configuration, context, token_provider);
    return columnsDescriptionFromBigQuerySchema(*fetched_fields);
}

void TableFunctionBigQuery::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function 'bigquery' must have arguments");

    configuration = std::make_shared<BigQueryConfiguration>(BigQueryConfiguration::fromArguments(func_args.arguments->children, context));
    /// The token provider only stores the configuration here; no network request is made until a token
    /// is actually needed (during schema inference or execution).
    token_provider = std::make_shared<BigQueryTokenProvider>(*configuration);
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
