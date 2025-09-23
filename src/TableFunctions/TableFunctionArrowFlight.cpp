#include <TableFunctions/TableFunctionArrowFlight.h>

#if USE_ARROWFLIGHT
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageArrowFlight.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
}

StoragePtr TableFunctionArrowFlight::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);

    return std::make_shared<StorageArrowFlight>(
        StorageID{"arrow_flight", table_name},
        connection,
        config.dataset_name,
        columns,
        ConstraintsDescription(),
        context);
}

ColumnsDescription TableFunctionArrowFlight::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    auto client = connection->getClient();
    auto options = connection->getOptions();

    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({config.dataset_name});
    auto status = client->GetSchema(*options, descriptor);
    if (!status.ok())
    {
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "Failed to get table schema: {}", status.status().ToString());
    }
    arrow::ipc::DictionaryMemo dict;
    auto schema_result = status.ValueOrDie()->GetSchema(&dict);
    if (!schema_result.ok())
    {
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "Failed to get table schema: {}", schema_result.status().ToString());
    }
    auto schema = std::move(schema_result).ValueOrDie();

    auto fields = schema->fields();
    ColumnsDescription desr;
    for (const auto & field : fields)
    {
        desr.add(ColumnDescription(field->name(), std::make_shared<DataTypeString>()));
    }
    return desr;
}

void TableFunctionArrowFlight::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'arrowFlight' must have arguments.");

    ASTs & args = func_args.arguments->children;
    config = StorageArrowFlight::getConfiguration(args, context);

    /// ArrowFlightConnection will establish connection lazily.
    connection = std::make_shared<ArrowFlightConnection>(config);
}

void registerTableFunctionArrowFlight(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionArrowFlight>(
        {.documentation
         = {.description = R"(Allows to perform queries on data exposed via an Apache Arrow Flight server.)",
            .examples{{"arrowFlight", "SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;", ""}},
            .category = FunctionDocumentation::Category::TableFunction}});

    /// "arrowflight" is an obsolete name.
    factory.registerAlias("arrowflight", "arrowFlight");
}

}

#endif
