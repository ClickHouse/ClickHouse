#include <TableFunctions/TableFunctionArrowFlight.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageArrowFlight.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <DataTypes/DataTypeString.h>
#include <Common/parseAddress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARROWFLIGHT_CONNECTION_FAILURE;
    extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
}

StoragePtr TableFunctionArrowFlight::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);

    return std::make_shared<StorageArrowFlight>(
        StorageID{"arrow_flight", table_name},
        configuration.host,
        configuration.port,
        configuration.dataset_name,
        columns,
        ConstraintsDescription(),
        context);
}

ColumnsDescription TableFunctionArrowFlight::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    
    arrow::flight::Location location;
    auto location_result = arrow::flight::Location::ForGrpcTcp(configuration.host, configuration.port);
    if (!location_result.ok())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid Arrow Flight endpoint specified: {}",
            location_result.status().ToString()
        );
    }
    location = std::move(location_result).ValueOrDie();
    auto result = arrow::flight::FlightClient::Connect(location);
    if (!result.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE,
            "Failed to connect to Arrow Flight server: {}",
            result.status().ToString()
        );
    }
    auto client = std::move(result).ValueOrDie();
    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({configuration.dataset_name});
    auto status = client->GetSchema(descriptor);
    if (!status.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR,
            "Failed to get table schema: {}",
            status.status().ToString()
        );
    }
    arrow::ipc::DictionaryMemo dict;
    auto schema = status.ValueOrDie()->GetSchema(&dict).ValueOrDie();

    auto fields = schema->fields();
    ColumnsDescription desr;
    for (auto field : fields) {
        desr.add(ColumnDescription(field->name(), std::make_shared<DataTypeString>()));
    }
    return desr;
}

void TableFunctionArrowFlight::parseArguments(const ASTPtr & ast_function, ContextPtr /*context*/)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'arrowFlight' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (args.size() != 2)
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function 'arrowflight' requires 2 parameters: "
            "arrowflight('host:port', 'dataset_name')"
        );
    }

    auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(args[0], "flight_endpoint"), 6379);
    configuration.host = parsed_host_port.first;
    configuration.port = parsed_host_port.second;
    configuration.dataset_name = checkAndGetLiteralArgument<String>(args[1], "dataset_name");
}

void registerTableFunctionArrowFlight(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionArrowFlight>();
}

}
