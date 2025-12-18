#include <TableFunctions/TableFunctionArrowFlight.h>

#if USE_ARROWFLIGHT
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
}

StoragePtr TableFunctionArrowFlight::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription cached_columns,
    bool /*is_insert_query*/) const
{
    return std::make_shared<StorageArrowFlight>(
        StorageID{"arrow_flight", table_name},
        connection,
        config.dataset_name,
        cached_columns,
        ConstraintsDescription{},
        context);
}

ColumnsDescription TableFunctionArrowFlight::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return StorageArrowFlight::getTableStructureFromData(connection, config.dataset_name, context);
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
