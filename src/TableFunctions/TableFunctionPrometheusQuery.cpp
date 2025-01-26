#include <TableFunctions/TableFunctionPrometheusQuery.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageNull.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>

#include "config.h"

#if USE_ANTLR4_GRAMMARS
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Weverything"
#include <PromQLLexer.h>
#include <PromQLParser.h>
#pragma clang diagnostic pop
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_PARSE_PROMQL_QUERY;
    extern const int SUPPORT_IS_DISABLED;
}


namespace
{
#if USE_ANTLR4_GRAMMARS
    class PromQLErrorListener : public antlr4::BaseErrorListener
    {
    public:
        void syntaxError(antlr4::Recognizer * /*recognizer*/, antlr4::Token * /*offendingSymbol*/,
            size_t line, size_t charPositionInLine, const std::string &msg, std::exception_ptr /*e*/) override
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "Syntax error: {} while parsing PromQL query (line {}, column {})",
                            msg, line, charPositionInLine + 1);
        }
    };

    void checkPromQLSyntax(const String & promql_query)
    {
        antlr4::ANTLRInputStream input{promql_query};
        PromQLErrorListener error_listener;

        PromQLLexer lexer{&input};
        lexer.removeErrorListeners();
        lexer.addErrorListener(&error_listener);
        antlr4::CommonTokenStream tokens(&lexer);

        PromQLParser parser{&tokens};
        parser.removeErrorListeners();
        parser.addErrorListener(&error_listener);

        auto * expression = parser.expression();
        chassert(expression);

        String info = expression->toStringTree(&parser, true);
        LOG_INFO(getLogger("TableFunctionPrometheusQuery"), "Parsed PromQL query: {}", info);
    }
#endif
}


void TableFunctionPrometheusQuery::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;

    if ((args.size() != 2) && (args.size() != 3))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires one or two arguments: {}([database, ] time_series_table)", name, name);

    if (args.size() == 2)
    {
        /// prometheusQuery( [my_db.]my_time_series_table )
        if (const auto * id = args[0]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
                time_series_storage_id = table_id->getTableId();
        }
    }

    if (time_series_storage_id.empty())
    {
        for (size_t i = 0; i != args.size() - 1; ++i)
        {
            auto & arg = args[i];
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);
        }

        if (args.size() == 2)
        {
            /// prometheusQuery( 'my_time_series_table', 'promql_query' )
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[0], "table_name");
        }
        else
        {
            /// timeSeriesMetrics( 'mydb', 'my_time_series_table', 'promql_query' )
            time_series_storage_id.database_name = checkAndGetLiteralArgument<String>(args[0], "database_name");
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[1], "table_name");
        }
    }

    if (time_series_storage_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Couldn't get a table name from the arguments of the {} table function", name);

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);

    auto & last_arg = args[args.size() - 1];
    last_arg = evaluateConstantExpressionOrIdentifierAsLiteral(last_arg, context);
    promql_query = checkAndGetLiteralArgument<String>(last_arg, "promql_query");
}


ColumnsDescription TableFunctionPrometheusQuery::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    return ColumnsDescription(NamesAndTypesList({
                {"metric_name", std::make_shared<DataTypeString>()},
                {"tags", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
                {"time_series", std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                    DataTypes{std::make_shared<DataTypeDateTime64>(3), std::make_shared<DataTypeFloat64>()},
                    Strings{"timestamp", "value"}))},
            }));
}


const char * TableFunctionPrometheusQuery::getStorageTypeName() const
{
    return "Null";
}


StoragePtr TableFunctionPrometheusQuery::executeImpl(
    const ASTPtr & /* ast_function */,
    [[maybe_unused]] ContextPtr context,
    [[maybe_unused]] const String & table_name,
    ColumnsDescription /* cached_columns */,
    bool /* is_insert_query */) const
{
#if USE_ANTLR4_GRAMMARS

    /// TODO
    checkPromQLSyntax(promql_query);

    ColumnsDescription columns = getActualTableStructure(context, /* is_insert_query = */ false);
    auto res = std::make_shared<StorageNull>(StorageID(getDatabaseName(), table_name), columns, ConstraintsDescription(), String{});
    res->startup();
    return res;

#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ANTLR4 support is disabled");
#endif
}


void registerTableFunctionPrometheusQuery(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPrometheusQuery>(
        {.documentation = {
            .description=R"(Executes a prometheus query on a TimeSeries table.)",
            .examples{{"prometheusQuery", "SELECT * from prometheusQuery('mydb', 'time_series_table', 'http_requests_total{job=\"prometheus\",group=\"canary\"}');", ""}},
            .category{""}}
        });
}

}
