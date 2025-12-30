#include <TableFunctions/TableFunctionPrometheusQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StoragePrometheusQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


template <bool range>
void TableFunctionPrometheusQuery<range>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;

    size_t num_evaluation_time_args = range ? 3 : 1;

    size_t num_storage_id_args = args.size() - num_evaluation_time_args - 1;
    if ((num_storage_id_args < 1) || (num_storage_id_args > 2))
    {
        std::string_view evaluation_time_args = range ? "evaluation_time" : "start_time, end_time, step";
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires {} or {} arguments: {}([database, ] time_series_table, promql_query, {})",
                        name, num_evaluation_time_args + 2, num_evaluation_time_args + 3, name, evaluation_time_args);
    }

    size_t argument_index = 0;
    if (num_storage_id_args == 1)
    {
        /// prometheusQuery( [my_db.]my_time_series_table )
        if (const auto * id = args[argument_index]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
            {
                time_series_storage_id = table_id->getTableId();
                ++argument_index;
            }
        }
    }

    for (size_t i = argument_index; i != args.size(); ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    if (time_series_storage_id.empty())
    {
        if (args.size() == 3)
        {
            /// prometheusQuery( 'my_time_series_table' )
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[argument_index++], "table_name");
        }
        else
        {
            /// prometheusQuery( 'mydb', 'my_time_series_table' )
            time_series_storage_id.database_name = checkAndGetLiteralArgument<String>(args[argument_index++], "database_name");
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[argument_index++], "table_name");
        }
    }

    if (time_series_storage_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Couldn't get a table name from the arguments of the {} table function", name);

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);

    promql_query.parse(checkAndGetLiteralArgument<String>(args[argument_index++], "promql_query"));

    if constexpr (range)
    {
        evaluation_range.start_time = args[argument_index++]->as<const ASTLiteral &>().value;
        evaluation_range.end_time = args[argument_index++]->as<const ASTLiteral &>().value;
        evaluation_range.step = args[argument_index++]->as<const ASTLiteral &>().value;
    }
    else
    {
        evaluation_time = args[argument_index++]->as<const ASTLiteral &>().value;
    }

    chassert(argument_index == args.size());
}


template <bool range>
ColumnsDescription TableFunctionPrometheusQuery<range>::getActualTableStructure(ContextPtr context, bool /* is_insert_query */) const
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    auto data_table = time_series_storage->getTargetTable(ViewTarget::Data, context);
    auto data_table_metadata = data_table->getInMemoryMetadataPtr();
    PrometheusQueryToSQLConverter::TimeSeriesTableInfo time_series_table_info;
    time_series_table_info.storage_id = time_series_storage_id;
    time_series_table_info.timestamp_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    time_series_table_info.value_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Value).type;
    PrometheusQueryToSQLConverter converter{promql_query, time_series_table_info, Field{}, Field{}};
    if constexpr (range)
        converter.setEvaluationRange(evaluation_range);
    else
        converter.setEvaluationTime(evaluation_time);
    return converter.getResultColumns();
}


template <bool range>
StoragePtr TableFunctionPrometheusQuery<range>::executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const String & table_name,
        ColumnsDescription /* cached_columns */,
        bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StoragePrometheusQuery>(
        StorageID(getDatabaseName(), table_name), columns, time_series_storage_id, promql_query);
    if constexpr (range)
        res->setEvaluationRange(evaluation_range);
    else
        res->setEvaluationTime(evaluation_time);
    res->startup();
    return res;
}


template class TableFunctionPrometheusQuery<false>;
template class TableFunctionPrometheusQuery<true>;

}
