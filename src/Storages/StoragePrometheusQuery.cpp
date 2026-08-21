#include <Storages/StoragePrometheusQuery.h>

#include <Common/logger_useful.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePrometheusQuery::Configuration StoragePrometheusQuery::getConfiguration(ASTs & args, const ContextPtr & context, bool over_range)
{
    std::string_view function_name = over_range ? "prometheusQueryRange" : "prometheusQuery";
    size_t min_num_args = 3 + over_range * 2;
    size_t max_num_args = 4 + over_range * 2;

    if ((args.size() < min_num_args) || (args.size() > max_num_args))
    {
        std::string_view expected_args = over_range ? "[database, ] time_series_table, promql_query, start_time, end_time, step"
                                                    : "[database, ] time_series_table, promql_query, evaluation_time";
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires {}..{} arguments: {}([database, ] time_series_table, promql_query, {})",
                        function_name, min_num_args, max_num_args, function_name, expected_args);
    }

    size_t argument_index = 0;

    StorageID time_series_storage_id = StorageID::createEmpty();

    if (args.size() == min_num_args)
    {
        /// prometheusQuery( [my_db.]my_time_series_table, ... )
        if (const auto * id = args[argument_index]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
            {
                time_series_storage_id = table_id->getTableId();
                ++argument_index;
            }
        }
    }

    if (time_series_storage_id.empty())
    {
        if (args.size() == min_num_args)
        {
            /// prometheusQuery( 'my_time_series_table', ... )
            auto table_name_field = evaluateConstantExpression(args[argument_index++], context).first;

            if (table_name_field.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'table_name' must be a literal with type String, got {}", table_name_field.getType());

            time_series_storage_id.table_name = table_name_field.safeGet<String>();
        }
        else
        {
            /// prometheusQuery( 'mydb', 'my_time_series_table', ... )
            auto database_name_field = evaluateConstantExpression(args[argument_index++], context).first;
            auto table_name_field = evaluateConstantExpression(args[argument_index++], context).first;

            if (database_name_field.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'database_name' must be a literal with type String, got {}", database_name_field.getType());

            if (table_name_field.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'table_name' must be a literal with type String, got {}", table_name_field.getType());

            time_series_storage_id.database_name = database_name_field.safeGet<String>();
            time_series_storage_id.table_name = table_name_field.safeGet<String>();
        }
    }

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);

    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    auto data_table_metadata = time_series_storage->getTargetTable(ViewTarget::Data, context)->getInMemoryMetadataPtr();
    auto timestamp_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    auto scalar_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Value).type;

    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    auto promql_query_field = evaluateConstantExpression(args[argument_index++], context).first;
    if (promql_query_field.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'promql_query' must be a literal with type String, got {}", promql_query_field.getType());

    PrometheusQueryTree promql_query{promql_query_field.safeGet<String>()};

    PrometheusQueryEvaluationMode mode;
    DateTime64 start_time;
    DateTime64 end_time;
    Decimal64 step;

    if (over_range)
    {
        auto [start_time_field, start_time_type] = evaluateConstantExpression(args[argument_index++], context);
        auto [end_time_field, end_time_type] = evaluateConstantExpression(args[argument_index++], context);
        auto [step_field, step_type] = evaluateConstantExpression(args[argument_index++], context);

        mode = PrometheusQueryEvaluationMode::QUERY_RANGE;
        start_time = parseTimeSeriesTimestamp(start_time_field, start_time_type, timestamp_scale);
        end_time = parseTimeSeriesTimestamp(end_time_field, end_time_type, timestamp_scale);
        step = parseTimeSeriesDuration(step_field, step_type, timestamp_scale);
    }
    else
    {
        auto [time_field, time_type] = evaluateConstantExpression(args[argument_index++], context);

        mode = PrometheusQueryEvaluationMode::QUERY;
        start_time = parseTimeSeriesTimestamp(time_field, time_type, timestamp_scale);
        end_time = start_time;
        step = 0;
    }

    chassert(argument_index == args.size());

    Configuration config;
    config.promql_query = std::make_shared<PrometheusQueryTree>(std::move(promql_query));
    auto & evaluation_settings = config.evaluation_settings;
    evaluation_settings.time_series_storage_id = std::move(time_series_storage_id);
    evaluation_settings.timestamp_data_type = std::move(timestamp_data_type);
    evaluation_settings.scalar_data_type = std::move(scalar_data_type);
    evaluation_settings.mode = mode;
    evaluation_settings.start_time = start_time;
    evaluation_settings.end_time = end_time;
    evaluation_settings.step = step;
    return config;
}

StoragePrometheusQuery::StoragePrometheusQuery(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const Configuration & config_)
    : IStorage{table_id_}
    , config(config_)
    , log(getLogger("StoragePrometheusQuery"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

void StoragePrometheusQuery::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    LOG_INFO(log, "Building SQL to evaluate promql: {}", *config.promql_query);
    PrometheusQueryToSQL::Converter converter{config.promql_query, config.evaluation_settings};
    ASTPtr select_query = converter.getSQL();

    LOG_INFO(log, "Will execute query:\n{}", select_query->formatForLogging());
    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);
    InterpreterSelectQueryAnalyzer interpreter(select_query, context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
