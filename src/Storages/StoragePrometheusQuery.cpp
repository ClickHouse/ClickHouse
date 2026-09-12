#include <Storages/StoragePrometheusQuery.h>

#include <Common/logger_useful.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Core/ConstantValue.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Setting
{
    extern const SettingsBool enable_materialized_cte;
}

namespace
{

/// Read a required String literal argument as a value, without materializing a `Field`.
String getStringConstArgument(const ASTPtr & arg, const ContextPtr & context, std::string_view arg_name)
{
    const auto value = evaluateConstantExpressionAsColumn(arg, context);
    /// Accept `Nullable`/`LowCardinality` wrappers: the previous `Field`-based code read the value
    /// via `operator[]`, which flattens wrappers, so a non-NULL `Nullable(String)`/
    /// `LowCardinality(String)` constant passed the String check. Preserve that, and still reject a
    /// NULL value as before.
    if (!isStringOrFixedString(removeLowCardinalityAndNullable(value.getType())))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got {}", arg_name, value.getType()->getName());
    if (value.isNull())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got NULL", arg_name);
    return String(value.getDataAt());
}

/// Rewrites the argument(s) naming the TimeSeries table to its resolved `database.table`, keeping the
/// argument count. A single argument becomes a compound identifier, the node the parser itself produces
/// for `database.table`; a table identifier there would be resolved as an expression and rejected.
void setResolvedTableArgument(ASTs & args, size_t num_table_args, const StorageID & storage_id)
{
    if (num_table_args == 1)
        args[0] = make_intrusive<ASTIdentifier>(std::vector<String>{storage_id.database_name, storage_id.table_name});
    else
    {
        args[0] = make_intrusive<ASTLiteral>(storage_id.database_name);
        args[1] = make_intrusive<ASTLiteral>(storage_id.table_name);
    }
}

}

StoragePrometheusQuery::Arguments StoragePrometheusQuery::parseArgumentsOnly(ASTs & args, const ContextPtr & context, bool over_range)
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
            time_series_storage_id.table_name = getStringConstArgument(args[argument_index++], context, "table_name");
        }
        else
        {
            /// prometheusQuery( 'mydb', 'my_time_series_table', ... )
            time_series_storage_id.database_name = getStringConstArgument(args[argument_index++], context, "database_name");
            time_series_storage_id.table_name = getStringConstArgument(args[argument_index++], context, "table_name");
        }
    }

    size_t num_table_args = argument_index;

    /// Fill in the database name while the creating context is still available and write it back into
    /// the argument: a stored definition is replayed against the loader's current database. Resolving
    /// the table itself here would wait for its startup job, which a load job cannot do.
    if (auto temporary_table_id = context->tryResolveStorageID(time_series_storage_id, Context::ResolveExternal))
    {
        /// A temporary table carries its own UUID and cannot appear in a stored definition.
        time_series_storage_id = std::move(temporary_table_id);
    }
    else
    {
        if (time_series_storage_id.database_name.empty())
            time_series_storage_id.database_name = context->getCurrentDatabase();
        if (!time_series_storage_id.database_name.empty())
            setResolvedTableArgument(args, num_table_args, time_series_storage_id);
    }

    Arguments parsed_args;
    parsed_args.time_series_storage_id = std::move(time_series_storage_id);
    parsed_args.promql_query = getStringConstArgument(args[argument_index++], context, "promql_query");

    if (over_range)
    {
        parsed_args.mode = PrometheusQueryEvaluationMode::QUERY_RANGE;
        std::tie(parsed_args.start_time, parsed_args.start_time_type) = evaluateConstantExpression(args[argument_index++], context);
        std::tie(parsed_args.end_time, parsed_args.end_time_type) = evaluateConstantExpression(args[argument_index++], context);
        std::tie(parsed_args.step, parsed_args.step_type) = evaluateConstantExpression(args[argument_index++], context);
    }
    else
    {
        parsed_args.mode = PrometheusQueryEvaluationMode::QUERY;
        std::tie(parsed_args.start_time, parsed_args.start_time_type) = evaluateConstantExpression(args[argument_index++], context);
        parsed_args.end_time = parsed_args.start_time;
        parsed_args.end_time_type = parsed_args.start_time_type;
    }

    chassert(argument_index == args.size());
    return parsed_args;
}

StoragePrometheusQuery::Configuration
StoragePrometheusQuery::resolveConfiguration(const Arguments & parsed_args, const ContextPtr & context)
{
    /// The parsed identifier carries a database name but no UUID: `parseArgumentsOnly` may not read the catalog.
    auto time_series_storage_id = context->tryResolveStorageID(parsed_args.time_series_storage_id);
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(context, false);
    auto [timestamp_data_type, scalar_data_type] = splitTimeSeriesType(
        time_series_metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type);

    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    Configuration config;
    config.promql_query = std::make_shared<PrometheusQueryTree>(parsed_args.promql_query, timestamp_scale);
    auto & evaluation_settings = config.evaluation_settings;
    evaluation_settings.time_series_storage_id = std::move(time_series_storage_id);
    evaluation_settings.timestamp_data_type = std::move(timestamp_data_type);
    evaluation_settings.scalar_data_type = std::move(scalar_data_type);
    evaluation_settings.mode = parsed_args.mode;
    evaluation_settings.start_time = parseTimeSeriesTimestamp(parsed_args.start_time, parsed_args.start_time_type, timestamp_scale);
    evaluation_settings.end_time = parseTimeSeriesTimestamp(parsed_args.end_time, parsed_args.end_time_type, timestamp_scale);
    evaluation_settings.step = parsed_args.step_type
        ? parseTimeSeriesDuration(parsed_args.step, parsed_args.step_type, timestamp_scale)
        : Decimal64{0};
    return config;
}

StoragePrometheusQuery::StoragePrometheusQuery(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const Configuration & config_)
    : StorageWithCommonVirtualColumns{table_id_}
    , config(config_)
    , log(getLogger("StoragePrometheusQuery"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StoragePrometheusQuery::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

void StoragePrometheusQuery::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(config.evaluation_settings.time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);

    LOG_INFO(log, "Building SQL to evaluate promql: {}", *config.promql_query);
    PrometheusQueryToSQL::Converter converter{config.promql_query, config.evaluation_settings};
    ASTPtr select_query = converter.getSQL();

    LOG_INFO(log, "Will execute query:\n{}", select_query->formatForLogging());
    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    /// Isolate the settings required by generated PromQL from the outer query.
    auto query_context = Context::createCopy(context);
    if (!context->getSettingsRef()[Setting::enable_materialized_cte].changed)
        query_context->setSetting("enable_materialized_cte", true);
    query_context->setSetting("empty_result_for_aggregation_by_empty_set", false);

    InterpreterSelectQueryAnalyzer interpreter(select_query, query_context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
