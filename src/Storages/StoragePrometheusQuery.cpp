#include <Storages/StoragePrometheusQuery.h>

#include <Common/logger_useful.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/UUID.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/SelectQueryOptions.h>
#include <IO/WriteHelpers.h>
#include <Core/ConstantValue.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Prometheus/PrometheusQueryClassifier.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/PromQLNativePlanBuilder.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <Storages/TimeSeries/getPromQLResultTimestampType.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>

#include <algorithm>
#include <base/arithmeticOverflow.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Setting
{
    extern const SettingsBool enable_promql_native_plan;
    extern const SettingsBool enable_materialized_cte;
    extern const SettingsBool make_distributed_plan;
    extern const SettingsUInt64 max_promql_native_output_groups;
    extern const SettingsUInt64 max_promql_native_vector_grid_cells;
    extern const SettingsUInt64 max_promql_query_block_size;
    extern const SettingsUInt64 min_promql_native_query_range_points;
    extern const SettingsBool serialize_query_plan;
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

const PrometheusQueryTree::Node * findH0NativeFragment(
    const PrometheusQueryTree & promql_query,
    bool is_query_range)
{
    const auto * root = promql_query.getRoot();
    if (!root || root->node_type != PrometheusQueryTree::NodeType::Function)
        return nullptr;

    const auto * function = typeid_cast<const PrometheusQueryTree::Function *>(root);
    if (!function || function->function_name != "clamp_max" || function->getArguments().size() != 2)
        return nullptr;

    const auto * fragment = function->getArguments().front();
    const auto * bound = function->getArguments().back();
    if (!bound || bound->node_type != PrometheusQueryTree::NodeType::Scalar)
        return nullptr;

    if (!extractPromQLRangeSumByQuery(fragment, is_query_range))
        return nullptr;

    return fragment;
}

const PrometheusQueryTree::Node * findH1NativeFragment(
    const PrometheusQueryTree & promql_query,
    bool is_query_range)
{
    if (!extractPromQLRangeTopKByQuery(promql_query, is_query_range))
        return nullptr;

    const auto * root = promql_query.getRoot();
    if (!root || root->node_type != PrometheusQueryTree::NodeType::AggregationOperator)
        return nullptr;

    const auto * aggregation = typeid_cast<const PrometheusQueryTree::AggregationOperator *>(root);
    if (!aggregation || aggregation->getArguments().size() != 2)
        return nullptr;

    return aggregation->getArguments().at(1);
}

std::shared_ptr<const PrometheusQueryTree> clonePromQLSubtree(
    const PrometheusQueryTree::Node * root,
    UInt32 timestamp_scale)
{
    std::vector<std::unique_ptr<PrometheusQueryTree::Node>> nodes;
    auto * cloned_root = root->clone(nodes);
    return std::make_shared<const PrometheusQueryTree>(std::move(nodes), cloned_root, timestamp_scale);
}

class StoragePromQLNativeFragment final : public IStorage
{
public:
    StoragePromQLNativeFragment(
        const StorageID & table_id,
        std::shared_ptr<const PrometheusQueryTree> promql_query_,
        PrometheusQueryEvaluationSettings evaluation_settings_,
        size_t max_output_groups_,
        BuiltSetsByHashPtr prepared_identifier_sets_)
        : IStorage(table_id)
        , promql_query(std::move(promql_query_))
        , evaluation_settings(std::move(evaluation_settings_))
        , max_output_groups(max_output_groups_)
        , prepared_identifier_sets(std::move(prepared_identifier_sets_))
    {
        const auto nullable_scalar_type = makeNullable(std::make_shared<DataTypeFloat64>());
        StorageInMemoryMetadata metadata;
        metadata.setColumns(ColumnsDescription({
            {TimeSeriesColumnNames::Group, std::make_shared<DataTypeUInt64>()},
            {TimeSeriesColumnNames::Values, std::make_shared<DataTypeArray>(nullable_scalar_type)},
        }));
        setInMemoryMetadata(metadata);
    }

    std::string getName() const override { return "PromQLNativeFragment"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        if (!tryBuildPromQLNativeVectorGridPlan(
                query_plan,
                column_names,
                storage_snapshot,
                query_info,
                context,
                processed_stage,
                max_block_size,
                num_streams,
                *promql_query,
                evaluation_settings,
                max_output_groups,
                prepared_identifier_sets))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The admitted PromQL native fragment cannot build its VECTOR_GRID plan");
        }
    }

private:
    const std::shared_ptr<const PrometheusQueryTree> promql_query;
    const PrometheusQueryEvaluationSettings evaluation_settings;
    const size_t max_output_groups;
    const BuiltSetsByHashPtr prepared_identifier_sets;
};

struct PreparedNativeFragment
{
    const PrometheusQueryTree::Node * node = nullptr;
    std::shared_ptr<const PrometheusQueryTree> promql_query;
    PromQLNativeVectorGridPreparation preparation;
    bool metric_name_dropped = false;
};

PrometheusQueryToSQL::NativeFragmentDescriptions installNativeFragments(
    const ContextMutablePtr & query_context,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    size_t max_output_groups,
    std::vector<PreparedNativeFragment> prepared_fragments)
{
    struct FragmentToInstall
    {
        const PrometheusQueryTree::Node * node = nullptr;
        String name;
        std::shared_ptr<TemporaryTableHolder> holder;
        bool metric_name_dropped = false;
    };

    std::vector<FragmentToInstall> fragments_to_install;
    fragments_to_install.reserve(prepared_fragments.size());
    for (auto & prepared : prepared_fragments)
    {
        String fragment_name = "__promql_native_fragment_" + toString(UUIDHelpers::generateV4());
        std::ranges::replace(fragment_name, '-', '_');

        auto fragment_query = std::move(prepared.promql_query);
        auto prepared_identifier_sets = std::move(prepared.preparation.identifier_sets);
        auto holder = std::make_shared<TemporaryTableHolder>(
            query_context,
            [fragment_query, evaluation_settings, max_output_groups, prepared_identifier_sets](const StorageID & table_id)
            {
                return std::make_shared<StoragePromQLNativeFragment>(
                    table_id, fragment_query, evaluation_settings, max_output_groups, prepared_identifier_sets);
            });

        fragments_to_install.push_back(FragmentToInstall{
            .node = prepared.node,
            .name = std::move(fragment_name),
            .holder = std::move(holder),
            .metric_name_dropped = prepared.metric_name_dropped,
        });
    }

    PrometheusQueryToSQL::NativeFragmentDescriptions result;
    result.reserve(fragments_to_install.size());
    for (const auto & fragment : fragments_to_install)
    {
        result.push_back(PrometheusQueryToSQL::NativeFragmentDescription{
            .node = fragment.node,
            .table_name = fragment.name,
            .metric_name_dropped = fragment.metric_name_dropped,
        });
    }

    size_t installed_fragments = 0;
    try
    {
        for (auto & fragment : fragments_to_install)
        {
            query_context->addExternalTable(fragment.name, fragment.holder);
            ++installed_fragments;
        }
    }
    catch (...)
    {
        for (size_t i = 0; i != installed_fragments; ++i)
            query_context->removeExternalTable(result[i].table_name);
        throw;
    }

    return result;
}

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
            time_series_storage_id.table_name = getStringConstArgument(args[argument_index++], context, "table_name");
        }
        else
        {
            /// prometheusQuery( 'mydb', 'my_time_series_table', ... )
            time_series_storage_id.database_name = getStringConstArgument(args[argument_index++], context, "database_name");
            time_series_storage_id.table_name = getStringConstArgument(args[argument_index++], context, "table_name");
        }
    }

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);

    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);
    UInt64 time_series_version = time_series_storage->getVersion();
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(context, false);
    auto table_timestamp_type = splitTimeSeriesType(
        time_series_metadata->columns.get(TimeSeriesColumnNames::getOuterSamples(time_series_version)).type).first;

    UInt32 time_scale = getPromQLResultTimestampScale(table_timestamp_type);

    PrometheusQueryTree promql_query{getStringConstArgument(args[argument_index++], context, "promql_query"), time_scale};

    PrometheusQueryEvaluationMode mode = {};
    DateTime64 start_time;
    DateTime64 end_time;
    Decimal64 step;
    DataTypes time_parameter_types;  /// The types of the timestamp parameters: they can specify the time zone of the results.

    if (over_range)
    {
        auto [start_time_field, start_time_type] = evaluateConstantExpression(args[argument_index++], context);
        auto [end_time_field, end_time_type] = evaluateConstantExpression(args[argument_index++], context);
        auto [step_field, step_type] = evaluateConstantExpression(args[argument_index++], context);

        mode = PrometheusQueryEvaluationMode::QUERY_RANGE;
        start_time = parseTimeSeriesTimestamp(start_time_field, start_time_type, time_scale);
        end_time = parseTimeSeriesTimestamp(end_time_field, end_time_type, time_scale);
        step = parseTimeSeriesDuration(step_field, step_type, time_scale);
        time_parameter_types = {start_time_type, end_time_type};
    }
    else
    {
        auto [time_field, time_type] = evaluateConstantExpression(args[argument_index++], context);

        mode = PrometheusQueryEvaluationMode::QUERY;
        start_time = parseTimeSeriesTimestamp(time_field, time_type, time_scale);
        end_time = start_time;
        step = 0;
        time_parameter_types = {time_type};
    }

    chassert(argument_index == args.size());

    Configuration config;
    config.promql_query = std::make_shared<PrometheusQueryTree>(std::move(promql_query));
    auto & evaluation_settings = config.evaluation_settings;
    evaluation_settings.time_series_storage_id = std::move(time_series_storage_id);
    evaluation_settings.time_series_version = time_series_version;
    evaluation_settings.time_zone = getPromQLResultTimeZone(table_timestamp_type, time_parameter_types);
    evaluation_settings.table_timestamp_type = std::move(table_timestamp_type);
    evaluation_settings.time_scale = time_scale;
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
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(config.evaluation_settings.time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);

    const bool is_query_range = config.evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE;
    const auto min_native_range_points = context->getSettingsRef()[Setting::min_promql_native_query_range_points];
    const auto query_range_points = is_query_range
        ? PrometheusQueryToSQL::stepsInTimeSeriesRange(
            *config.evaluation_settings.start_time,
            *config.evaluation_settings.end_time,
            *config.evaluation_settings.step)
        : 1;
    const bool native_range_admitted = !is_query_range || !min_native_range_points || query_range_points >= min_native_range_points;
    const auto & settings = context->getSettingsRef();
    size_t promql_max_block_size = max_block_size;
    const auto configured_promql_block_size = settings[Setting::max_promql_query_block_size];
    if (configured_promql_block_size.value)
    {
        promql_max_block_size = std::min(
            promql_max_block_size, static_cast<size_t>(configured_promql_block_size.value));
    }
    const bool native_plan_enabled = settings[Setting::enable_promql_native_plan]
        && !settings[Setting::make_distributed_plan]
        && !settings[Setting::serialize_query_plan];

    if (settings[Setting::enable_promql_native_plan] && !native_plan_enabled)
    {
        LOG_INFO(
            log,
            "PromQL native plan steps are not serializable; using the SQL plan while make_distributed_plan={} and serialize_query_plan={}",
            settings[Setting::make_distributed_plan].value,
            settings[Setting::serialize_query_plan].value);
    }

    if (native_plan_enabled && !native_range_admitted)
    {
        LOG_INFO(
            log,
            "PromQL range has {} evaluation points, below min_promql_native_query_range_points={}; using the SQL plan",
            query_range_points,
            min_native_range_points.value);
    }

    const bool has_exact_native_root = extractPromQLRangeSumByQuery(*config.promql_query, is_query_range)
        || extractPromQLRangeRateQuery(config.promql_query->getRoot(), is_query_range);
    if (native_plan_enabled
        && native_range_admitted
        && has_exact_native_root
        && tryBuildPromQLNativePlan(
            query_plan,
            column_names,
            storage_snapshot,
            query_info,
            context,
            processed_stage,
            promql_max_block_size,
            num_streams,
            *config.promql_query,
            config.evaluation_settings,
            context->getSettingsRef()[Setting::max_promql_native_output_groups]))
    {
        LOG_INFO(log, "Using the native execution plan to evaluate promql: {}", *config.promql_query);
        return;
    }

    /// Isolate the settings required by generated PromQL from the outer query.
    auto query_context = Context::createCopy(context);
    if (configured_promql_block_size.value)
        query_context->setSetting("max_block_size", Field(static_cast<UInt64>(promql_max_block_size)));
    if (!context->getSettingsRef()[Setting::enable_materialized_cte].changed)
        query_context->setSetting("enable_materialized_cte", true);
    query_context->setSetting("empty_result_for_aggregation_by_empty_set", false);

    PrometheusQueryToSQL::NativeFragmentDescriptions native_fragments;
    if (native_plan_enabled && native_range_admitted)
    {
        const auto prepare_fragment = [&](const PrometheusQueryTree::Node * fragment_node, bool metric_name_dropped)
            -> std::optional<PreparedNativeFragment>
        {
            auto fragment_query = clonePromQLSubtree(fragment_node, config.promql_query->getTimeScale());
            auto preparation = tryPreparePromQLNativeVectorGridPlan(
                query_info,
                query_context,
                processed_stage,
                promql_max_block_size,
                num_streams,
                *fragment_query,
                config.evaluation_settings);
            if (!preparation)
                return {};
            return PreparedNativeFragment{
                .node = fragment_node,
                .promql_query = std::move(fragment_query),
                .preparation = std::move(*preparation),
                .metric_name_dropped = metric_name_dropped,
            };
        };

        if (auto two_rate_query = extractPromQLTwoRangeRatesSumByQuery(*config.promql_query, is_query_range))
        {
            const auto max_grid_cells = settings[Setting::max_promql_native_vector_grid_cells];
            auto prepared = max_grid_cells.value
                ? prepare_fragment(two_rate_query->binary_node, /* metric_name_dropped = */ true)
                : std::optional<PreparedNativeFragment>{};
            bool admitted = prepared.has_value();
            UInt64 grid_cells = 0;
            if (admitted
                && (common::mulOverflow(
                        static_cast<UInt64>(prepared->preparation.selected_series),
                        static_cast<UInt64>(query_range_points),
                        grid_cells)
                    || grid_cells > max_grid_cells.value))
            {
                admitted = false;
            }

            if (admitted)
            {
                native_fragments = installNativeFragments(
                    query_context,
                    config.evaluation_settings,
                    settings[Setting::max_promql_native_output_groups],
                    std::vector<PreparedNativeFragment>{std::move(*prepared)});
            }
            else
            {
                LOG_INFO(
                    log,
                    "The fused PromQL two-rate fragment exceeds native admission limits or is incompatible with the VECTOR_GRID contract; using the SQL plan");
            }
        }
        else
        {
            const auto * fragment_node = findH0NativeFragment(*config.promql_query, is_query_range);
            if (!fragment_node)
                fragment_node = findH1NativeFragment(*config.promql_query, is_query_range);
            if (fragment_node)
            {
                auto prepared = prepare_fragment(fragment_node, /* metric_name_dropped = */ true);
                if (prepared)
                {
                    native_fragments = installNativeFragments(
                        query_context,
                        config.evaluation_settings,
                        settings[Setting::max_promql_native_output_groups],
                        std::vector<PreparedNativeFragment>{std::move(*prepared)});
                }
                else
                {
                    LOG_INFO(log, "The PromQL fragment is not compatible with the native VECTOR_GRID contract; using the SQL plan");
                }
            }
        }
    }

    LOG_INFO(
        log,
        "Building {} to evaluate promql: {}",
        native_fragments.empty() ? "SQL" : "hybrid native-fragment/SQL plan",
        *config.promql_query);
    PrometheusQueryToSQL::Converter converter{config.promql_query, config.evaluation_settings, std::move(native_fragments)};
    ASTPtr select_query = converter.getSQL();

    LOG_INFO(log, "Will execute query:\n{}", select_query->formatForLogging());
    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    InterpreterSelectQueryAnalyzer interpreter(select_query, query_context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
