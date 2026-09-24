#include <Storages/TimeSeries/PromQLNativePlanBuilder.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/NullsAction.h>
#include <Parsers/Prometheus/PrometheusQueryClassifier.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/PromQLRangeRateStep.h>
#include <Processors/QueryPlan/PromQLRangeSumByStep.h>
#include <Processors/QueryPlan/PromQLRangeTopKByStep.h>
#include <Processors/QueryPlan/PromQLTwoRangeRatesStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageTimeSeriesSelector.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <Storages/TimeSeries/getPromQLResultTimestampType.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Common/re2.h>

#include <base/arithmeticOverflow.h>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsBool enable_promql_native_parallel_processing;
extern const SettingsBool enable_promql_native_raw_samples;
extern const SettingsBool enable_promql_native_storage_fusion;
extern const SettingsUInt64 max_promql_native_parallel_lanes;
extern const SettingsUInt64 max_promql_native_rate_samples_per_series;
extern const SettingsUInt64 max_promql_native_rate_series;
extern const SettingsUInt64 max_promql_native_vector_grid_cells;
}

namespace
{

constexpr auto nonempty_filter_column = "__promql_native_nonempty";

bool hasCompatibleEvaluationSettings(const PrometheusQueryEvaluationSettings & evaluation_settings)
{
    return !evaluation_settings.use_current_time && evaluation_settings.start_time && evaluation_settings.end_time
        && evaluation_settings.step && evaluation_settings.step->value > 0 && evaluation_settings.table_timestamp_type;
}

StorageTimeSeriesSelector::Configuration makeRangeSelectorConfiguration(
    ContextPtr context,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    const PrometheusQueryTree::MatcherList & matchers,
    PrometheusQueryTree::DurationType window)
{
    auto time_series_storage
        = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(evaluation_settings.time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);

    auto tags_target = time_series_storage->getTargetTable(ViewTarget::Tags, context);
    auto tags_metadata = tags_target->getInMemoryMetadataPtr(context, false);
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(context, false);
    auto [table_timestamp_type, table_value_type] = splitTimeSeriesType(
        time_series_metadata->columns.get(TimeSeriesColumnNames::getOuterSamples(time_series_storage->getVersion())).type);

    auto selector_node = std::make_unique<PrometheusQueryTree::InstantSelector>();
    selector_node->matchers = matchers;

    StorageTimeSeriesSelector::Configuration selector_config;
    selector_config.time_series_storage_id = evaluation_settings.time_series_storage_id;
    selector_config.time_series_version = time_series_storage->getVersion();
    selector_config.table_id_type = tags_metadata->columns.get(TimeSeriesColumnNames::ID).type;
    selector_config.table_timestamp_type = std::move(table_timestamp_type);
    selector_config.table_value_type = std::move(table_value_type);
    selector_config.selector = PrometheusQueryTree(std::move(selector_node), promql_query.getTimeScale());
    selector_config.time_scale = evaluation_settings.time_scale;
    selector_config.min_time = *evaluation_settings.start_time - window + 1;
    selector_config.max_time = *evaluation_settings.end_time;
    return selector_config;
}

std::shared_ptr<StorageTimeSeriesSelector> makeRangeSelectorStorage(
    const StorageTimeSeriesSelector::Configuration & selector_config, StorageTimeSeriesSelector::SamplesReadMode samples_read_mode)
{
    auto time_series_data_type = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{selector_config.table_timestamp_type, selector_config.table_value_type}));
    ColumnsDescription selector_columns({
        {TimeSeriesColumnNames::ID, selector_config.table_id_type},
        {TimeSeriesColumnNames::Bucket, selector_config.table_timestamp_type},
        {samples_read_mode == StorageTimeSeriesSelector::SamplesReadMode::Raw ? TimeSeriesColumnNames::Samples
                                                                              : TimeSeriesColumnNames::TimeSeries,
         time_series_data_type},
    });

    return std::make_shared<StorageTimeSeriesSelector>(StorageID{"", "_promql_native_selector"}, selector_columns, selector_config);
}

bool tryBuildPromQLRangeSelectorPlan(
    QueryPlan & selector_plan,
    ContextPtr & selector_context,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    const PrometheusQueryTree::MatcherList & matchers,
    PrometheusQueryTree::DurationType window,
    bool enable_whole_metric_id_range_optimization,
    StorageTimeSeriesSelector::SamplesReadMode samples_read_mode = StorageTimeSeriesSelector::SamplesReadMode::Sliced,
    const Names & exact_metric_names_for_whole_metric_id_range = {})
{
    auto selector_config = makeRangeSelectorConfiguration(context, promql_query, evaluation_settings, matchers, window);
    auto selector_storage = makeRangeSelectorStorage(selector_config, samples_read_mode);

    selector_storage->startup();
    auto selector_metadata = selector_storage->getInMemoryMetadataPtr(context, false);
    auto selector_snapshot = selector_storage->getStorageSnapshot(selector_metadata, context);

    /// The selector's tags subquery is an execution dependency: besides producing the id set it
    /// populates the query-local id-to-tags collector used by the native kernel. Disable the only
    /// cache which can skip execution of that subquery while still returning a prepared set.
    auto mutable_selector_context = Context::createCopy(context);
    mutable_selector_context->setPreparedSetsCache(nullptr);
    mutable_selector_context->setSetting("max_block_size", UInt64{max_block_size});
    selector_context = std::move(mutable_selector_context);

    const char * samples_column_name = samples_read_mode == StorageTimeSeriesSelector::SamplesReadMode::Raw
        ? TimeSeriesColumnNames::Samples
        : TimeSeriesColumnNames::TimeSeries;
    if (!selector_storage->buildQueryPlan(
            selector_plan,
            Names{TimeSeriesColumnNames::ID, TimeSeriesColumnNames::Bucket, samples_column_name},
            selector_snapshot,
            query_info,
            selector_context,
            processed_stage,
            max_block_size,
            num_streams,
            StorageTimeSeriesSelector::SamplesReadOrder::IdBucket,
            enable_whole_metric_id_range_optimization,
            samples_read_mode,
            exact_metric_names_for_whole_metric_id_range))
        return false;

    selector_plan.addStorageHolder(selector_storage);
    selector_plan.addInterpreterContext(selector_context);
    return true;
}

bool materializeIdentifierSet(
    QueryPlan & selector_plan, const ContextPtr & selector_context, const BuiltSetsByHashPtr & prepared_identifier_sets)
{
    /// Hybrid admission materializes the exact selector set once. The fragment must adopt that
    /// same ready SetAndKey, so duplicate-tag validation and sample reads use one selector result.
    if (prepared_identifier_sets)
        reuseBuiltSets(selector_plan, prepared_identifier_sets);

    size_t identifier_set_count = 0;
    bool identifier_set_ready = true;
    forEachSubquerySet(
        &selector_plan,
        [&](FutureSetFromSubquery & future_set)
        {
            ++identifier_set_count;
            if (!prepared_identifier_sets)
                future_set.buildSetInplace(selector_context);
            else
            {
                const auto it = prepared_identifier_sets->sets.find(future_set.getHash());
                if (it == prepared_identifier_sets->sets.end() || future_set.getSetAndKey() != it->second)
                    identifier_set_ready = false;
            }
            if (!future_set.get())
                identifier_set_ready = false;
            return false;
        });

    if (identifier_set_count != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected exactly one identifier set in the native PromQL selector plan, found {}",
            identifier_set_count);

    return identifier_set_ready;
}

bool hasUniqueIdentifiersPerFullTagSet(const ContextPtr & context)
{
    return !context->getQueryContext()->getTimeSeriesTagsCollector()->hasMultipleIdentifiersForSameTags();
}

AggregateFunctionPtr makeRangeRateFunction(
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    PrometheusQueryTree::DurationType window,
    const Block & input_header,
    bool reads_raw_samples = false)
{
    const UInt32 timestamp_scale = evaluation_settings.time_scale;
    Array rate_parameters{
        DecimalField<DateTime64>(*evaluation_settings.start_time, timestamp_scale),
        DecimalField<DateTime64>(*evaluation_settings.end_time, timestamp_scale),
        DecimalField<Decimal64>(*evaluation_settings.step, timestamp_scale),
        DecimalField<Decimal64>(window, timestamp_scale),
    };

    AggregateFunctionProperties rate_properties;
    DataTypes rate_arguments;
    if (reads_raw_samples)
    {
        const auto & samples_type = input_header.getByName(TimeSeriesColumnNames::Samples).type;
        const auto * array_type = typeid_cast<const DataTypeArray *>(samples_type.get());
        const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
        if (!tuple_type || tuple_type->getElements().size() != 2)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected {} to be Array(Tuple(timestamp, value)), got {}",
                TimeSeriesColumnNames::Samples,
                samples_type->getName());
        rate_arguments = tuple_type->getElements();
    }
    else
        rate_arguments = DataTypes{input_header.getByName(TimeSeriesColumnNames::TimeSeries).type};
    return AggregateFunctionFactory::instance().get(
        "timeSeriesRateToGrid", NullsAction::EMPTY, rate_arguments, rate_parameters, rate_properties);
}

PrometheusQueryTree::MatcherList makeTwoRangeRatesUnionMatchers(const PromQLTwoRangeRatesQuery & query)
{
    auto matchers = query.rates[0].common_matchers;
    matchers.push_back(
        PrometheusQueryTree::Matcher{
            .label_name = TimeSeriesTagNames::MetricName,
            .label_value = fmt::format("(?:{}|{})", RE2::QuoteMeta(query.rates[0].metric_name), RE2::QuoteMeta(query.rates[1].metric_name)),
            .matcher_type = PrometheusQueryTree::MatcherType::RE,
        });
    return matchers;
}

bool tryBuildPromQLRangeSumByPlan(
    QueryPlan & native_plan,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    PromQLRangeSumByQuery range_sum_query,
    size_t max_output_groups,
    BuiltSetsByHashPtr prepared_identifier_sets)
{
    ContextPtr selector_context;
    if (!tryBuildPromQLRangeSelectorPlan(
            native_plan,
            selector_context,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            range_sum_query.matchers,
            range_sum_query.window,
            /* enable_whole_metric_id_range_optimization = */ true))
        return false;

    if (!materializeIdentifierSet(native_plan, selector_context, prepared_identifier_sets) || !hasUniqueIdentifiersPerFullTagSet(context))
        return false;

    auto rate_function = makeRangeRateFunction(
        evaluation_settings, range_sum_query.window, *native_plan.getCurrentHeader());

    AggregateFunctionProperties sum_properties;
    auto sum_function = AggregateFunctionFactory::instance().get(
        "sumForEach", NullsAction::EMPTY, DataTypes{rate_function->getResultType()}, {}, sum_properties);

    const auto max_samples_per_series = context->getSettingsRef()[Setting::max_promql_native_rate_samples_per_series];
    if (!max_samples_per_series)
        return false;

    native_plan.addStep(
        std::make_unique<PromQLRangeSumByStep>(
            native_plan.getCurrentHeader(),
            context->getQueryContext()->getTimeSeriesTagsCollector(),
            rate_function,
            sum_function,
            std::move(range_sum_query.labels_to_keep),
            max_samples_per_series,
            max_output_groups,
            max_block_size,
            context->getSettingsRef()[Setting::enable_promql_native_parallel_processing],
            context->getSettingsRef()[Setting::max_promql_native_parallel_lanes]));

    return true;
}

bool tryBuildPromQLRangeRatePlan(
    QueryPlan & native_plan,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    const PromQLRangeRateQuery & range_rate_query,
    BuiltSetsByHashPtr prepared_identifier_sets)
{
    const bool reads_raw_samples = context->getSettingsRef()[Setting::enable_promql_native_raw_samples];
    ContextPtr selector_context;
    if (!tryBuildPromQLRangeSelectorPlan(
            native_plan,
            selector_context,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            range_rate_query.matchers,
            range_rate_query.window,
            /* enable_whole_metric_id_range_optimization = */ true,
            reads_raw_samples ? StorageTimeSeriesSelector::SamplesReadMode::Raw : StorageTimeSeriesSelector::SamplesReadMode::Sliced))
        return false;

    if (!materializeIdentifierSet(native_plan, selector_context, prepared_identifier_sets) || !hasUniqueIdentifiersPerFullTagSet(context))
        return false;

    const auto max_samples_per_series = context->getSettingsRef()[Setting::max_promql_native_rate_samples_per_series];
    if (!max_samples_per_series)
        return false;

    std::optional<Field> raw_min_time;
    std::optional<Field> raw_max_time;
    if (reads_raw_samples)
    {
        auto result_timestamp_type = getPromQLResultTimestampType(evaluation_settings.time_scale, evaluation_settings.time_zone);
        const auto & table_timestamp_type = native_plan.getCurrentHeader()->getByName(TimeSeriesColumnNames::Samples).type;
        const auto & tuple_type = typeid_cast<const DataTypeTuple &>(
            *typeid_cast<const DataTypeArray &>(*table_timestamp_type).getNestedType());
        raw_min_time = convertFieldToType(
            DecimalField<DateTime64>(*evaluation_settings.start_time - range_rate_query.window + 1, evaluation_settings.time_scale),
            *tuple_type.getElement(0),
            result_timestamp_type.get());
        raw_max_time = convertFieldToType(
            DecimalField<DateTime64>(*evaluation_settings.end_time, evaluation_settings.time_scale),
            *tuple_type.getElement(0),
            result_timestamp_type.get());
    }

    native_plan.addStep(
        std::make_unique<PromQLRangeRateStep>(
            native_plan.getCurrentHeader(),
            context->getQueryContext()->getTimeSeriesTagsCollector(),
            makeRangeRateFunction(
                evaluation_settings, range_rate_query.window, *native_plan.getCurrentHeader(), reads_raw_samples),
            max_samples_per_series,
            max_block_size,
            context->getSettingsRef()[Setting::enable_promql_native_parallel_processing],
            context->getSettingsRef()[Setting::max_promql_native_parallel_lanes],
            std::move(raw_min_time),
            std::move(raw_max_time)));
    return true;
}

bool tryBuildPromQLTwoRangeRatesPlan(
    QueryPlan & native_plan,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    const PromQLTwoRangeRatesQuery & two_rates_query,
    BuiltSetsByHashPtr prepared_identifier_sets)
{
    const bool reads_raw_samples = context->getSettingsRef()[Setting::enable_promql_native_raw_samples];
    ContextPtr selector_context;
    if (!tryBuildPromQLRangeSelectorPlan(
            native_plan,
            selector_context,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            makeTwoRangeRatesUnionMatchers(two_rates_query),
            two_rates_query.rates[0].window,
            /* enable_whole_metric_id_range_optimization = */ true,
            reads_raw_samples ? StorageTimeSeriesSelector::SamplesReadMode::Raw : StorageTimeSeriesSelector::SamplesReadMode::Sliced,
            Names{two_rates_query.rates[0].metric_name, two_rates_query.rates[1].metric_name}))
        return false;

    if (!materializeIdentifierSet(native_plan, selector_context, prepared_identifier_sets) || !hasUniqueIdentifiersPerFullTagSet(context))
        return false;

    const auto max_samples_per_series = context->getSettingsRef()[Setting::max_promql_native_rate_samples_per_series];
    const auto max_join_groups = context->getSettingsRef()[Setting::max_promql_native_rate_series];
    const auto max_grid_cells = context->getSettingsRef()[Setting::max_promql_native_vector_grid_cells];
    if (!max_samples_per_series || !max_join_groups || !max_grid_cells)
        return false;

    std::optional<Field> raw_min_time;
    std::optional<Field> raw_max_time;
    if (reads_raw_samples)
    {
        auto result_timestamp_type = getPromQLResultTimestampType(evaluation_settings.time_scale, evaluation_settings.time_zone);
        const auto & table_timestamp_type = native_plan.getCurrentHeader()->getByName(TimeSeriesColumnNames::Samples).type;
        const auto & tuple_type = typeid_cast<const DataTypeTuple &>(
            *typeid_cast<const DataTypeArray &>(*table_timestamp_type).getNestedType());
        raw_min_time = convertFieldToType(
            DecimalField<DateTime64>(*evaluation_settings.start_time - two_rates_query.rates[0].window + 1, evaluation_settings.time_scale),
            *tuple_type.getElement(0),
            result_timestamp_type.get());
        raw_max_time = convertFieldToType(
            DecimalField<DateTime64>(*evaluation_settings.end_time, evaluation_settings.time_scale),
            *tuple_type.getElement(0),
            result_timestamp_type.get());
    }

    native_plan.addStep(
        std::make_unique<PromQLTwoRangeRatesStep>(
            native_plan.getCurrentHeader(),
            context->getQueryContext()->getTimeSeriesTagsCollector(),
            makeRangeRateFunction(
                evaluation_settings, two_rates_query.rates[0].window, *native_plan.getCurrentHeader(), reads_raw_samples),
            two_rates_query.rates[0].metric_name,
            two_rates_query.rates[1].metric_name,
            max_samples_per_series,
            max_block_size,
            max_join_groups,
            max_grid_cells,
            context->getSettingsRef()[Setting::enable_promql_native_parallel_processing],
            context->getSettingsRef()[Setting::max_promql_native_parallel_lanes],
            std::move(raw_min_time),
            std::move(raw_max_time),
            context->getSettingsRef()[Setting::enable_promql_native_storage_fusion]));
    return true;
}

const ActionsDAG::Node & addNullableValues(ActionsDAG & dag, ContextPtr context)
{
    const auto & values = dag.findInOutputs(TimeSeriesColumnNames::Values);
    const auto nullable_scalar_type = makeNullable(std::make_shared<DataTypeFloat64>());
    const auto nullable_values_type = std::make_shared<DataTypeArray>(nullable_scalar_type);
    return dag.addCast(values, nullable_values_type, "__promql_native_nullable_values", context);
}

void convertToTargetHeader(QueryPlan & plan, const Names & column_names, const StorageSnapshotPtr & storage_snapshot, ContextPtr context)
{
    const auto target_header = storage_snapshot->getSampleBlockForColumns(column_names);
    auto convert_actions = ActionsDAG::makeConvertingActions(
        plan.getCurrentHeader()->getColumnsWithTypeAndName(),
        target_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);
    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(convert_actions)));
}

}

bool canBuildPromQLNativeVectorGridPlan(
    const PrometheusQueryTree & promql_query, const PrometheusQueryEvaluationSettings & evaluation_settings, ContextPtr context)
{
    auto range_sum_query
        = extractPromQLRangeSumByQuery(promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    auto range_rate_query
        = extractPromQLRangeRateQuery(promql_query.getRoot(), evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    auto two_rates_query
        = extractPromQLTwoRangeRatesQuery(promql_query.getRoot(), evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    if ((!range_sum_query && !range_rate_query && !two_rates_query) || !hasCompatibleEvaluationSettings(evaluation_settings))
        return false;
    if (!context->getSettingsRef()[Setting::max_promql_native_rate_series]
        || !context->getSettingsRef()[Setting::max_promql_native_rate_samples_per_series])
        return false;
    if (two_rates_query && !context->getSettingsRef()[Setting::max_promql_native_vector_grid_cells])
        return false;

    auto time_series_storage
        = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(evaluation_settings.time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);

    const auto can_read_target_in_order = [&](ViewTarget::Kind target_kind)
    {
        auto target = time_series_storage->tryGetTargetTable(target_kind, context);
        if (!target)
            return target_kind == ViewTarget::RecentSamples;
        auto target_metadata = target->getInMemoryMetadataPtr(context, false);
        return StorageTimeSeriesSelector::canReadSamplesInOrder(target, target_metadata);
    };

    /// A recent-samples read can cross its TTL admission boundary between
    /// fragment admission and pipeline construction. Require both possible
    /// targets to preserve `(id, bucket)` so a valid query always falls back
    /// to SQL before the external native fragment is installed.
    return can_read_target_in_order(ViewTarget::Samples) && can_read_target_in_order(ViewTarget::RecentSamples);
}

std::optional<PromQLNativeVectorGridPreparation> tryPreparePromQLNativeVectorGridPlan(
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings)
{
    if (!canBuildPromQLNativeVectorGridPlan(promql_query, evaluation_settings, context) || !hasUniqueIdentifiersPerFullTagSet(context))
        return {};

    auto range_sum_query
        = extractPromQLRangeSumByQuery(promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    auto range_rate_query
        = extractPromQLRangeRateQuery(promql_query.getRoot(), evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    auto two_rates_query
        = extractPromQLTwoRangeRatesQuery(promql_query.getRoot(), evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    chassert(range_sum_query || range_rate_query || two_rates_query);

    QueryPlan selector_plan;
    ContextPtr selector_context;
    auto two_rates_matchers = two_rates_query ? makeTwoRangeRatesUnionMatchers(*two_rates_query) : PrometheusQueryTree::MatcherList{};
    const auto & matchers
        = range_sum_query ? range_sum_query->matchers : (range_rate_query ? range_rate_query->matchers : two_rates_matchers);
    const auto window
        = range_sum_query ? range_sum_query->window : (range_rate_query ? range_rate_query->window : two_rates_query->rates[0].window);
    if (!tryBuildPromQLRangeSelectorPlan(
            selector_plan,
            selector_context,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            matchers,
            window,
            /* enable_whole_metric_id_range_optimization = */ false))
        return {};

    if (!materializeIdentifierSet(selector_plan, selector_context, nullptr) || !hasUniqueIdentifiersPerFullTagSet(context))
        return {};

    auto built_sets = collectBuiltSets(selector_plan);
    if (built_sets->sets.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected one materialized identifier set for a native PromQL fragment, found {}",
            built_sets->sets.size());
    const auto & set_and_key = built_sets->sets.begin()->second;
    if (!set_and_key || !set_and_key->set)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The materialized native PromQL identifier set is missing");

    const size_t selected_series = set_and_key->set->getTotalRowCount();
    if (selected_series > context->getSettingsRef()[Setting::max_promql_native_rate_series])
        return {};

    const auto max_grid_cells = context->getSettingsRef()[Setting::max_promql_native_vector_grid_cells];
    if (max_grid_cells.value)
    {
        const size_t evaluation_points = PrometheusQueryToSQL::stepsInTimeSeriesRange(
            *evaluation_settings.start_time, *evaluation_settings.end_time, *evaluation_settings.step);
        UInt64 grid_cells = 0;
        if (common::mulOverflow(static_cast<UInt64>(selected_series), static_cast<UInt64>(evaluation_points), grid_cells)
            || grid_cells > max_grid_cells.value)
            return {};
    }

    return PromQLNativeVectorGridPreparation{
        .identifier_sets = std::move(built_sets),
        .selected_series = selected_series,
    };
}

bool tryBuildPromQLNativePlan(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    size_t max_output_groups)
{
    auto native_query = extractPromQLRangeSumByQuery(promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    auto topk_query = extractPromQLRangeTopKByQuery(promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    auto range_rate_query
        = extractPromQLRangeRateQuery(promql_query.getRoot(), evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    if ((!native_query && !topk_query && !range_rate_query) || !hasCompatibleEvaluationSettings(evaluation_settings))
        return false;

    auto preparation = tryPreparePromQLNativeVectorGridPlan(
        query_info,
        context,
        processed_stage,
        max_block_size,
        num_streams,
        promql_query,
        evaluation_settings);
    if (!preparation)
        return false;

    QueryPlan native_plan;
    if (range_rate_query)
    {
        if (!tryBuildPromQLRangeRatePlan(
                native_plan,
                query_info,
                context,
                processed_stage,
                max_block_size,
                num_streams,
                promql_query,
                evaluation_settings,
                *range_rate_query,
                std::move(preparation->identifier_sets)))
            return false;
    }
    else
    {
        auto range_sum_query = topk_query ? topk_query->range_sum : *native_query;
        if (!tryBuildPromQLRangeSumByPlan(
                native_plan,
                query_info,
                context,
                processed_stage,
                max_block_size,
                num_streams,
                promql_query,
                evaluation_settings,
                std::move(range_sum_query),
                max_output_groups,
                std::move(preparation->identifier_sets)))
            return false;
    }

    if (topk_query)
        native_plan.addStep(
            std::make_unique<PromQLRangeTopKByStep>(
                native_plan.getCurrentHeader(), topk_query->k, topk_query->bottomk, max_block_size));

    const UInt32 timestamp_scale = evaluation_settings.time_scale;
    auto result_timestamp_type = getPromQLResultTimestampType(evaluation_settings.time_scale, evaluation_settings.time_zone);
    ActionsDAG finalize_dag(native_plan.getCurrentHeader()->getColumnsWithTypeAndName());
    const auto & group = finalize_dag.findInOutputs(TimeSeriesColumnNames::Group);
    const auto & nullable_values = addNullableValues(finalize_dag, context);
    const auto & tags = finalize_dag.addFunction(
        FunctionFactory::instance().get("timeSeriesGroupToTags", context), {&group}, TimeSeriesColumnNames::Tags);
    const auto & start = finalize_dag.addColumn(
        result_timestamp_type->createColumnConst(
            0, DecimalField<DateTime64>(*evaluation_settings.start_time, timestamp_scale)),
        result_timestamp_type,
        "__promql_native_start");
    const auto & end = finalize_dag.addColumn(
        result_timestamp_type->createColumnConst(
            0, DecimalField<DateTime64>(*evaluation_settings.end_time, timestamp_scale)),
        result_timestamp_type,
        "__promql_native_end");
    auto duration_type = std::make_shared<DataTypeDecimal<Decimal64>>(DataTypeDecimal<Decimal64>::maxPrecision(), timestamp_scale);
    const auto & step = finalize_dag.addColumn(
        duration_type->createColumnConst(0, DecimalField<Decimal64>(*evaluation_settings.step, timestamp_scale)),
        duration_type,
        "__promql_native_step");
    const auto * samples_column_name = TimeSeriesColumnNames::getOuterSamples(evaluation_settings.time_series_version);
    const auto & time_series = finalize_dag.addFunction(
        FunctionFactory::instance().get("timeSeriesFromGrid", context),
        {&start, &end, &step, &nullable_values},
        samples_column_name);
    finalize_dag.getOutputs() = {&tags, &time_series};
    native_plan.addStep(std::make_unique<ExpressionStep>(native_plan.getCurrentHeader(), std::move(finalize_dag)));

    ActionsDAG filter_dag(native_plan.getCurrentHeader()->getColumnsWithTypeAndName());
    const auto & filter_time_series = filter_dag.findInOutputs(samples_column_name);
    const auto & nonempty
        = filter_dag.addFunction(FunctionFactory::instance().get("notEmpty", context), {&filter_time_series}, nonempty_filter_column);
    filter_dag.getOutputs().push_back(&nonempty);
    native_plan.addStep(std::make_unique<FilterStep>(native_plan.getCurrentHeader(), std::move(filter_dag), nonempty_filter_column, true));

    SortDescription sort_description;
    sort_description.emplace_back(TimeSeriesColumnNames::Tags, 1, 1);
    auto sort_settings = SortingStep::Settings(context->getSettingsRef());
    sort_settings.max_block_size = max_block_size;
    native_plan.addStep(
        std::make_unique<SortingStep>(
            native_plan.getCurrentHeader(), std::move(sort_description), 0, std::move(sort_settings)));

    convertToTargetHeader(native_plan, column_names, storage_snapshot, context);

    query_plan = std::move(native_plan);
    return true;
}

bool tryBuildPromQLNativeVectorGridPlan(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    size_t max_output_groups,
    BuiltSetsByHashPtr prepared_identifier_sets)
{
    if (!canBuildPromQLNativeVectorGridPlan(promql_query, evaluation_settings, context))
        return false;

    auto range_sum_query
        = extractPromQLRangeSumByQuery(promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    auto range_rate_query
        = extractPromQLRangeRateQuery(promql_query.getRoot(), evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    auto two_rates_query
        = extractPromQLTwoRangeRatesQuery(promql_query.getRoot(), evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    chassert(range_sum_query || range_rate_query || two_rates_query);

    QueryPlan native_plan;
    bool built = false;
    if (range_sum_query)
    {
        built = tryBuildPromQLRangeSumByPlan(
            native_plan,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            std::move(*range_sum_query),
            max_output_groups,
            std::move(prepared_identifier_sets));
    }
    else if (range_rate_query)
    {
        built = tryBuildPromQLRangeRatePlan(
            native_plan,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            *range_rate_query,
            std::move(prepared_identifier_sets));
    }
    else
    {
        built = tryBuildPromQLTwoRangeRatesPlan(
            native_plan,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            *two_rates_query,
            std::move(prepared_identifier_sets));
    }
    if (!built)
        return false;

    ActionsDAG output_dag(native_plan.getCurrentHeader()->getColumnsWithTypeAndName());
    const auto & group = output_dag.findInOutputs(TimeSeriesColumnNames::Group);
    const auto & nullable_values = addNullableValues(output_dag, context);
    const auto & output_values = output_dag.addAlias(nullable_values, TimeSeriesColumnNames::Values);
    output_dag.getOutputs() = {&group, &output_values};
    native_plan.addStep(std::make_unique<ExpressionStep>(native_plan.getCurrentHeader(), std::move(output_dag)));

    convertToTargetHeader(native_plan, column_names, storage_snapshot, context);
    query_plan = std::move(native_plan);
    return true;
}

}
