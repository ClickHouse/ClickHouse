#include <Storages/TimeSeries/PromQLNativePlanBuilder.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/NullsAction.h>
#include <Parsers/Prometheus/PrometheusQueryClassifier.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/PromQLRangeTopKByStep.h>
#include <Processors/QueryPlan/PromQLRangeSumByStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageTimeSeriesSelector.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsBool enable_promql_native_parallel_processing;
}

namespace
{

constexpr auto nonempty_filter_column = "__promql_native_nonempty";

bool hasCompatibleEvaluationSettings(const PrometheusQueryEvaluationSettings & evaluation_settings)
{
    return !evaluation_settings.use_current_time && evaluation_settings.start_time && evaluation_settings.end_time
        && evaluation_settings.step && evaluation_settings.step->value > 0
        && isDateTime64(evaluation_settings.timestamp_data_type);
}

StorageTimeSeriesSelector::Configuration makeRangeSumSelectorConfiguration(
    ContextPtr context,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    const PromQLRangeSumByQuery & range_sum_query)
{
    auto time_series_storage = storagePtrToTimeSeries(
        DatabaseCatalog::instance().getTable(evaluation_settings.time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);

    auto tags_target = time_series_storage->getTargetTable(ViewTarget::Tags, context);
    auto tags_metadata = tags_target->getInMemoryMetadataPtr(context, false);

    auto selector_node = std::make_unique<PrometheusQueryTree::InstantSelector>();
    selector_node->matchers = range_sum_query.matchers;

    StorageTimeSeriesSelector::Configuration selector_config;
    selector_config.time_series_storage_id = evaluation_settings.time_series_storage_id;
    selector_config.id_data_type = tags_metadata->columns.get(TimeSeriesColumnNames::ID).type;
    selector_config.timestamp_data_type = evaluation_settings.timestamp_data_type;
    selector_config.scalar_data_type = evaluation_settings.scalar_data_type;
    selector_config.selector = PrometheusQueryTree(std::move(selector_node), promql_query.getTimestampScale());
    selector_config.min_time = *evaluation_settings.start_time - range_sum_query.window + 1;
    selector_config.max_time = *evaluation_settings.end_time;
    return selector_config;
}

std::shared_ptr<StorageTimeSeriesSelector> makeRangeSumSelectorStorage(
    const StorageTimeSeriesSelector::Configuration & selector_config)
{
    auto time_series_data_type = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(
            DataTypes{selector_config.timestamp_data_type, selector_config.scalar_data_type}));
    ColumnsDescription selector_columns({
        {TimeSeriesColumnNames::ID, selector_config.id_data_type},
        {TimeSeriesColumnNames::Bucket, selector_config.timestamp_data_type},
        {TimeSeriesColumnNames::TimeSeries, time_series_data_type},
    });

    return std::make_shared<StorageTimeSeriesSelector>(
        StorageID{"", "_promql_native_selector"}, selector_columns, selector_config);
}

bool tryBuildPromQLRangeSumSelectorPlan(
    QueryPlan & selector_plan,
    ContextPtr & selector_context,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    const PromQLRangeSumByQuery & range_sum_query,
    bool enable_whole_metric_id_range_optimization)
{
    auto selector_config = makeRangeSumSelectorConfiguration(context, promql_query, evaluation_settings, range_sum_query);
    auto selector_storage = makeRangeSumSelectorStorage(selector_config);

    selector_storage->startup();
    auto selector_metadata = selector_storage->getInMemoryMetadataPtr(context, false);
    auto selector_snapshot = selector_storage->getStorageSnapshot(selector_metadata, context);

    /// The selector's tags subquery is an execution dependency: besides producing the id set it
    /// populates the query-local id-to-tags collector used by the native kernel. Disable the only
    /// cache which can skip execution of that subquery while still returning a prepared set.
    auto mutable_selector_context = Context::createCopy(context);
    mutable_selector_context->setPreparedSetsCache(nullptr);
    selector_context = std::move(mutable_selector_context);

    if (!selector_storage->buildQueryPlan(
            selector_plan,
            Names{TimeSeriesColumnNames::ID, TimeSeriesColumnNames::Bucket, TimeSeriesColumnNames::TimeSeries},
            selector_snapshot,
            query_info,
            selector_context,
            processed_stage,
            max_block_size,
            num_streams,
            StorageTimeSeriesSelector::SamplesReadOrder::IdBucket,
            enable_whole_metric_id_range_optimization))
        return false;

    selector_plan.addStorageHolder(selector_storage);
    selector_plan.addInterpreterContext(selector_context);
    return true;
}

bool materializeIdentifierSet(
    QueryPlan & selector_plan,
    const ContextPtr & selector_context,
    const BuiltSetsByHashPtr & prepared_identifier_sets)
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
    if (!tryBuildPromQLRangeSumSelectorPlan(
            native_plan,
            selector_context,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            range_sum_query,
            /* enable_whole_metric_id_range_optimization = */ true))
        return false;

    if (!materializeIdentifierSet(native_plan, selector_context, prepared_identifier_sets)
        || !hasUniqueIdentifiersPerFullTagSet(context))
        return false;

    const UInt32 timestamp_scale = getDecimalScale(*evaluation_settings.timestamp_data_type);
    auto time_series_data_type = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{evaluation_settings.timestamp_data_type, evaluation_settings.scalar_data_type}));
    Array rate_parameters{
        DecimalField<DateTime64>(*evaluation_settings.start_time, timestamp_scale),
        DecimalField<DateTime64>(*evaluation_settings.end_time, timestamp_scale),
        DecimalField<Decimal64>(*evaluation_settings.step, timestamp_scale),
        DecimalField<Decimal64>(range_sum_query.window, timestamp_scale),
    };

    AggregateFunctionProperties rate_properties;
    auto rate_function = AggregateFunctionFactory::instance().get(
        "timeSeriesRateToGrid",
        NullsAction::EMPTY,
        DataTypes{time_series_data_type},
        rate_parameters,
        rate_properties);

    AggregateFunctionProperties sum_properties;
    auto sum_function = AggregateFunctionFactory::instance().get(
        "sumForEach",
        NullsAction::EMPTY,
        DataTypes{rate_function->getResultType()},
        {},
        sum_properties);

    native_plan.addStep(std::make_unique<PromQLRangeSumByStep>(
        native_plan.getCurrentHeader(),
        context->getQueryContext()->getTimeSeriesTagsCollector(),
        rate_function,
        sum_function,
        std::move(range_sum_query.labels_to_keep),
        max_output_groups,
        context->getSettingsRef()[Setting::enable_promql_native_parallel_processing]));

    return true;
}

const ActionsDAG::Node & addNullableValues(
    ActionsDAG & dag,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    ContextPtr context)
{
    const auto & values = dag.findInOutputs(TimeSeriesColumnNames::Values);
    const auto nullable_scalar_type = makeNullable(evaluation_settings.scalar_data_type);
    const auto nullable_values_type = std::make_shared<DataTypeArray>(nullable_scalar_type);
    return dag.addCast(values, nullable_values_type, "__promql_native_nullable_values", context);
}

void convertToTargetHeader(
    QueryPlan & plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr context)
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
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    ContextPtr context)
{
    auto native_query = extractPromQLRangeSumByQuery(
        promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    if (!native_query || !hasCompatibleEvaluationSettings(evaluation_settings))
        return false;

    auto time_series_storage = storagePtrToTimeSeries(
        DatabaseCatalog::instance().getTable(evaluation_settings.time_series_storage_id, context));
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
    return can_read_target_in_order(ViewTarget::Samples)
        && can_read_target_in_order(ViewTarget::RecentSamples);
}

BuiltSetsByHashPtr tryPreparePromQLNativeVectorGridPlan(
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings)
{
    if (!canBuildPromQLNativeVectorGridPlan(promql_query, evaluation_settings, context)
        || !hasUniqueIdentifiersPerFullTagSet(context))
        return nullptr;

    auto native_query = extractPromQLRangeSumByQuery(
        promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    chassert(native_query);

    QueryPlan selector_plan;
    ContextPtr selector_context;
    if (!tryBuildPromQLRangeSumSelectorPlan(
            selector_plan,
            selector_context,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            *native_query,
            /* enable_whole_metric_id_range_optimization = */ false))
        return nullptr;

    if (!materializeIdentifierSet(selector_plan, selector_context, nullptr)
        || !hasUniqueIdentifiersPerFullTagSet(context))
        return nullptr;

    auto built_sets = collectBuiltSets(selector_plan);
    if (built_sets->sets.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected one materialized identifier set for a native PromQL fragment, found {}",
            built_sets->sets.size());
    return built_sets;
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
    auto native_query = extractPromQLRangeSumByQuery(
        promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    auto topk_query = extractPromQLRangeTopKByQuery(
        promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    if ((!native_query && !topk_query) || !hasCompatibleEvaluationSettings(evaluation_settings))
        return false;

    QueryPlan native_plan;
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
            /* prepared_identifier_sets = */ nullptr))
        return false;

    if (topk_query)
        native_plan.addStep(std::make_unique<PromQLRangeTopKByStep>(
            native_plan.getCurrentHeader(), topk_query->k, topk_query->bottomk));

    const UInt32 timestamp_scale = getDecimalScale(*evaluation_settings.timestamp_data_type);
    ActionsDAG finalize_dag(native_plan.getCurrentHeader()->getColumnsWithTypeAndName());
    const auto & group = finalize_dag.findInOutputs(TimeSeriesColumnNames::Group);
    const auto & nullable_values = addNullableValues(finalize_dag, evaluation_settings, context);
    const auto & tags = finalize_dag.addFunction(
        FunctionFactory::instance().get("timeSeriesGroupToTags", context),
        {&group},
        TimeSeriesColumnNames::Tags);
    const auto & start = finalize_dag.addColumn(
        evaluation_settings.timestamp_data_type->createColumnConst(
            0, DecimalField<DateTime64>(*evaluation_settings.start_time, timestamp_scale)),
        evaluation_settings.timestamp_data_type,
        "__promql_native_start");
    const auto & end = finalize_dag.addColumn(
        evaluation_settings.timestamp_data_type->createColumnConst(
            0, DecimalField<DateTime64>(*evaluation_settings.end_time, timestamp_scale)),
        evaluation_settings.timestamp_data_type,
        "__promql_native_end");
    auto duration_type = std::make_shared<DataTypeDecimal<Decimal64>>(DataTypeDecimal<Decimal64>::maxPrecision(), timestamp_scale);
    const auto & step = finalize_dag.addColumn(
        duration_type->createColumnConst(0, DecimalField<Decimal64>(*evaluation_settings.step, timestamp_scale)),
        duration_type,
        "__promql_native_step");
    const auto & time_series = finalize_dag.addFunction(
        FunctionFactory::instance().get("timeSeriesFromGrid", context),
        {&start, &end, &step, &nullable_values},
        TimeSeriesColumnNames::TimeSeries);
    finalize_dag.getOutputs() = {&tags, &time_series};
    native_plan.addStep(std::make_unique<ExpressionStep>(native_plan.getCurrentHeader(), std::move(finalize_dag)));

    ActionsDAG filter_dag(native_plan.getCurrentHeader()->getColumnsWithTypeAndName());
    const auto & filter_time_series = filter_dag.findInOutputs(TimeSeriesColumnNames::TimeSeries);
    const auto & nonempty = filter_dag.addFunction(
        FunctionFactory::instance().get("notEmpty", context),
        {&filter_time_series},
        nonempty_filter_column);
    filter_dag.getOutputs().push_back(&nonempty);
    native_plan.addStep(std::make_unique<FilterStep>(
        native_plan.getCurrentHeader(), std::move(filter_dag), nonempty_filter_column, true));

    SortDescription sort_description;
    sort_description.emplace_back(TimeSeriesColumnNames::Tags, 1, 1);
    native_plan.addStep(std::make_unique<SortingStep>(
        native_plan.getCurrentHeader(),
        std::move(sort_description),
        0,
        SortingStep::Settings(context->getSettingsRef())));

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

    auto native_query = extractPromQLRangeSumByQuery(
        promql_query, evaluation_settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE);
    chassert(native_query);

    QueryPlan native_plan;
    if (!tryBuildPromQLRangeSumByPlan(
            native_plan,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams,
            promql_query,
            evaluation_settings,
            std::move(*native_query),
            max_output_groups,
            std::move(prepared_identifier_sets)))
        return false;

    ActionsDAG output_dag(native_plan.getCurrentHeader()->getColumnsWithTypeAndName());
    const auto & group = output_dag.findInOutputs(TimeSeriesColumnNames::Group);
    const auto & nullable_values = addNullableValues(output_dag, evaluation_settings, context);
    const auto & output_values = output_dag.addAlias(nullable_values, TimeSeriesColumnNames::Values);
    output_dag.getOutputs() = {&group, &output_values};
    native_plan.addStep(std::make_unique<ExpressionStep>(native_plan.getCurrentHeader(), std::move(output_dag)));

    convertToTargetHeader(native_plan, column_names, storage_snapshot, context);
    query_plan = std::move(native_plan);
    return true;
}

}
