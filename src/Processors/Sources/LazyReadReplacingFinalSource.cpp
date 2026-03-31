#include <string_view>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Sources/LazyReadReplacingFinalSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <fmt/format.h>

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 aggregation_memory_efficient_merge_threads;
extern const SettingsBool enable_software_prefetch_in_aggregation;
extern const SettingsUInt64 group_by_two_level_threshold;
extern const SettingsUInt64 group_by_two_level_threshold_bytes;
extern const SettingsNonZeroUInt64 max_block_size;
extern const SettingsMaxThreads max_threads;
extern const SettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
}

LazyReadReplacingFinalSource::LazyReadReplacingFinalSource(
    StorageMetadataPtr metadata_snapshot_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageSnapshotPtr storage_snapshot_,
    MergeTreeSettingsPtr data_settings_,
    const MergeTreeData & data_,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
    RangesInDataPartsPtr ranges_,
    ContextPtr query_context_)
    : IProcessor({}, {Block({ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "__global_row_index"}})})
    , metadata_snapshot(std::move(metadata_snapshot_))
    , mutations_snapshot(std::move(mutations_snapshot_))
    , storage_snapshot(std::move(storage_snapshot_))
    , data_settings(std::move(data_settings_))
    , data(data_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , ranges(std::move(ranges_))
    , query_context(std::move(query_context_))
{
}

IProcessor::Status LazyReadReplacingFinalSource::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::NeedData;

    if (inputs.empty())
    {
        if (processors.empty())
            return Status::Ready;
        else
            return Status::ExpandPipeline;
    }

    /// Forward chunks
    auto & input = inputs.front();
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    auto chunk = input.pull();
    output.push(std::move(chunk));
    return Status::NeedData;
}

static void calculateGlobalOffset(ActionsDAG & dag, ReadFromMergeTree & reading_step)
{
    bool added_part_starting_offset;
    bool added_part_offset;
    reading_step.addStartingPartOffsetAndPartOffset(added_part_starting_offset, added_part_offset);
    DataTypePtr uint64_type = std::make_shared<DataTypeUInt64>();
    const auto * part_starting_offset_in = &dag.addInput("_part_starting_offset", uint64_type);
    const auto * part_offset_in = &dag.addInput("_part_offset", uint64_type);

    auto plus = FunctionFactory::instance().get("plus", nullptr);
    const auto * global_offset_node = &dag.addFunction(plus, {part_starting_offset_in, part_offset_in}, {});
    global_offset_node = &dag.addAlias(*global_offset_node, "__global_row_index");

    dag.getOutputs().push_back(global_offset_node);

    /// Remove virtual columns if they were not initially needed.
    if (!added_part_starting_offset)
        dag.getOutputs().push_back(part_starting_offset_in);
    if (!added_part_offset)
        dag.getOutputs().push_back(part_offset_in);
}

void LazyReadReplacingFinalSource::work()
{
    const auto & settings = query_context->getSettingsRef();
    const auto & sorting_key = metadata_snapshot->getSortingKey();
    const auto & merging_params = data.merging_params;

    QueryPlan plan;

    {
        Names all_column_names;
        // all_column_names.push_back("_part_starting_offset");
        // all_column_names.push_back("_part_offset");

        std::unordered_set<std::string_view> columns_to_read;
        for (const auto & column : sorting_key.expression->getRequiredColumnsWithTypes())
        {
            columns_to_read.insert(column.name);
            all_column_names.push_back(column.name);
        }

        if (!merging_params.version_column.empty() && !columns_to_read.contains(merging_params.version_column))
        {
            columns_to_read.insert(merging_params.version_column);
            all_column_names.push_back(merging_params.version_column);
        }
        if (!merging_params.is_deleted_column.empty() && !columns_to_read.contains(merging_params.is_deleted_column))
        {
            columns_to_read.insert(merging_params.is_deleted_column);
            all_column_names.push_back(merging_params.is_deleted_column);
        }

        SelectQueryInfo query_info;
        query_info.table_expression_modifiers = TableExpressionModifiers(false, {}, {});

        auto reading = std::make_unique<ReadFromMergeTree>(
            ranges,
            mutations_snapshot,
            all_column_names,
            data,
            data_settings,
            query_info,
            storage_snapshot,
            query_context,
            settings[Setting::max_block_size],
            settings[Setting::max_threads],
            max_block_numbers_to_read,
            getLogger("LazyReadReplacingFinalSource"),
            nullptr,
            false);

        ActionsDAG dag = sorting_key.expression->getActionsDAG().clone();
        calculateGlobalOffset(dag, *reading);

        plan.addStep(std::move(reading));
        auto expression = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag));
        plan.addStep(std::move(expression));
    }

    {
        const auto & header = plan.getCurrentHeader();
        AggregateFunctionProperties properties;
        AggregateDescriptions aggregates;

        auto version_type = header->getByName(merging_params.version_column).type;
        String argmax_row_index_name = fmt::format("argMax(__global_row_index, {})", merging_params.version_column);
        String argmax_is_deleted_name;

        /// argMax(__global_row_index, version)
        {
            auto uint64_type = std::make_shared<DataTypeUInt64>();

            AggregateDescription desc;
            desc.function
                = AggregateFunctionFactory::instance().get("argMax", NullsAction::EMPTY, {uint64_type, version_type}, {}, properties);
            desc.argument_names = {"__global_row_index", merging_params.version_column};
            desc.column_name = argmax_row_index_name;
            aggregates.push_back(std::move(desc));
        }

        /// argMax(is_deleted, version) if is_deleted column exists
        if (!merging_params.is_deleted_column.empty())
        {
            argmax_is_deleted_name = fmt::format("argMax({}, {})", merging_params.is_deleted_column, merging_params.version_column);
            auto is_deleted_type = header->getByName(merging_params.is_deleted_column).type;

            AggregateDescription desc;
            desc.function
                = AggregateFunctionFactory::instance().get("argMax", NullsAction::EMPTY, {is_deleted_type, version_type}, {}, properties);
            desc.argument_names = {merging_params.is_deleted_column, merging_params.version_column};
            desc.column_name = argmax_is_deleted_name;
            aggregates.push_back(std::move(desc));
        }

        Aggregator::Params params(
            sorting_key.column_names,
            aggregates,
            /*overflow_row_=*/false,
            /*max_rows_to_group_by_=*/0,
            /*group_by_overflow_mode_=*/OverflowMode::THROW,
            /*group_by_two_level_threshold_=*/settings[Setting::group_by_two_level_threshold],
            /*group_by_two_level_threshold_bytes_=*/settings[Setting::group_by_two_level_threshold_bytes],
            /*max_bytes_before_external_group_by_=*/0,
            /*empty_result_for_aggregation_by_empty_set_=*/true,
            /*tmp_data_scope_=*/nullptr,
            /*max_threads_=*/settings[Setting::max_threads],
            /*min_free_disk_space_=*/0,
            /*compile_aggregate_expressions_=*/false,
            /*min_count_to_compile_aggregate_expression_=*/0,
            /*max_block_size_=*/settings[Setting::max_block_size],
            /*enable_prefetch_=*/settings[Setting::enable_software_prefetch_in_aggregation],
            /*only_merge_=*/false,
            /*optimize_group_by_constant_keys_=*/false,
            /*min_hit_rate_to_use_consecutive_keys_optimization_=*/settings[Setting::min_hit_rate_to_use_consecutive_keys_optimization],
            /*stats_collecting_params_=*/{},
            /*enable_producing_buckets_out_of_order_in_aggregation_=*/false,
            /*serialize_string_with_zero_byte_=*/false);

        auto merge_threads = settings[Setting::max_threads];
        auto temporary_data_merge_threads = settings[Setting::aggregation_memory_efficient_merge_threads]
            ? static_cast<size_t>(settings[Setting::aggregation_memory_efficient_merge_threads])
            : static_cast<size_t>(settings[Setting::max_threads]);

        auto aggregating_step = std::make_unique<AggregatingStep>(
            plan.getCurrentHeader(),
            std::move(params),
            /*grouping_sets_params_=*/GroupingSetsParamsList{},
            /*final_=*/true,
            /*max_block_size_=*/settings[Setting::max_block_size],
            /*aggregation_in_order_max_block_bytes_=*/size_t(0),
            /*merge_threads_=*/merge_threads,
            /*temporary_data_merge_threads_=*/temporary_data_merge_threads,
            /*storage_has_evenly_distributed_read_=*/false,
            /*group_by_use_nulls_=*/false,
            /*sort_description_for_merging_=*/SortDescription{},
            /*group_by_sort_description_=*/SortDescription{},
            /*should_produce_results_in_order_of_bucket_number_=*/false,
            /*memory_bound_merging_of_aggregation_results_enabled_=*/false,
            /*explicit_sorting_required_for_aggregation_in_order_=*/false);
        plan.addStep(std::move(aggregating_step));

        /// Rename aggregate columns back to original names and project only needed columns.
        {
            const auto & agg_header = plan.getCurrentHeader();
            ActionsDAG rename_dag(plan.getCurrentHeader()->getColumnsWithTypeAndName());
            const auto & inputs = rename_dag.getInputs();

            const auto * row_index_node = inputs[agg_header->getPositionByName(argmax_row_index_name)];
            row_index_node = &rename_dag.addAlias(*row_index_node, "__global_row_index");

            auto & outputs = rename_dag.getOutputs();
            outputs.clear();
            outputs.push_back(row_index_node);
            if (!merging_params.is_deleted_column.empty())
            {
                const auto * is_deleted_node = inputs[agg_header->getPositionByName(argmax_is_deleted_name)];
                is_deleted_node = &rename_dag.addAlias(*is_deleted_node, merging_params.is_deleted_column);
                outputs.push_back(is_deleted_node);
            }

            plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(rename_dag)));
        }

        /// Filter out rows where is_deleted is set, and remove the column.
        if (!merging_params.is_deleted_column.empty())
        {
            ActionsDAG filter_dag(plan.getCurrentHeader()->getColumnsWithTypeAndName());
            auto not_func = FunctionFactory::instance().get("not", nullptr);
            const auto * not_node = &filter_dag.addFunction(not_func, {filter_dag.getOutputs().back()}, {});
            filter_dag.getOutputs().back() = not_node;
            plan.addStep(
                std::make_unique<FilterStep>(
                    plan.getCurrentHeader(), std::move(filter_dag), not_node->result_name, /*remove_filter_column=*/true));
        }
    }

    {
        WriteBufferFromOwnString out;
        plan.explainPlan(out, {.header = true, .actions = true});
        LOG_DEBUG(getLogger("LazyReadReplacingFinalSource"), "Plan:\n{}", out.str());
    }

    auto builder = plan.buildQueryPipeline(QueryPlanOptimizationSettings(query_context), BuildQueryPipelineSettings(query_context));

    QueryPlanResourceHolder resources;
    auto pipe = QueryPipelineBuilder::getPipe(std::move(*builder), resources);
    pipe.resize(1);

    pipeline_output = pipe.getOutputPort(0);
    processors = Pipe::detachProcessors(std::move(pipe));
}

Processors LazyReadReplacingFinalSource::expandPipeline()
{
    inputs.emplace_back(pipeline_output->getHeader(), this);
    connect(*pipeline_output, inputs.back());
    inputs.back().setNeeded();
    return std::move(processors);
}

}
