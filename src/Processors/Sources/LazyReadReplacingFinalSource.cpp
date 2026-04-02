#include <string_view>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Sources/LazyReadReplacingFinalSource.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/AggregatingStep.h>

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsNonZeroUInt64 max_block_size;
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
    : metadata_snapshot(std::move(metadata_snapshot_))
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
    const auto * global_offset_node = &dag.addFunction(plus, dag.getInputs(), {});
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
            columns_to_read.insert(merging_params.version_column);
        if (!merging_params.is_deleted_column.empty() && !columns_to_read.contains(merging_params.is_deleted_column))
            columns_to_read.insert(merging_params.is_deleted_column);

        SelectQueryInfo query_info;

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
            false
        );

        ActionsDAG dag = sorting_key.expression->getActionsDAG().clone();
        calculateGlobalOffset(dag, *reading);

        plan.addStep(std::move(reading));
        auto expression = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag));
        plan.addStep(std::move(expression));
    }

    {
        AggregateDescriptions aggregates;

        Aggregator::Params params(
            sorting_key.column_names,
            const AggregateDescriptions & aggregates_,
            bool overflow_row_,
            size_t max_rows_to_group_by_,
            OverflowMode group_by_overflow_mode_,
            size_t group_by_two_level_threshold_,
            size_t group_by_two_level_threshold_bytes_,
            size_t max_bytes_before_external_group_by_,
            bool empty_result_for_aggregation_by_empty_set_,
            TemporaryDataOnDiskScopePtr tmp_data_scope_,
            size_t max_threads_,
            size_t min_free_disk_space_,
            bool compile_aggregate_expressions_,
            size_t min_count_to_compile_aggregate_expression_,
            size_t max_block_size_,
            bool enable_prefetch_,
            bool only_merge_, // true for projections
            bool optimize_group_by_constant_keys_,
            float min_hit_rate_to_use_consecutive_keys_optimization_,
            const StatsCollectingParams & stats_collecting_params_,
            bool enable_producing_buckets_out_of_order_in_aggregation_,
            bool serialize_string_with_zero_byte_);
    }
}

}
