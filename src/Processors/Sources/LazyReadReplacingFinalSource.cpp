#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
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
    const MergeTreeData & data_,
    ContextPtr query_context_,
    LazyFinalSharedStatePtr shared_state_)
    : IProcessor({}, {Block({ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "__global_row_index"}})})
    , metadata_snapshot(std::move(metadata_snapshot_))
    , data(data_)
    , query_context(std::move(query_context_))
    , shared_state(std::move(shared_state_))
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
        return Status::PortFull;

    if (inputs.empty())
    {
        if (processors.empty())
            return Status::Ready;
        else
            return Status::UpdatePipeline;
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
    return Status::PortFull;
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

    if (!added_part_starting_offset)
        dag.getOutputs().push_back(part_starting_offset_in);
    if (!added_part_offset)
        dag.getOutputs().push_back(part_offset_in);
}

QueryPlan LazyReadReplacingFinalSource::buildPlanFromReadingStep(
    std::unique_ptr<ReadFromMergeTree> reading,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData & data,
    ContextPtr query_context)
{
    const auto & settings = query_context->getSettingsRef();
    const auto & sorting_key = metadata_snapshot->getSortingKey();
    const auto & merging_params = data.merging_params;

    QueryPlan plan;

    {
        ActionsDAG dag = sorting_key.expression->getActionsDAG().clone();
        calculateGlobalOffset(dag, *reading);

        plan.addStep(std::move(reading));
        plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag)));
    }

    /// When there's a version column, compute a tiebreaker for argMax so that
    /// equal versions resolve to the last inserted row (highest __global_row_index).
    /// For version types ≤ 64 bits, pack (version, __global_row_index) into UInt128.
    /// For wider types, use tuple(version, __global_row_index).
    /// When there's no version column, just use max(__global_row_index).
    bool has_version = !merging_params.version_column.empty();
    static constexpr auto tiebreaker_column_name = "__lazy_final_tiebreaker";

    if (has_version)
    {
        const auto & header = plan.getCurrentHeader();
        auto version_type = header->getByName(merging_params.version_column).type;
        WhichDataType which(version_type);
        bool use_packed = which.isNativeUInt() || which.isNativeInt() || which.isDate() || which.isDate32() || which.isDateTime();

        ActionsDAG dag(header->getColumnsWithTypeAndName());
        const auto * version_node = &dag.findInOutputs(merging_params.version_column);
        const auto * row_index_node = &dag.findInOutputs("__global_row_index");

        if (use_packed)
        {
            auto to_uint128 = FunctionFactory::instance().get("toUInt128", nullptr);
            auto bit_shift_left = FunctionFactory::instance().get("bitShiftLeft", nullptr);
            auto plus_func = FunctionFactory::instance().get("plus", nullptr);

            /// For signed types (and Date32 which is Int32 internally),
            /// widen to Int64 first, then flip the sign bit
            /// to convert signed order to unsigned order.
            if (which.isNativeInt() || which.isDate32())
            {
                auto to_int64 = FunctionFactory::instance().get("toInt64", nullptr);
                auto reinterpret_func = FunctionFactory::instance().get("reinterpretAsUInt64", nullptr);
                auto bitxor_func = FunctionFactory::instance().get("bitXor", nullptr);
                ColumnWithTypeAndName sign_bit_const;
                sign_bit_const.type = std::make_shared<DataTypeUInt64>();
                sign_bit_const.column = sign_bit_const.type->createColumnConst(1, Field(UInt64(1) << 63));
                sign_bit_const.name = "__sign_bit";
                const auto * sign_bit_node = &dag.addColumn(std::move(sign_bit_const));
                const auto * version_int64 = &dag.addFunction(to_int64, {version_node}, {});
                const auto * version_uint64 = &dag.addFunction(reinterpret_func, {version_int64}, {});
                version_node = &dag.addFunction(bitxor_func, {version_uint64, sign_bit_node}, {});
            }

            const auto * version_128 = &dag.addFunction(to_uint128, {version_node}, {});
            const auto * row_index_128 = &dag.addFunction(to_uint128, {row_index_node}, {});
            ColumnWithTypeAndName shift_const;
            shift_const.type = std::make_shared<DataTypeUInt8>();
            shift_const.column = shift_const.type->createColumnConst(1, Field(UInt8(64)));
            shift_const.name = "__shift_64";
            const auto * shift_amount = &dag.addColumn(std::move(shift_const));
            const auto * shifted = &dag.addFunction(bit_shift_left, {version_128, shift_amount}, {});
            const auto * tiebreaker = &dag.addFunction(plus_func, {shifted, row_index_128}, {});
            tiebreaker = &dag.addAlias(*tiebreaker, tiebreaker_column_name);
            dag.getOutputs().push_back(tiebreaker);
        }
        else
        {
            auto tuple_func = FunctionFactory::instance().get("tuple", query_context);
            const auto * tiebreaker = &dag.addFunction(tuple_func, {version_node, row_index_node}, {});
            tiebreaker = &dag.addAlias(*tiebreaker, tiebreaker_column_name);
            dag.getOutputs().push_back(tiebreaker);
        }

        plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag)));
    }

    {
        const auto & header = plan.getCurrentHeader();
        AggregateFunctionProperties properties;
        AggregateDescriptions aggregates;
        auto uint64_type = std::make_shared<DataTypeUInt64>();

        String argmax_row_index_name;
        String argmax_is_deleted_name;

        if (has_version)
        {
            auto tiebreaker_type = header->getByName(tiebreaker_column_name).type;
            argmax_row_index_name = fmt::format("argMax(__global_row_index, {})", tiebreaker_column_name);

            /// argMax(__global_row_index, __tiebreaker)
            AggregateDescription desc;
            desc.function = AggregateFunctionFactory::instance().get(
                "argMax", NullsAction::EMPTY, {uint64_type, tiebreaker_type}, {}, properties);
            desc.argument_names = {"__global_row_index", tiebreaker_column_name};
            desc.column_name = argmax_row_index_name;
            aggregates.push_back(std::move(desc));

            /// argMax(is_deleted, __tiebreaker) if is_deleted column exists
            if (!merging_params.is_deleted_column.empty())
            {
                argmax_is_deleted_name = fmt::format("argMax({}, {})", merging_params.is_deleted_column, tiebreaker_column_name);
                auto is_deleted_type = header->getByName(merging_params.is_deleted_column).type;

                AggregateDescription desc2;
                desc2.function = AggregateFunctionFactory::instance().get(
                    "argMax", NullsAction::EMPTY, {is_deleted_type, tiebreaker_type}, {}, properties);
                desc2.argument_names = {merging_params.is_deleted_column, tiebreaker_column_name};
                desc2.column_name = argmax_is_deleted_name;
                aggregates.push_back(std::move(desc2));
            }
        }
        else
        {
            /// No version column — just pick the last inserted row.
            argmax_row_index_name = "max(__global_row_index)";

            AggregateDescription desc;
            desc.function = AggregateFunctionFactory::instance().get(
                "max", NullsAction::EMPTY, {uint64_type}, {}, properties);
            desc.argument_names = {"__global_row_index"};
            desc.column_name = argmax_row_index_name;
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

    return plan;
}

void LazyReadReplacingFinalSource::work()
{
    auto & reading = shared_state->reading_step;
    chassert(reading);

    auto plan = buildPlanFromReadingStep(std::move(reading), metadata_snapshot, data, query_context);

    auto builder = plan.buildQueryPipeline(QueryPlanOptimizationSettings(query_context), BuildQueryPipelineSettings(query_context));

    auto pipe = QueryPipelineBuilder::getPipe(std::move(*builder), resources);
    pipe.resize(1);

    pipeline_output = pipe.getOutputPort(0);
    processors = Pipe::detachProcessors(std::move(pipe));
}

IProcessor::PipelineUpdate LazyReadReplacingFinalSource::updatePipeline()
{
    inputs.emplace_back(pipeline_output->getHeader(), this);
    connect(*pipeline_output, inputs.back());
    inputs.back().setNeeded();
    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

}
