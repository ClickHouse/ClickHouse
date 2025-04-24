#include <Interpreters/Context.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MemoryBoundMerging.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Core/Settings.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 aggregation_in_order_max_block_bytes;
    extern const QueryPlanSerializationSettingsBool aggregation_in_order_memory_bound_merging;
    extern const QueryPlanSerializationSettingsBool aggregation_sort_result_by_bucket_number;
    extern const QueryPlanSerializationSettingsBool collect_hash_table_stats_during_aggregation;
    extern const QueryPlanSerializationSettingsBool compile_aggregate_expressions;
    extern const QueryPlanSerializationSettingsBool empty_result_for_aggregation_by_empty_set;
    extern const QueryPlanSerializationSettingsBool enable_software_prefetch_in_aggregation;
    extern const QueryPlanSerializationSettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const QueryPlanSerializationSettingsUInt64 group_by_two_level_threshold_bytes;
    extern const QueryPlanSerializationSettingsUInt64 group_by_two_level_threshold;
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_group_by;
    extern const QueryPlanSerializationSettingsUInt64 max_entries_for_hash_table_stats;
    extern const QueryPlanSerializationSettingsUInt64 max_rows_to_group_by;
    extern const QueryPlanSerializationSettingsUInt64 max_size_to_preallocate_for_aggregation;
    extern const QueryPlanSerializationSettingsUInt64 min_count_to_compile_aggregate_expression;
    extern const QueryPlanSerializationSettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const QueryPlanSerializationSettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
    extern const QueryPlanSerializationSettingsBool optimize_group_by_constant_keys;
}

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 aggregation_memory_efficient_merge_threads;
    extern const SettingsBool enable_memory_bound_merging_of_aggregation_results;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

static ITransformingStep::Traits getTraits(bool should_produce_results_in_order_of_bucket_number)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = should_produce_results_in_order_of_bucket_number,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

MergingAggregatedStep::MergingAggregatedStep(
    const Header & input_header_,
    Aggregator::Params params_,
    GroupingSetsParamsList grouping_sets_params_,
    bool final_,
    bool memory_efficient_aggregation_,
    size_t memory_efficient_merge_threads_,
    bool should_produce_results_in_order_of_bucket_number_,
    size_t max_block_size_,
    size_t memory_bound_merging_max_block_bytes_,
    bool memory_bound_merging_of_aggregation_results_enabled_)
    : ITransformingStep(
          input_header_,
          MergingAggregatedTransform::appendGroupingIfNeeded(input_header_, params_.getHeader(input_header_, final_)),
          getTraits(should_produce_results_in_order_of_bucket_number_))
    , params(std::move(params_))
    , grouping_sets_params(std::move(grouping_sets_params_))
    , final(final_)
    , memory_efficient_aggregation(memory_efficient_aggregation_)
    , max_threads(params.max_threads)
    , memory_efficient_merge_threads(memory_efficient_merge_threads_)
    , max_block_size(max_block_size_)
    , memory_bound_merging_max_block_bytes(memory_bound_merging_max_block_bytes_)
    , should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number_)
    , memory_bound_merging_of_aggregation_results_enabled(memory_bound_merging_of_aggregation_results_enabled_)
{
}

void MergingAggregatedStep::applyOrder(SortDescription input_sort_description)
{
    /// Columns might be reordered during optimization, so we better to update sort description.
    group_by_sort_description = std::move(input_sort_description);
}

void MergingAggregatedStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (memoryBoundMergingWillBeUsed())
    {
        if (input_headers.front().has("__grouping_set") || !grouping_sets_params.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                 "Memory bound merging of aggregated results is not supported for grouping sets.");

        auto transform_params = std::make_shared<AggregatingTransformParams>(pipeline.getHeader(), std::move(params), final);
        auto transform = std::make_shared<FinishAggregatingInOrderTransform>(
            pipeline.getHeader(),
            pipeline.getNumStreams(),
            transform_params,
            group_by_sort_description,
            max_block_size,
            memory_bound_merging_max_block_bytes);

        pipeline.addTransform(std::move(transform));

        /// Do merge of aggregated data in parallel.
        pipeline.resize(max_threads);

        const auto & required_sort_description
            = should_produce_results_in_order_of_bucket_number ? group_by_sort_description : SortDescription{};

        pipeline.addSimpleTransform(
            [&](const Block &) { return std::make_shared<MergingAggregatedBucketTransform>(transform_params, required_sort_description); });

        if (should_produce_results_in_order_of_bucket_number)
        {
            pipeline.addTransform(
                std::make_shared<SortingAggregatedForMemoryBoundMergingTransform>(pipeline.getHeader(), pipeline.getNumStreams()));
        }

        return;
    }

    if (!memory_efficient_aggregation)
    {
        /// We union several sources into one, paralleling the work.
        pipeline.resize(1);

        /// Now merge the aggregated blocks
        auto transform = std::make_shared<MergingAggregatedTransform>(pipeline.getHeader(), params, final, grouping_sets_params);
        pipeline.addTransform(std::move(transform));
    }
    else
    {
        if (input_headers.front().has("__grouping_set") || !grouping_sets_params.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                 "Memory efficient merging of aggregated results is not supported for grouping sets.");
        auto num_merge_threads = memory_efficient_merge_threads
                                 ? memory_efficient_merge_threads
                                 : max_threads;

        auto transform_params = std::make_shared<AggregatingTransformParams>(pipeline.getHeader(), std::move(params), final);
        pipeline.addMergingAggregatedMemoryEfficientTransform(transform_params, num_merge_threads);
    }

    pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : max_threads);
}

void MergingAggregatedStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
    if (!group_by_sort_description.empty())
    {
        String prefix(settings.offset, settings.indent_char);
        settings.out << prefix << "Order: " << dumpSortDescription(group_by_sort_description) << '\n';
    }
}

void MergingAggregatedStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
    if (!group_by_sort_description.empty())
        map.add("Order", dumpSortDescription(group_by_sort_description));
}

void MergingAggregatedStep::updateOutputHeader()
{
    const auto & in_header = input_headers.front();
    output_header = MergingAggregatedTransform::appendGroupingIfNeeded(in_header, params.getHeader(in_header, final));
}

bool MergingAggregatedStep::memoryBoundMergingWillBeUsed() const
{
    return memory_bound_merging_of_aggregation_results_enabled && !group_by_sort_description.empty();
}

const SortDescription & MergingAggregatedStep::getSortDescription() const
{
    if (memoryBoundMergingWillBeUsed() && should_produce_results_in_order_of_bucket_number)
        return group_by_sort_description;

    return IQueryPlanStep::getSortDescription();
}

void MergingAggregatedStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;
    settings[QueryPlanSerializationSetting::aggregation_in_order_max_block_bytes] = memory_bound_merging_max_block_bytes;

    settings[QueryPlanSerializationSetting::aggregation_in_order_memory_bound_merging] = should_produce_results_in_order_of_bucket_number;
    settings[QueryPlanSerializationSetting::aggregation_sort_result_by_bucket_number] = memory_bound_merging_of_aggregation_results_enabled;

    settings[QueryPlanSerializationSetting::max_rows_to_group_by] = params.max_rows_to_group_by;
    settings[QueryPlanSerializationSetting::group_by_overflow_mode] = params.group_by_overflow_mode;

    settings[QueryPlanSerializationSetting::group_by_two_level_threshold] = params.group_by_two_level_threshold;
    settings[QueryPlanSerializationSetting::group_by_two_level_threshold_bytes] = params.group_by_two_level_threshold_bytes;

    settings[QueryPlanSerializationSetting::max_bytes_before_external_group_by] = params.max_bytes_before_external_group_by;
    settings[QueryPlanSerializationSetting::empty_result_for_aggregation_by_empty_set] = params.empty_result_for_aggregation_by_empty_set;

    settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data] = params.min_free_disk_space;

    settings[QueryPlanSerializationSetting::compile_aggregate_expressions] = params.compile_aggregate_expressions;
    settings[QueryPlanSerializationSetting::min_count_to_compile_aggregate_expression] = params.min_count_to_compile_aggregate_expression;

    settings[QueryPlanSerializationSetting::enable_software_prefetch_in_aggregation] = params.enable_prefetch;
    settings[QueryPlanSerializationSetting::optimize_group_by_constant_keys] = params.optimize_group_by_constant_keys;
    settings[QueryPlanSerializationSetting::min_hit_rate_to_use_consecutive_keys_optimization] = params.min_hit_rate_to_use_consecutive_keys_optimization;

    settings[QueryPlanSerializationSetting::collect_hash_table_stats_during_aggregation] = params.stats_collecting_params.isCollectionAndUseEnabled();
    settings[QueryPlanSerializationSetting::max_entries_for_hash_table_stats] = params.stats_collecting_params.max_entries_for_hash_table_stats;
    settings[QueryPlanSerializationSetting::max_size_to_preallocate_for_aggregation] = params.stats_collecting_params.max_size_to_preallocate;
}

void MergingAggregatedStep::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    if (final)
        flags |= 1;
    if (params.overflow_row)
        flags |= 2;
    if (memory_efficient_aggregation)
        flags |= 4;
    if (!grouping_sets_params.empty())
        flags |= 8;
    if (params.stats_collecting_params.isCollectionAndUseEnabled())
        flags |= 16;
    if (params.only_merge)
        flags |= 32;
    if (should_produce_results_in_order_of_bucket_number)
        flags |= 64;
    if (memory_bound_merging_of_aggregation_results_enabled)
        flags |= 128;

    writeIntBinary(flags, ctx.out);

    writeVarUInt(params.keys.size(), ctx.out);
    for (const auto & key : params.keys)
        writeStringBinary(key, ctx.out);

    if (!grouping_sets_params.empty())
    {
        writeVarUInt(grouping_sets_params.size(), ctx.out);
        for (const auto & grouping_set : grouping_sets_params)
        {
            /// Only used keys are needed.
            writeVarUInt(grouping_set.used_keys.size(), ctx.out);
            for (const auto & used_key : grouping_set.used_keys)
                writeStringBinary(used_key, ctx.out);
        }
    }

    serializeAggregateDescriptions(params.aggregates, ctx.out);

    serializeSortDescription(group_by_sort_description, ctx.out);

    if (params.stats_collecting_params.isCollectionAndUseEnabled())
        writeIntBinary(params.stats_collecting_params.key, ctx.out);
}

std::unique_ptr<IQueryPlanStep> MergingAggregatedStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "MergingAggregatedStep must have one input stream");

    UInt8 flags;
    readIntBinary(flags, ctx.in);

    const bool final = bool(flags & 1);
    const bool overflow_row = bool(flags & 2);
    const bool memory_efficient_aggregation = bool(flags & 4);
    const bool has_grouping_sets = bool(flags & 8);
    const bool has_stats_key = bool(flags & 16);
    const bool only_merge = bool(flags & 32);
    const bool should_produce_results_in_order_of_bucket_number = bool(flags & 64);
    const bool memory_bound_merging_of_aggregation_results_enabled = bool(flags & 128);

    UInt64 num_keys;
    readVarUInt(num_keys, ctx.in);
    Names keys(num_keys);
    for (auto & key : keys)
        readStringBinary(key, ctx.in);

    GroupingSetsParamsList grouping_sets_params;
    if (has_grouping_sets)
    {
        UInt64 num_groups;
        readVarUInt(num_groups, ctx.in);
        for (size_t group_num = 0; group_num < num_groups; ++group_num)
        {
            auto & grouping_set = grouping_sets_params.emplace_back();
            UInt64 num_used_keys;
            readVarUInt(num_used_keys, ctx.in);
            grouping_set.used_keys.resize(num_used_keys);
            NameSet used_keys_set;
            for (auto & used_key : grouping_set.used_keys)
            {
                readStringBinary(used_key, ctx.in);
                used_keys_set.insert(used_key);
            }
            if (num_keys > num_used_keys)
                grouping_set.missing_keys.reserve(num_keys - num_used_keys);
            for (const auto & key : keys)
                if (!used_keys_set.contains(key))
                    grouping_set.missing_keys.push_back(key);
        }
    }

    AggregateDescriptions aggregates;
    deserializeAggregateDescriptions(aggregates, ctx.in);

    SortDescription group_by_sort_description;
    deserializeSortDescription(group_by_sort_description, ctx.in);

    UInt64 stats_key = 0;
    if (has_stats_key)
        readIntBinary(stats_key, ctx.in);

    StatsCollectingParams stats_collecting_params(
        stats_key,
        ctx.settings[QueryPlanSerializationSetting::collect_hash_table_stats_during_aggregation],
        ctx.settings[QueryPlanSerializationSetting::max_entries_for_hash_table_stats],
        ctx.settings[QueryPlanSerializationSetting::max_size_to_preallocate_for_aggregation]);

    const auto & settings = ctx.context->getSettingsRef();

    Aggregator::Params params
    {
        keys,
        aggregates,
        overflow_row,
        ctx.settings[QueryPlanSerializationSetting::max_rows_to_group_by],
        ctx.settings[QueryPlanSerializationSetting::group_by_overflow_mode],
        ctx.settings[QueryPlanSerializationSetting::group_by_two_level_threshold],
        ctx.settings[QueryPlanSerializationSetting::group_by_two_level_threshold_bytes],
        ctx.settings[QueryPlanSerializationSetting::max_bytes_before_external_group_by],
        ctx.settings[QueryPlanSerializationSetting::empty_result_for_aggregation_by_empty_set],
        Context::getGlobalContextInstance()->getTempDataOnDisk(),
        settings[Setting::max_threads],
        ctx.settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data],
        ctx.settings[QueryPlanSerializationSetting::compile_aggregate_expressions],
        ctx.settings[QueryPlanSerializationSetting::min_count_to_compile_aggregate_expression],
        ctx.settings[QueryPlanSerializationSetting::max_block_size],
        ctx.settings[QueryPlanSerializationSetting::enable_software_prefetch_in_aggregation],
        /* only_merge */ only_merge,
        ctx.settings[QueryPlanSerializationSetting::optimize_group_by_constant_keys],
        ctx.settings[QueryPlanSerializationSetting::min_hit_rate_to_use_consecutive_keys_optimization],
        stats_collecting_params
    };

    auto merging_aggregated_step = std::make_unique<MergingAggregatedStep>(
        ctx.input_headers.front(),
        std::move(params),
        std::move(grouping_sets_params),
        final,
        memory_efficient_aggregation,
        settings[Setting::aggregation_memory_efficient_merge_threads],
        should_produce_results_in_order_of_bucket_number,
        ctx.settings[QueryPlanSerializationSetting::max_block_size],
        ctx.settings[QueryPlanSerializationSetting::aggregation_in_order_max_block_bytes],
        memory_bound_merging_of_aggregation_results_enabled);

    merging_aggregated_step->applyOrder(std::move(group_by_sort_description));

    return merging_aggregated_step;
}

void registerMergingAggregatedStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("MergingAggregated", MergingAggregatedStep::deserialize);
}

}
