#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Core/Settings.h>
#include <IO/Operators.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CoalescingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <Processors/Transforms/SelectByIndicesTransform.h>
#include <Processors/Transforms/VirtualRowTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/LazilyReadInfo.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>
#include <Storages/MergeTree/MergeTreePrefetchedReadPool.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>
#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicas.h>
#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicasInOrder.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Poco/Logger.h>
#include <Common/JSONBuilder.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <unordered_map>

#include <fmt/ranges.h>

#include "config.h"

using namespace DB;

namespace
{
template <typename Container, typename Getter>
size_t countPartitions(const Container & parts, Getter get_partition_id)
{
    if (parts.empty())
        return 0;

    String cur_partition_id = get_partition_id(parts[0]);
    size_t unique_partitions = 1;
    for (size_t i = 1; i < parts.size(); ++i)
    {
        if (get_partition_id(parts[i]) != cur_partition_id)
        {
            ++unique_partitions;
            cur_partition_id = get_partition_id(parts[i]);
        }
    }
    return unique_partitions;
}

size_t countPartitions(const RangesInDataParts & parts_with_ranges)
{
    auto get_partition_id = [](const RangesInDataPart & rng) { return rng.data_part->info.getPartitionId(); };
    return countPartitions(parts_with_ranges, get_partition_id);
}

bool restoreDAGInputs(ActionsDAG & dag, const NameSet & inputs)
{
    std::unordered_set<const ActionsDAG::Node *> outputs(dag.getOutputs().begin(), dag.getOutputs().end());
    bool added = false;
    for (const auto * input : dag.getInputs())
    {
        if (inputs.contains(input->result_name) && !outputs.contains(input))
        {
            dag.getOutputs().push_back(input);
            added = true;
        }
    }

    return added;
}

bool restorePrewhereInputs(PrewhereInfo & info, const NameSet & inputs)
{
    bool added = false;
    if (info.row_level_filter)
        added = added || restoreDAGInputs(*info.row_level_filter, inputs);

    added = added || restoreDAGInputs(info.prewhere_actions, inputs);

    return added;
}

}

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedPartsTotal;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
    extern const Event SelectedMarksTotal;
    extern const Event SelectQueriesWithPrimaryKeyUsage;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_asynchronous_read_from_io_pool_for_merge_tree;
    extern const SettingsBool allow_prefetched_read_pool_for_local_filesystem;
    extern const SettingsBool allow_prefetched_read_pool_for_remote_filesystem;
    extern const SettingsBool apply_deleted_mask;
    extern const SettingsBool checksum_on_read;
    extern const SettingsBool compile_sort_description;
    extern const SettingsBool do_not_merge_across_partitions_select_final;
    extern const SettingsBool enable_multiple_prewhere_read_steps;
    extern const SettingsBool enable_vertical_final;
    extern const SettingsBool force_aggregate_partitions_independently;
    extern const SettingsBool force_primary_key;
    extern const SettingsString ignore_data_skipping_indices;
    extern const SettingsUInt64 max_number_of_partitions_for_independent_aggregation;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsMaxThreads max_final_threads;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsFloat max_streams_to_max_threads_ratio;
    extern const SettingsUInt64 max_streams_for_merge_tree_reading;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 merge_tree_max_bytes_to_use_cache;
    extern const SettingsUInt64 merge_tree_max_rows_to_use_cache;
    extern const SettingsUInt64 merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem;
    extern const SettingsUInt64 merge_tree_min_rows_for_concurrent_read_for_remote_filesystem;
    extern const SettingsUInt64 merge_tree_min_bytes_for_concurrent_read;
    extern const SettingsUInt64 merge_tree_min_rows_for_concurrent_read;
    extern const SettingsFloat merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability;
    extern const SettingsBool merge_tree_use_const_size_tasks_for_remote_reading;
    extern const SettingsUInt64 min_count_to_compile_sort_description;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsOverflowMode read_overflow_mode_leaf;
    extern const SettingsUInt64 parallel_replicas_count;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsUInt64 preferred_max_column_in_block_size_bytes;
    extern const SettingsUInt64 read_in_order_two_level_merge_threshold;
    extern const SettingsBool split_parts_ranges_into_intersecting_and_non_intersecting_final;
    extern const SettingsBool split_intersecting_parts_ranges_into_layers_final;
    extern const SettingsBool use_skip_indexes;
    extern const SettingsBool use_skip_indexes_if_final;
    extern const SettingsBool use_uncompressed_cache;
    extern const SettingsBool query_plan_merge_filters;
    extern const SettingsUInt64 merge_tree_min_read_task_size;
    extern const SettingsBool read_in_order_use_virtual_row;
    extern const SettingsBool use_skip_indexes_if_final_exact_mode;
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool query_condition_cache_store_conditions_as_plaintext;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool merge_tree_use_deserialization_prefixes_cache;
    extern const SettingsBool merge_tree_use_prefixes_deserialization_thread_pool;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsBool enable_shared_storage_snapshot_in_query;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
}

namespace ErrorCodes
{
    extern const int INDEX_NOT_USED;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS;
    extern const int CANNOT_PARSE_TEXT;
}

static MergeTreeReaderSettings getMergeTreeReaderSettings(
    const ContextPtr & context, const SelectQueryInfo & query_info)
{
    const auto & settings = context->getSettingsRef();
    return {
        .read_settings = context->getReadSettings(),
        .save_marks_in_cache = true,
        .checksum_on_read = settings[Setting::checksum_on_read],
        .read_in_order = query_info.input_order_info != nullptr,
        .apply_deleted_mask = settings[Setting::apply_deleted_mask],
        .use_asynchronous_read_from_pool = settings[Setting::allow_asynchronous_read_from_io_pool_for_merge_tree]
            && (settings[Setting::max_streams_to_max_threads_ratio] > 1 || settings[Setting::max_streams_for_merge_tree_reading] > 1),
        .enable_multiple_prewhere_read_steps = settings[Setting::enable_multiple_prewhere_read_steps],
        .force_short_circuit_execution = settings[Setting::query_plan_merge_filters],
        .use_query_condition_cache = settings[Setting::use_query_condition_cache] && settings[Setting::allow_experimental_analyzer],
        .query_condition_cache_store_conditions_as_plaintext = settings[Setting::query_condition_cache_store_conditions_as_plaintext],
        .use_deserialization_prefixes_cache = settings[Setting::merge_tree_use_deserialization_prefixes_cache],
        .use_prefixes_deserialization_thread_pool = settings[Setting::merge_tree_use_prefixes_deserialization_thread_pool],
    };
}

static bool checkAllPartsOnRemoteFS(const RangesInDataParts & parts)
{
    for (const auto & part : parts)
    {
        if (!part.data_part->isStoredOnRemoteDisk())
            return false;
    }
    return true;
}

/// build sort description for output stream
static SortDescription getSortDescriptionForOutputHeader(
    const Header & output_header,
    const Names & sorting_key_columns,
    const std::vector<bool> & reverse_flags,
    const int sort_direction,
    InputOrderInfoPtr input_order_info,
    PrewhereInfoPtr prewhere_info,
    bool enable_vertical_final)
{
    /// Updating sort description can be done after PREWHERE actions are applied to the header.
    /// After PREWHERE actions are applied, column names in header can differ from storage column names due to aliases
    /// To mitigate it, we're trying to build original header and use it to deduce sorting description
    /// TODO: this approach is fragile, it'd be more robust to update sorting description for the whole plan during plan optimization
    Block original_header = output_header.cloneEmpty();
    if (prewhere_info)
    {
        {
            FindOriginalNodeForOutputName original_column_finder(prewhere_info->prewhere_actions);
            for (auto & column : original_header)
            {
                const auto * original_node = original_column_finder.find(column.name);
                if (original_node)
                    column.name = original_node->result_name;
            }
        }

        if (prewhere_info->row_level_filter)
        {
            FindOriginalNodeForOutputName original_column_finder(*prewhere_info->row_level_filter);
            for (auto & column : original_header)
            {
                const auto * original_node = original_column_finder.find(column.name);
                if (original_node)
                    column.name = original_node->result_name;
            }
        }
    }

    SortDescription sort_description;
    const Block & header = output_header;
    size_t sort_columns_size = sorting_key_columns.size();
    sort_description.reserve(sort_columns_size);
    for (size_t i = 0; i < sort_columns_size; ++i)
    {
        const auto & sorting_key = sorting_key_columns[i];
        const auto it = std::find_if(
            original_header.begin(), original_header.end(), [&sorting_key](const auto & column) { return column.name == sorting_key; });
        if (it == original_header.end())
            break;

        const size_t column_pos = std::distance(original_header.begin(), it);
        if (!reverse_flags.empty() && reverse_flags[i])
            sort_description.emplace_back((header.begin() + column_pos)->name, sort_direction * -1);
        else
            sort_description.emplace_back((header.begin() + column_pos)->name, sort_direction);
    }

    if (input_order_info && !enable_vertical_final)
    {
        // output_stream.sort_scope = DataStream::SortScope::Stream;
        const size_t used_prefix_of_sorting_key_size = input_order_info->used_prefix_of_sorting_key_size;
        if (sort_description.size() > used_prefix_of_sorting_key_size)
            sort_description.resize(used_prefix_of_sorting_key_size);

        return sort_description;
    }

    return {};
}

void ReadFromMergeTree::AnalysisResult::checkLimits(const Settings & settings, const SelectQueryInfo & query_info_) const
{

    /// Do not check number of read rows if we have reading
    /// in order of sorting key with limit.
    /// In general case, when there exists WHERE clause
    /// it's impossible to estimate number of rows precisely,
    /// because we can stop reading at any time.

    SizeLimits limits;
    if (settings[Setting::read_overflow_mode] == OverflowMode::THROW && settings[Setting::max_rows_to_read] && !query_info_.input_order_info)
        limits = SizeLimits(settings[Setting::max_rows_to_read], 0, settings[Setting::read_overflow_mode]);

    SizeLimits leaf_limits;
    if (settings[Setting::read_overflow_mode_leaf] == OverflowMode::THROW && settings[Setting::max_rows_to_read_leaf] && !query_info_.input_order_info)
        leaf_limits = SizeLimits(settings[Setting::max_rows_to_read_leaf], 0, settings[Setting::read_overflow_mode_leaf]);

    if (limits.max_rows || leaf_limits.max_rows)
    {
        /// Fail fast if estimated number of rows to read exceeds the limit
        size_t total_rows_estimate = selected_rows;
        if (query_info_.trivial_limit > 0 && total_rows_estimate > query_info_.trivial_limit)
        {
            total_rows_estimate = query_info_.trivial_limit;
        }
        limits.check(total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
        leaf_limits.check(
            total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
    }
}

ReadFromMergeTree::ReadFromMergeTree(
    RangesInDataParts parts_,
    MergeTreeData::MutationsSnapshotPtr mutations_,
    Names all_column_names_,
    const MergeTreeData & data_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    size_t max_block_size_,
    size_t num_streams_,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
    LoggerPtr log_,
    AnalysisResultPtr analyzed_result_ptr_,
    bool enable_parallel_reading_,
    std::optional<MergeTreeAllRangesCallback> all_ranges_callback_,
    std::optional<MergeTreeReadTaskCallback> read_task_callback_,
    std::optional<size_t> number_of_current_replica_)
    : SourceStepWithFilter(MergeTreeSelectProcessor::transformHeader(
        storage_snapshot_->getSampleBlockForColumns(all_column_names_),
        {},
        query_info_.prewhere_info), all_column_names_, query_info_, storage_snapshot_, context_)
    , reader_settings(getMergeTreeReaderSettings(context_, query_info_))
    , prepared_parts(std::move(parts_))
    , mutations_snapshot(std::move(mutations_))
    , all_column_names(std::move(all_column_names_))
    , data(data_)
    , actions_settings(ExpressionActionsSettings(context_))
    , block_size{
        .max_block_size_rows = max_block_size_,
        .preferred_block_size_bytes = context->getSettingsRef()[Setting::preferred_block_size_bytes],
        .preferred_max_column_in_block_size_bytes = context->getSettingsRef()[Setting::preferred_max_column_in_block_size_bytes]}
    , requested_num_streams(num_streams_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , log(std::move(log_))
    , analyzed_result_ptr(analyzed_result_ptr_)
    , is_parallel_reading_from_replicas(enable_parallel_reading_)
    , enable_remove_parts_from_snapshot_optimization(!context->getSettingsRef()[Setting::enable_shared_storage_snapshot_in_query] && query_info_.merge_tree_enable_remove_parts_from_snapshot_optimization)
    , number_of_current_replica(number_of_current_replica_)
{
    if (is_parallel_reading_from_replicas)
    {
        if (all_ranges_callback_.has_value())
            all_ranges_callback = all_ranges_callback_.value();
        else
            all_ranges_callback = context->getMergeTreeAllRangesCallback();

        if (read_task_callback_.has_value())
            read_task_callback = read_task_callback_.value();
        else
            read_task_callback = context->getMergeTreeReadTaskCallback();
    }

    const auto & settings = context->getSettingsRef();
    if (settings[Setting::max_streams_for_merge_tree_reading])
    {
        if (settings[Setting::allow_asynchronous_read_from_io_pool_for_merge_tree])
        {
            /// When async reading is enabled, allow to read using more streams.
            /// Will add resize to output_streams_limit to reduce memory usage.
            output_streams_limit = std::min<size_t>(requested_num_streams, settings[Setting::max_streams_for_merge_tree_reading]);
            /// We intentionally set `max_streams` to 1 in InterpreterSelectQuery in case of small limit.
            /// Changing it here to `max_streams_for_merge_tree_reading` proven itself as a threat for performance.
            if (requested_num_streams != 1)
                requested_num_streams = std::max<size_t>(requested_num_streams, settings[Setting::max_streams_for_merge_tree_reading]);
        }
        else
            /// Just limit requested_num_streams otherwise.
            requested_num_streams = std::min<size_t>(requested_num_streams, settings[Setting::max_streams_for_merge_tree_reading]);
    }

    /// Add explicit description.
    setStepDescription(data.getStorageID().getFullNameNotQuoted());
    enable_vertical_final = query_info.isFinal() && context->getSettingsRef()[Setting::enable_vertical_final]
        && data.merging_params.mode == MergeTreeData::MergingParams::Replacing;
}

std::unique_ptr<ReadFromMergeTree> ReadFromMergeTree::createLocalParallelReplicasReadingStep(
    AnalysisResultPtr analyzed_result_ptr_,
    MergeTreeAllRangesCallback all_ranges_callback_,
    MergeTreeReadTaskCallback read_task_callback_,
    size_t replica_number)
{
    const bool enable_parallel_reading = true;
    return std::make_unique<ReadFromMergeTree>(
        getParts(),
        mutations_snapshot,
        all_column_names,
        data,
        getQueryInfo(),
        getStorageSnapshot(),
        getContext(),
        block_size.max_block_size_rows,
        requested_num_streams,
        max_block_numbers_to_read,
        log,
        std::move(analyzed_result_ptr_),
        enable_parallel_reading,
        all_ranges_callback_,
        read_task_callback_,
        replica_number);
}

Pipe ReadFromMergeTree::readFromPoolParallelReplicas(RangesInDataParts parts_with_range, Names required_columns, PoolSettings pool_settings)
{
    const auto & client_info = context->getClientInfo();

    auto extension = ParallelReadingExtension{
        all_ranges_callback.value(),
        read_task_callback.value(),
        number_of_current_replica.value_or(client_info.number_of_current_replica),
        context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount()};

    auto pool = std::make_shared<MergeTreeReadPoolParallelReplicas>(
        std::move(extension),
        std::move(parts_with_range),
        mutations_snapshot,
        shared_virtual_fields,
        storage_snapshot,
        prewhere_info,
        actions_settings,
        reader_settings,
        required_columns,
        pool_settings,
        block_size,
        context);

    Pipes pipes;

    for (size_t i = 0; i < pool_settings.threads; ++i)
    {
        auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(i);

        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool, std::move(algorithm), prewhere_info, lazily_read_info, actions_settings, reader_settings);

        auto source = std::make_shared<MergeTreeSource>(std::move(processor), data.getLogName());
        pipes.emplace_back(std::move(source));
    }

    return Pipe::unitePipes(std::move(pipes));
}


Pipe ReadFromMergeTree::readFromPool(
    RangesInDataParts parts_with_range,
    Names required_columns,
    PoolSettings pool_settings)
{
    size_t total_rows = parts_with_range.getRowsCountAllParts();

    if (query_info.trivial_limit > 0 && query_info.trivial_limit < total_rows)
        total_rows = query_info.trivial_limit;

    const auto & settings = context->getSettingsRef();

    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    /// If granularity is adaptive it doesn't make sense
    /// Maybe it will make sense to add settings `max_block_size_bytes`
    if (block_size.max_block_size_rows && !data.canUseAdaptiveGranularity())
    {
        size_t fixed_index_granularity = (*data.getSettings())[MergeTreeSetting::index_granularity];
        pool_settings.min_marks_for_concurrent_read = (pool_settings.min_marks_for_concurrent_read * fixed_index_granularity + block_size.max_block_size_rows - 1)
            / block_size.max_block_size_rows * block_size.max_block_size_rows / fixed_index_granularity;
    }

    bool all_parts_are_remote = true;
    bool all_parts_are_local = true;
    for (const auto & part : parts_with_range)
    {
        const bool is_remote = part.data_part->isStoredOnRemoteDisk();
        all_parts_are_local &= !is_remote;
        all_parts_are_remote &= is_remote;
    }

    MergeTreeReadPoolPtr pool;

    bool allow_prefetched_remote = all_parts_are_remote && settings[Setting::allow_prefetched_read_pool_for_remote_filesystem]
        && MergeTreePrefetchedReadPool::checkReadMethodAllowed(reader_settings.read_settings.remote_fs_method);

    bool allow_prefetched_local = all_parts_are_local && settings[Setting::allow_prefetched_read_pool_for_local_filesystem]
        && MergeTreePrefetchedReadPool::checkReadMethodAllowed(reader_settings.read_settings.local_fs_method);

    /** Do not use prefetched read pool if query is trivial limit query.
      * Because time spend during filling per thread tasks can be greater than whole query
      * execution for big tables with small limit.
      */
    bool use_prefetched_read_pool = query_info.trivial_limit == 0 && (allow_prefetched_remote || allow_prefetched_local);

    if (use_prefetched_read_pool)
    {
        pool = std::make_shared<MergeTreePrefetchedReadPool>(
            std::move(parts_with_range),
            mutations_snapshot,
            shared_virtual_fields,
            storage_snapshot,
            prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            pool_settings,
            block_size,
            context);
    }
    else
    {
        pool = std::make_shared<MergeTreeReadPool>(
            std::move(parts_with_range),
            mutations_snapshot,
            shared_virtual_fields,
            storage_snapshot,
            prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            pool_settings,
            block_size,
            context);
    }

    LOG_DEBUG(log, "Reading approx. {} rows with {} streams", total_rows, pool_settings.threads);

    Pipes pipes;
    for (size_t i = 0; i < pool_settings.threads; ++i)
    {
        auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(i);

        auto processor
            = std::make_unique<MergeTreeSelectProcessor>(pool, std::move(algorithm), prewhere_info, lazily_read_info, actions_settings, reader_settings);

        auto source = std::make_shared<MergeTreeSource>(std::move(processor), data.getLogName());

        if (i == 0)
            source->addTotalRowsApprox(total_rows);

        pipes.emplace_back(std::move(source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (output_streams_limit && output_streams_limit < pipe.numOutputPorts())
        pipe.resize(output_streams_limit);
    return pipe;
}

Pipe ReadFromMergeTree::readInOrder(
    RangesInDataParts parts_with_ranges,
    Names required_columns,
    PoolSettings pool_settings,
    ReadType read_type,
    UInt64 read_limit)
{
    /// For reading in order it makes sense to read only
    /// one range per task to reduce number of read rows.
    bool has_limit_below_one_block = read_type != ReadType::Default && read_limit && read_limit < block_size.max_block_size_rows;
    MergeTreeReadPoolPtr pool;

    if (is_parallel_reading_from_replicas)
    {
        const auto & client_info = context->getClientInfo();
        ParallelReadingExtension extension{
            all_ranges_callback.value(),
            read_task_callback.value(),
            number_of_current_replica.value_or(client_info.number_of_current_replica), context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount()};

        CoordinationMode mode = read_type == ReadType::InOrder
            ? CoordinationMode::WithOrder
            : CoordinationMode::ReverseOrder;

        pool = std::make_shared<MergeTreeReadPoolParallelReplicasInOrder>(
            std::move(extension),
            mode,
            parts_with_ranges,
            mutations_snapshot,
            shared_virtual_fields,
            has_limit_below_one_block,
            storage_snapshot,
            prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            pool_settings,
            block_size,
            context);
    }
    else
    {
        pool = std::make_shared<MergeTreeReadPoolInOrder>(
            has_limit_below_one_block,
            read_type,
            parts_with_ranges,
            mutations_snapshot,
            shared_virtual_fields,
            storage_snapshot,
            prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            pool_settings,
            block_size,
            context);
    }

    /// If parallel replicas enabled, set total rows in progress here only on initiator with local plan
    /// Otherwise rows will counted multiple times
    const UInt64 in_order_limit = query_info.input_order_info ? query_info.input_order_info->limit : 0;
    const bool parallel_replicas_local_plan_for_initiator = is_parallel_reading_from_replicas
        && context->getSettingsRef()[Setting::parallel_replicas_local_plan] && context->canUseParallelReplicasOnInitiator();
    const bool set_total_rows_approx = !is_parallel_reading_from_replicas || parallel_replicas_local_plan_for_initiator;

    Pipes pipes;
    for (size_t i = 0; i < parts_with_ranges.size(); ++i)
    {
        const auto & part_with_ranges = parts_with_ranges[i];

        UInt64 total_rows = part_with_ranges.getRowsCount();
        if (query_info.trivial_limit > 0 && query_info.trivial_limit < total_rows)
            total_rows = query_info.trivial_limit;
        else if (in_order_limit > 0 && in_order_limit < total_rows)
            total_rows = in_order_limit;

        LOG_TRACE(log, "Reading {} ranges in{}order from part {}, approx. {} rows starting from {}",
            part_with_ranges.ranges.size(),
            read_type == ReadType::InReverseOrder ? " reverse " : " ",
            part_with_ranges.data_part->name, total_rows,
            part_with_ranges.data_part->index_granularity->getMarkStartingRow(part_with_ranges.ranges.front().begin));

        MergeTreeSelectAlgorithmPtr algorithm;
        if (read_type == ReadType::InReverseOrder)
            algorithm = std::make_unique<MergeTreeInReverseOrderSelectAlgorithm>(i);
        else
            algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(i);

        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool, std::move(algorithm), prewhere_info, lazily_read_info, actions_settings, reader_settings);

        processor->addPartLevelToChunk(isQueryWithFinal());

        auto source = std::make_shared<MergeTreeSource>(std::move(processor), data.getLogName());
        if (set_total_rows_approx)
            source->addTotalRowsApprox(total_rows);

        Pipe pipe(source);

        if (virtual_row_conversion && (read_type == ReadType::InOrder))
        {
            const auto & index = part_with_ranges.data_part->getIndex();
            const auto & primary_key = storage_snapshot->metadata->primary_key;
            size_t mark_range_begin = part_with_ranges.ranges.front().begin;

            ColumnsWithTypeAndName pk_columns;
            size_t num_columns = virtual_row_conversion->getRequiredColumnsWithTypes().size();
            pk_columns.reserve(num_columns);

            for (size_t j = 0; j < num_columns; ++j)
            {
                auto column = primary_key.data_types[j]->createColumn()->cloneEmpty();
                column->insert((*(*index)[j])[mark_range_begin]);
                pk_columns.push_back({std::move(column), primary_key.data_types[j], primary_key.column_names[j]});
            }

            Block pk_block(std::move(pk_columns));

            pipe.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<VirtualRowTransform>(header, pk_block, virtual_row_conversion);
            });
        }

        pipes.emplace_back(std::move(pipe));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    if (read_type == ReadType::InReverseOrder)
    {
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ReverseTransform>(header);
        });
    }

    return pipe;
}

Pipe ReadFromMergeTree::read(
    RangesInDataParts parts_with_range,
    Names required_columns,
    ReadType read_type,
    size_t max_streams,
    size_t min_marks_for_concurrent_read,
    bool use_uncompressed_cache)
{
    const auto & settings = context->getSettingsRef();
    size_t sum_marks = parts_with_range.getMarksCountAllParts();

    const size_t total_query_nodes = is_parallel_reading_from_replicas
        ? std::min<size_t>(
              context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount(),
              context->getSettingsRef()[Setting::max_parallel_replicas])
        : 1;

    PoolSettings pool_settings{
        .threads = max_streams,
        .sum_marks = sum_marks,
        .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        .preferred_block_size_bytes = settings[Setting::preferred_block_size_bytes],
        .use_uncompressed_cache = use_uncompressed_cache,
        .use_const_size_tasks_for_remote_reading = settings[Setting::merge_tree_use_const_size_tasks_for_remote_reading],
        .total_query_nodes = total_query_nodes,
    };

    if (read_type == ReadType::ParallelReplicas)
        return readFromPoolParallelReplicas(std::move(parts_with_range), std::move(required_columns), std::move(pool_settings));

    /// Reading from default thread pool is beneficial for remote storage because of new prefetches.
    if (read_type == ReadType::Default && (max_streams > 1 || checkAllPartsOnRemoteFS(parts_with_range)))
        return readFromPool(std::move(parts_with_range), std::move(required_columns), std::move(pool_settings));

    auto pipe = readInOrder(parts_with_range, required_columns, pool_settings, read_type, /*limit=*/ 0);

    /// Use ConcatProcessor to concat sources together.
    /// It is needed to read in parts order (and so in PK order) if single thread is used.
    if (read_type == ReadType::Default && pipe.numOutputPorts() > 1)
        pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));

    return pipe;
}

namespace
{

struct PartRangesReadInfo
{
    std::vector<size_t> sum_marks_in_parts;

    size_t sum_marks = 0;
    size_t total_rows = 0;
    size_t adaptive_parts = 0;
    size_t index_granularity_bytes = 0;
    size_t max_marks_to_use_cache = 0;
    size_t min_marks_for_concurrent_read = 0;
    bool use_uncompressed_cache = false;

    PartRangesReadInfo(
        const RangesInDataParts & parts,
        const Settings & settings,
        const MergeTreeSettings & data_settings)
    {
        /// Count marks for each part.
        sum_marks_in_parts.resize(parts.size());

        for (size_t i = 0; i < parts.size(); ++i)
        {
            total_rows += parts[i].getRowsCount();
            sum_marks_in_parts[i] = parts[i].getMarksCount();
            sum_marks += sum_marks_in_parts[i];

            if (parts[i].data_part->index_granularity_info.mark_type.adaptive)
                ++adaptive_parts;
        }

        if (adaptive_parts > parts.size() / 2)
            index_granularity_bytes = data_settings[MergeTreeSetting::index_granularity_bytes];

        max_marks_to_use_cache = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
            settings[Setting::merge_tree_max_rows_to_use_cache],
            settings[Setting::merge_tree_max_bytes_to_use_cache],
            data_settings[MergeTreeSetting::index_granularity],
            index_granularity_bytes);

        auto all_parts_on_remote_disk = checkAllPartsOnRemoteFS(parts);

        size_t min_rows_for_concurrent_read;
        size_t min_bytes_for_concurrent_read;
        if (all_parts_on_remote_disk)
        {
            min_rows_for_concurrent_read = settings[Setting::merge_tree_min_rows_for_concurrent_read_for_remote_filesystem];
            min_bytes_for_concurrent_read = settings[Setting::merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem];
        }
        else
        {
            min_rows_for_concurrent_read = settings[Setting::merge_tree_min_rows_for_concurrent_read];
            min_bytes_for_concurrent_read = settings[Setting::merge_tree_min_bytes_for_concurrent_read];
        }

        min_marks_for_concurrent_read = MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
            min_rows_for_concurrent_read, min_bytes_for_concurrent_read,
            data_settings[MergeTreeSetting::index_granularity], index_granularity_bytes, settings[Setting::merge_tree_min_read_task_size], sum_marks);

        use_uncompressed_cache = settings[Setting::use_uncompressed_cache];
        if (sum_marks > max_marks_to_use_cache)
            use_uncompressed_cache = false;
    }
};

}

Pipe ReadFromMergeTree::readByLayers(const RangesInDataParts & parts_with_ranges, SplitPartsByRanges split_parts, const Names & column_names, const InputOrderInfoPtr & input_order_info)
{
    const auto & settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();

    LOG_TRACE(log, "Spreading mark ranges among streams (reading by layers)");

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);
    if (0 == info.sum_marks)
        return {};

    ReadingInOrderStepGetter reading_step_getter;
    Names in_order_column_names_to_read;
    SortDescription sort_description;

    if (reader_settings.read_in_order)
    {
        NameSet column_names_set(column_names.begin(), column_names.end());
        in_order_column_names_to_read = column_names;

        /// Add columns needed to calculate the sorting expression
        for (const auto & column_name : storage_snapshot->metadata->getColumnsRequiredForSortingKey())
        {
            if (column_names_set.contains(column_name))
                continue;

            in_order_column_names_to_read.push_back(column_name);
            column_names_set.insert(column_name);
        }
        auto sorting_expr = storage_snapshot->metadata->getSortingKey().expression;
        const auto & sorting_columns = storage_snapshot->metadata->getSortingKey().column_names;
        std::vector<bool> reverse_flags = storage_snapshot->metadata->getSortingKeyReverseFlags();

        sort_description.compile_sort_description = settings[Setting::compile_sort_description];
        sort_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];

        sort_description.reserve(input_order_info->used_prefix_of_sorting_key_size);
        for (size_t i = 0; i < input_order_info->used_prefix_of_sorting_key_size; ++i)
        {
            if (!reverse_flags.empty() && reverse_flags[i])
                sort_description.emplace_back(sorting_columns[i], input_order_info->direction * -1);
            else
                sort_description.emplace_back(sorting_columns[i], input_order_info->direction);
        }

        reading_step_getter = [this, &in_order_column_names_to_read, &info, sorting_expr, &sort_description](auto parts)
        {
            auto pipe = this->read(
                std::move(parts),
                in_order_column_names_to_read,
                ReadType::InOrder,
                1 /* num_streams */,
                0 /* min_marks_for_concurrent_read */,
                info.use_uncompressed_cache);

            if (pipe.empty())
            {
                auto header = MergeTreeSelectProcessor::transformHeader(
                    storage_snapshot->getSampleBlockForColumns(in_order_column_names_to_read),
                    lazily_read_info,
                    query_info.prewhere_info);
                pipe = Pipe(std::make_shared<NullSource>(header));
            }

            pipe.addSimpleTransform([sorting_expr](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, sorting_expr);
            });

            if (pipe.numOutputPorts() != 1)
            {
                auto transform = std::make_shared<MergingSortedTransform>(
                    pipe.getHeader(),
                    pipe.numOutputPorts(),
                    sort_description,
                    block_size.max_block_size_rows,
                    /*max_block_size_bytes=*/0,
                    SortingQueueStrategy::Batch,
                    0,
                    false,
                    nullptr,
                    false,
                    /*apply_virtual_row_conversions*/ false);

                pipe.addTransform(std::move(transform));
            }

            return pipe;
        };
    }
    else
    {
        reading_step_getter = [this, &column_names, &info](auto parts)
        {
            return this->read(
                std::move(parts),
                column_names,
                ReadType::Default,
                1 /* num_streams */,
                info.min_marks_for_concurrent_read,
                info.use_uncompressed_cache);
        };
    }

    auto pipes = ::readByLayers(std::move(split_parts), storage_snapshot->metadata->getPrimaryKey(), std::move(reading_step_getter), false, context);
    return Pipe::unitePipes(std::move(pipes));
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreams(RangesInDataParts && parts_with_ranges, size_t num_streams, const Names & column_names)
{
    const auto & settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();

    LOG_TRACE(log, "Spreading mark ranges among streams (default reading)");

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);
    Names tmp_column_names(column_names.begin(), column_names.end());

    if (0 == info.sum_marks)
        return {};

    if (num_streams > 1)
    {
        /// Reduce the number of num_streams if the data is small.
        if (info.sum_marks < num_streams * info.min_marks_for_concurrent_read && parts_with_ranges.size() < num_streams)
        {
            /*
            If the data is fragmented, then allocate the size of parts to num_streams. If the data is not fragmented, besides the sum_marks and
            min_marks_for_concurrent_read, involve the system cores to get the num_streams. Increase the num_streams and decrease the min_marks_for_concurrent_read
            if the data is small but system has plentiful cores. It helps to improve the parallel performance of `MergeTreeRead` significantly.
            Make sure the new num_streams `num_streams * increase_num_streams_ratio` will not exceed the previous calculated prev_num_streams.
            The new info.min_marks_for_concurrent_read `info.min_marks_for_concurrent_read / increase_num_streams_ratio` should be larger than 8.
            https://github.com/ClickHouse/ClickHouse/pull/53867
            */
            if ((info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read > parts_with_ranges.size())
            {
                const size_t prev_num_streams = num_streams;
                num_streams = (info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read;
                const size_t increase_num_streams_ratio = std::min(prev_num_streams / num_streams, info.min_marks_for_concurrent_read / 8);
                if (increase_num_streams_ratio > 1)
                {
                    num_streams = num_streams * increase_num_streams_ratio;
                    info.min_marks_for_concurrent_read = (info.sum_marks + num_streams - 1) / num_streams;
                }
            }
            else
                num_streams = parts_with_ranges.size();
        }
    }

    auto read_type = is_parallel_reading_from_replicas ? ReadType::ParallelReplicas : ReadType::Default;

    double read_split_ranges_into_intersecting_and_non_intersecting_injection_probability
        = settings[Setting::merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability];
    std::bernoulli_distribution fault(read_split_ranges_into_intersecting_and_non_intersecting_injection_probability);

    if (read_type != ReadType::ParallelReplicas &&
        num_streams > 1 &&
        read_split_ranges_into_intersecting_and_non_intersecting_injection_probability > 0.0 &&
        fault(thread_local_rng) &&
        !isQueryWithFinal() &&
        data.merging_params.is_deleted_column.empty() &&
        !prewhere_info &&
        !lazily_read_info &&
        !reader_settings.use_query_condition_cache) /// the query condition cache produces incorrect results with intersecting ranges
    {
        NameSet column_names_set(column_names.begin(), column_names.end());
        Names in_order_column_names_to_read(column_names);

        /// Add columns needed to calculate the sorting expression
        for (const auto & column_name : storage_snapshot->metadata->getColumnsRequiredForSortingKey())
        {
            if (column_names_set.contains(column_name))
                continue;

            in_order_column_names_to_read.push_back(column_name);
            column_names_set.insert(column_name);
        }

        auto in_order_reading_step_getter = [this, &in_order_column_names_to_read, &info](auto parts)
        {
            return this->read(
                std::move(parts),
                in_order_column_names_to_read,
                ReadType::InOrder,
                1 /* num_streams */,
                0 /* min_marks_for_concurrent_read */,
                info.use_uncompressed_cache);
        };

        auto sorting_expr = storage_snapshot->metadata->getSortingKey().expression;

        SplitPartsWithRangesByPrimaryKeyResult split_ranges_result = splitPartsWithRangesByPrimaryKey(
            storage_snapshot->metadata->getPrimaryKey(),
            storage_snapshot->metadata->getSortingKey(),
            std::move(sorting_expr),
            std::move(parts_with_ranges),
            num_streams,
            context,
            std::move(in_order_reading_step_getter),
            true /*split_parts_ranges_into_intersecting_and_non_intersecting_final*/,
            true /*split_intersecting_parts_ranges_into_layers*/);

        auto merging_pipes = std::move(split_ranges_result.merging_pipes);
        auto non_intersecting_parts_ranges_read_pipe = read(std::move(split_ranges_result.non_intersecting_parts_ranges),
            tmp_column_names,
            read_type,
            num_streams,
            info.min_marks_for_concurrent_read,
            info.use_uncompressed_cache);

        if (merging_pipes.empty())
            return non_intersecting_parts_ranges_read_pipe;

        Pipes pipes;
        pipes.resize(2);
        pipes[0] = Pipe::unitePipes(std::move(merging_pipes));
        pipes[1] = std::move(non_intersecting_parts_ranges_read_pipe);

        auto conversion_action = ActionsDAG::makeConvertingActions(
            pipes[0].getHeader().getColumnsWithTypeAndName(),
            pipes[1].getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);
        auto converting_expr = std::make_shared<ExpressionActions>(std::move(conversion_action));
        pipes[0].addSimpleTransform(
            [converting_expr](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, converting_expr);
            });
        return Pipe::unitePipes(std::move(pipes));
    }

    return read(std::move(parts_with_ranges),
        tmp_column_names,
        read_type,
        num_streams,
        info.min_marks_for_concurrent_read,
        info.use_uncompressed_cache);
}

static ActionsDAG createProjection(const Block & header)
{
    return ActionsDAG(header.getNamesAndTypesList());
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsWithOrder(
    RangesInDataParts && parts_with_ranges,
    size_t num_streams,
    const Names & column_names,
    std::optional<ActionsDAG> & out_projection,
    const InputOrderInfoPtr & input_order_info)
{
    const auto & settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();

    LOG_TRACE(log, "Spreading ranges among streams with order");

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);

    Pipes res;

    if (info.sum_marks == 0)
        return {};

    /// PREWHERE actions can remove some input columns (which are needed only for prewhere condition).
    /// In case of read-in-order, PREWHERE is executed before sorting. But removed columns could be needed for sorting key.
    /// To fix this, we prohibit removing any input in prewhere actions. Instead, projection actions will be added after sorting.
    /// See 02354_read_in_order_prewhere.sql as an example.
    bool have_input_columns_removed_after_prewhere = false;
    if (prewhere_info)
    {
        NameSet sorting_columns;
        for (const auto & column : storage_snapshot->metadata->getSortingKey().expression->getRequiredColumnsWithTypes())
            sorting_columns.insert(column.name);

        have_input_columns_removed_after_prewhere = restorePrewhereInputs(*prewhere_info, sorting_columns);
    }

    /// Let's split ranges to avoid reading much data.
    auto split_ranges
        = [rows_granularity = (*data_settings)[MergeTreeSetting::index_granularity], my_max_block_size = block_size.max_block_size_rows]
        (const auto & ranges, int direction)
    {
        MarkRanges new_ranges;
        const size_t max_marks_in_range = (my_max_block_size + rows_granularity - 1) / rows_granularity;
        size_t marks_in_range = 1;

        if (direction == 1)
        {
            /// Split first few ranges to avoid reading much data.
            bool split = false;
            for (auto range : ranges)
            {
                while (!split && range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_back(range.begin, range.begin + marks_in_range);
                    range.begin += marks_in_range;
                    marks_in_range *= 2;

                    if (marks_in_range > max_marks_in_range)
                        split = true;
                }
                new_ranges.emplace_back(range.begin, range.end);
            }
        }
        else
        {
            /// Split all ranges to avoid reading much data, because we have to
            ///  store whole range in memory to reverse it.
            for (auto it = ranges.rbegin(); it != ranges.rend(); ++it)
            {
                auto range = *it;
                while (range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_front(range.end - marks_in_range, range.end);
                    range.end -= marks_in_range;
                    marks_in_range = std::min(marks_in_range * 2, max_marks_in_range);
                }
                new_ranges.emplace_front(range.begin, range.end);
            }
        }

        return new_ranges;
    };

    if (num_streams > 1)
    {
        /// Reduce num_streams if requested value is unnecessarily large.
        ///
        /// Additional increase of streams number in case of skewed parts, like it's
        /// done in `spreadMarkRangesAmongStreams` won't affect overall performance
        /// due to the single downstream `MergingSortedTransform`.
        if (info.sum_marks < num_streams * info.min_marks_for_concurrent_read && parts_with_ranges.size() < num_streams)
        {
            num_streams = std::max(
                (info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read, parts_with_ranges.size());
        }
    }

    const size_t min_marks_per_stream = (info.sum_marks - 1) / num_streams + 1;
    bool need_preliminary_merge = (parts_with_ranges.size() > settings[Setting::read_in_order_two_level_merge_threshold]);

    const auto read_type = input_order_info->direction == 1 ? ReadType::InOrder : ReadType::InReverseOrder;

    const size_t total_query_nodes = is_parallel_reading_from_replicas
        ? std::min<size_t>(
              context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount(),
              context->getSettingsRef()[Setting::max_parallel_replicas])
        : 1;

    PoolSettings pool_settings{
        .threads = num_streams,
        .sum_marks = parts_with_ranges.getMarksCountAllParts(),
        .min_marks_for_concurrent_read = info.min_marks_for_concurrent_read,
        .preferred_block_size_bytes = settings[Setting::preferred_block_size_bytes],
        .use_uncompressed_cache = info.use_uncompressed_cache,
        .total_query_nodes = total_query_nodes,
    };

    Pipes pipes;
    /// For parallel replicas the split will be performed on the initiator side.
    if (is_parallel_reading_from_replicas)
    {
        pipes.emplace_back(readInOrder(std::move(parts_with_ranges), column_names, pool_settings, read_type, input_order_info->limit));
    }
    else
    {
        std::vector<RangesInDataParts> split_parts_and_ranges;
        split_parts_and_ranges.reserve(num_streams);

        for (size_t i = 0; i < num_streams && !parts_with_ranges.empty(); ++i)
        {
            size_t need_marks = min_marks_per_stream;
            RangesInDataParts new_parts;

            /// Loop over parts.
            /// We will iteratively take part or some subrange of a part from the back
            ///  and assign a stream to read from it.
            while (need_marks > 0 && !parts_with_ranges.empty())
            {
                RangesInDataPart part = parts_with_ranges.back();
                parts_with_ranges.pop_back();
                size_t & marks_in_part = info.sum_marks_in_parts.back();

                /// We will not take too few rows from a part.
                if (marks_in_part >= info.min_marks_for_concurrent_read && need_marks < info.min_marks_for_concurrent_read)
                    need_marks = info.min_marks_for_concurrent_read;

                /// Do not leave too few rows in the part.
                if (marks_in_part > need_marks && marks_in_part - need_marks < info.min_marks_for_concurrent_read)
                    need_marks = marks_in_part;

                MarkRanges ranges_to_get_from_part;

                /// We take full part if it contains enough marks or
                /// if we know limit and part contains less than 'limit' rows.
                bool take_full_part = marks_in_part <= need_marks || (input_order_info->limit && input_order_info->limit < part.getRowsCount());

                /// We take the whole part if it is small enough.
                if (take_full_part)
                {
                    ranges_to_get_from_part = part.ranges;

                    need_marks -= marks_in_part;
                    info.sum_marks_in_parts.pop_back();
                }
                else
                {
                    /// Loop through ranges in part. Take enough ranges to cover "need_marks".
                    while (need_marks > 0)
                    {
                        if (part.ranges.empty())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected end of ranges while spreading marks among streams");

                        MarkRange & range = part.ranges.front();

                        const size_t marks_in_range = range.end - range.begin;
                        const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                        ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                        range.begin += marks_to_get_from_range;
                        marks_in_part -= marks_to_get_from_range;
                        need_marks -= marks_to_get_from_range;
                        if (range.begin == range.end)
                            part.ranges.pop_front();
                    }
                    parts_with_ranges.emplace_back(part);
                }

                ranges_to_get_from_part = split_ranges(ranges_to_get_from_part, input_order_info->direction);
                new_parts.emplace_back(
                    part.data_part,
                    part.parent_part,
                    part.part_index_in_query,
                    part.part_starting_offset_in_query,
                    std::move(ranges_to_get_from_part));
            }

            split_parts_and_ranges.emplace_back(std::move(new_parts));
        }

        for (auto && item : split_parts_and_ranges)
            pipes.emplace_back(readInOrder(std::move(item), column_names, pool_settings, read_type, input_order_info->limit));
    }

    Block pipe_header;
    if (!pipes.empty())
        pipe_header = pipes.front().getHeader();

    if (need_preliminary_merge || output_each_partition_through_separate_port)
    {
        size_t prefix_size = input_order_info->used_prefix_of_sorting_key_size;
        auto order_key_prefix_ast = storage_snapshot->metadata->getSortingKey().expression_list_ast->clone();
        order_key_prefix_ast->children.resize(prefix_size);

        auto syntax_result = TreeRewriter(context).analyze(order_key_prefix_ast, storage_snapshot->metadata->getColumns().get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()));
        auto sorting_key_prefix_expr = ExpressionAnalyzer(order_key_prefix_ast, syntax_result, context).getActionsDAG(false);
        const auto & sorting_columns = storage_snapshot->metadata->getSortingKey().column_names;
        std::vector<bool> reverse_flags = storage_snapshot->metadata->getSortingKeyReverseFlags();

        SortDescription sort_description;
        sort_description.compile_sort_description = settings[Setting::compile_sort_description];
        sort_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];

        sort_description.reserve(prefix_size);
        for (size_t i = 0; i < prefix_size; ++i)
        {
            if (!reverse_flags.empty() && reverse_flags[i])
                sort_description.emplace_back(sorting_columns[i], input_order_info->direction * -1);
            else
                sort_description.emplace_back(sorting_columns[i], input_order_info->direction);
        }

        auto sorting_key_expr = std::make_shared<ExpressionActions>(std::move(sorting_key_prefix_expr));

        auto merge_streams = [&](Pipe & pipe)
        {
            pipe.addSimpleTransform([sorting_key_expr](const Block & header)
                                    { return std::make_shared<ExpressionTransform>(header, sorting_key_expr); });

            if (pipe.numOutputPorts() > 1)
            {
                auto transform = std::make_shared<MergingSortedTransform>(
                    pipe.getHeader(),
                    pipe.numOutputPorts(),
                    sort_description,
                    block_size.max_block_size_rows,
                    /*max_block_size_bytes=*/0,
                    SortingQueueStrategy::Batch,
                    0,
                    false,
                    nullptr,
                    false,
                    /*apply_virtual_row_conversions*/ false);

                pipe.addTransform(std::move(transform));
            }
        };

        if (!pipes.empty() && output_each_partition_through_separate_port)
        {
            /// In contrast with usual aggregation in order that allocates separate AggregatingTransform for each data part,
            /// aggregation of partitioned data uses the same AggregatingTransform for all parts of the same partition.
            /// Thus we need to merge all partition parts into a single sorted stream.
            Pipe pipe = Pipe::unitePipes(std::move(pipes));
            merge_streams(pipe);
            return pipe;
        }

        for (auto & pipe : pipes)
            merge_streams(pipe);
    }

    if (!pipes.empty() && (need_preliminary_merge || have_input_columns_removed_after_prewhere))
        /// Drop temporary columns, added by 'sorting_key_prefix_expr'
        out_projection = createProjection(pipe_header);

    return Pipe::unitePipes(std::move(pipes));
}

static void addMergingFinal(
    Pipe & pipe,
    const SortDescription & sort_description,
    MergeTreeData::MergingParams merging_params,
    const StorageMetadataPtr & metadata_snapshot,
    size_t max_block_size_rows,
    bool enable_vertical_final)
{
    const auto & header = pipe.getHeader();
    size_t num_outputs = pipe.numOutputPorts();

    auto now = time(nullptr);

    auto get_merging_processor = [&]() -> MergingTransformPtr
    {
        switch (merging_params.mode)
        {
            case MergeTreeData::MergingParams::Ordinary:
                return std::make_shared<MergingSortedTransform>(header, num_outputs,
                            sort_description, max_block_size_rows, /*max_block_size_bytes=*/0, SortingQueueStrategy::Batch);

            case MergeTreeData::MergingParams::Collapsing:
                return std::make_shared<CollapsingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.sign_column, true, max_block_size_rows, /*max_block_size_bytes=*/0);

            case MergeTreeData::MergingParams::Summing: {
                auto required_columns = metadata_snapshot->getPartitionKey().expression->getRequiredColumns();
                required_columns.append_range(metadata_snapshot->getSortingKey().expression->getRequiredColumns());
                return std::make_shared<SummingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.columns_to_sum, required_columns, max_block_size_rows, /*max_block_size_bytes=*/0);
            }

            case MergeTreeData::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedTransform>(header, num_outputs,
                            sort_description, max_block_size_rows, /*max_block_size_bytes=*/0);

            case MergeTreeData::MergingParams::Replacing:
                return std::make_shared<ReplacingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.is_deleted_column, merging_params.version_column, max_block_size_rows, /*max_block_size_bytes=*/0, /*out_row_sources_buf_*/ nullptr, /*use_average_block_sizes*/ false, /*cleanup*/ !merging_params.is_deleted_column.empty(), enable_vertical_final);


            case MergeTreeData::MergingParams::VersionedCollapsing:
                return std::make_shared<VersionedCollapsingTransform>(header, num_outputs,
                            sort_description, merging_params.sign_column, max_block_size_rows, /*max_block_size_bytes=*/0);

            case MergeTreeData::MergingParams::Graphite:
                return std::make_shared<GraphiteRollupSortedTransform>(header, num_outputs,
                            sort_description, max_block_size_rows, /*max_block_size_bytes=*/0, merging_params.graphite_params, now);

            case MergeTreeData::MergingParams::Coalescing:
            {
                auto required_columns = metadata_snapshot->getPartitionKey().expression->getRequiredColumns();
                required_columns.append_range(metadata_snapshot->getSortingKey().expression->getRequiredColumns());
                return std::make_shared<CoalescingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.columns_to_sum, required_columns, max_block_size_rows, /*max_block_size_bytes=*/0);
            }
        }
    };

    pipe.addTransform(get_merging_processor());
    if (enable_vertical_final)
        pipe.addSimpleTransform([](const Block & header_)
                                { return std::make_shared<SelectByIndicesTransform>(header_); });
}

static std::pair<std::shared_ptr<ExpressionActions>, String> createExpressionForPositiveSign(const String & sign_column_name, const Block & header, const ContextPtr & context)
{
    ASTPtr sign_indentifier = std::make_shared<ASTIdentifier>(sign_column_name);
    ASTPtr sign_filter = makeASTFunction("equals", sign_indentifier, std::make_shared<ASTLiteral>(Field(static_cast<Int8>(1))));
    const auto & sign_column = header.getByName(sign_column_name);

    auto syntax_result = TreeRewriter(context).analyze(sign_filter, {{sign_column.name, sign_column.type}});
    auto actions = ExpressionAnalyzer(sign_filter, syntax_result, context).getActionsDAG(false);
    return {std::make_shared<ExpressionActions>(std::move(actions)), sign_filter->getColumnName()};
}

bool ReadFromMergeTree::doNotMergePartsAcrossPartitionsFinal() const
{
    const auto & settings = context->getSettingsRef();

    /// If setting do_not_merge_across_partitions_select_final is set always prefer it
    if (settings[Setting::do_not_merge_across_partitions_select_final].changed)
        return settings[Setting::do_not_merge_across_partitions_select_final];

    if (!storage_snapshot->metadata->hasPrimaryKey() || !storage_snapshot->metadata->hasPartitionKey())
        return false;

    /** To avoid merging parts across partitions we want result of partition key expression for
      * rows with same primary key to be the same.
      *
      * If partition key expression is deterministic, and contains only columns that are included
      * in primary key, then for same primary key column values, result of partition key expression
      * will be the same.
      */
    const auto & partition_key_expression = storage_snapshot->metadata->getPartitionKey().expression;
    if (partition_key_expression->getActionsDAG().hasNonDeterministic())
        return false;

    const auto & primary_key_columns = storage_snapshot->metadata->getPrimaryKey().column_names;
    NameSet primary_key_columns_set(primary_key_columns.begin(), primary_key_columns.end());

    const auto & partition_key_required_columns = partition_key_expression->getRequiredColumns();
    for (const auto & partition_key_required_column : partition_key_required_columns)
        if (!primary_key_columns_set.contains(partition_key_required_column))
            return false;

    return true;
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsFinal(
    RangesInDataParts && parts_with_ranges, size_t num_streams, const Names & origin_column_names, const Names & column_names, std::optional<ActionsDAG> & out_projection)
{
    const auto & settings = context->getSettingsRef();
    const auto & data_settings = data.getSettings();
    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);

    assert(num_streams == requested_num_streams);
    num_streams = std::min<size_t>(num_streams, settings[Setting::max_final_threads]);

    /// If setting do_not_merge_across_partitions_select_final is true than we won't merge parts from different partitions.
    /// We have all parts in parts vector, where parts with same partition are nearby.
    /// So we will store iterators pointed to the beginning of each partition range (and parts.end()),
    /// then we will create a pipe for each partition that will run selecting processor and merging processor
    /// for the parts with this partition. In the end we will unite all the pipes.
    std::vector<RangesInDataParts::iterator> parts_to_merge_ranges;
    auto it = parts_with_ranges.begin();
    parts_to_merge_ranges.push_back(it);

    bool do_not_merge_across_partitions_select_final = doNotMergePartsAcrossPartitionsFinal();
    if (do_not_merge_across_partitions_select_final)
    {
        while (it != parts_with_ranges.end())
        {
            it = std::find_if(
                it, parts_with_ranges.end(), [&it](auto & part) { return it->data_part->info.getPartitionId() != part.data_part->info.getPartitionId(); });
            parts_to_merge_ranges.push_back(it);
        }
    }
    else
    {
        /// If do_not_merge_across_partitions_select_final is false we just merge all the parts.
        parts_to_merge_ranges.push_back(parts_with_ranges.end());
    }

    Pipes merging_pipes;
    Pipes no_merging_pipes;

    /// If do_not_merge_across_partitions_select_final is true and num_streams > 1
    /// we will store lonely parts with level > 0 to use parallel select on them.
    RangesInDataParts non_intersecting_parts_by_primary_key;

    auto sorting_expr = storage_snapshot->metadata->getSortingKey().expression;

    if (prewhere_info)
    {
        NameSet sorting_columns;
        for (const auto & column : storage_snapshot->metadata->getSortingKey().expression->getRequiredColumnsWithTypes())
            sorting_columns.insert(column.name);
        restorePrewhereInputs(*prewhere_info, sorting_columns);
    }

    for (size_t range_index = 0; range_index < parts_to_merge_ranges.size() - 1; ++range_index)
    {
        /// If do_not_merge_across_partitions_select_final is true and there is only one part in partition
        /// with level > 0 then we won't post-process this part, and if num_streams > 1 we
        /// can use parallel select on such parts.
        bool no_merging_final = do_not_merge_across_partitions_select_final &&
            std::distance(parts_to_merge_ranges[range_index], parts_to_merge_ranges[range_index + 1]) == 1 &&
            parts_to_merge_ranges[range_index]->data_part->info.level > 0 &&
            data.merging_params.is_deleted_column.empty() && !reader_settings.read_in_order;

        if (no_merging_final)
        {
            non_intersecting_parts_by_primary_key.push_back(std::move(*parts_to_merge_ranges[range_index]));
            continue;
        }

        Pipes pipes;
        {
            RangesInDataParts new_parts;

            for (auto part_it = parts_to_merge_ranges[range_index]; part_it != parts_to_merge_ranges[range_index + 1]; ++part_it)
            {
                new_parts.emplace_back(
                    part_it->data_part,
                    part_it->parent_part,
                    part_it->part_index_in_query,
                    part_it->part_starting_offset_in_query,
                    part_it->ranges);
            }

            if (new_parts.empty())
                continue;

            if (num_streams > 1 && storage_snapshot->metadata->hasPrimaryKey())
            {
                // Let's split parts into non intersecting parts ranges and layers to ensure data parallelism of FINAL.
                auto in_order_reading_step_getter = [this, &column_names, &info](auto parts)
                {
                    return this->read(
                        std::move(parts),
                        column_names,
                        ReadType::InOrder,
                        1 /* num_streams */,
                        0 /* min_marks_for_concurrent_read */,
                        info.use_uncompressed_cache);
                };

                /// Parts of non-zero level still may contain duplicate PK values to merge on FINAL if there's is_deleted column,
                /// so we have to process all ranges. It would be more optimal to remove this flag and add an extra filtering step.
                bool split_parts_ranges_into_intersecting_and_non_intersecting_final
                    = settings[Setting::split_parts_ranges_into_intersecting_and_non_intersecting_final]
                    && data.merging_params.is_deleted_column.empty() && !reader_settings.read_in_order;

                SplitPartsWithRangesByPrimaryKeyResult split_ranges_result = splitPartsWithRangesByPrimaryKey(
                    storage_snapshot->metadata->getPrimaryKey(),
                    storage_snapshot->metadata->getSortingKey(),
                    sorting_expr,
                    std::move(new_parts),
                    num_streams,
                    context,
                    std::move(in_order_reading_step_getter),
                    split_parts_ranges_into_intersecting_and_non_intersecting_final,
                    settings[Setting::split_intersecting_parts_ranges_into_layers_final]);

                for (auto && non_intersecting_parts_range : split_ranges_result.non_intersecting_parts_ranges)
                    non_intersecting_parts_by_primary_key.push_back(std::move(non_intersecting_parts_range));

                for (auto && merging_pipe : split_ranges_result.merging_pipes)
                    pipes.push_back(std::move(merging_pipe));
            }
            else
            {
                pipes.emplace_back(read(
                    std::move(new_parts), column_names, ReadType::InOrder, num_streams, 0, info.use_uncompressed_cache));

                pipes.back().addSimpleTransform([sorting_expr](const Block & header)
                                                { return std::make_shared<ExpressionTransform>(header, sorting_expr); });
            }

            /// Drop temporary columns, added by 'sorting_key_expr'
            if (!out_projection && !pipes.empty())
                out_projection = createProjection(pipes.front().getHeader());
        }

        if (pipes.empty())
            continue;

        Names sort_columns = storage_snapshot->metadata->getSortingKeyColumns();
        std::vector<bool> reverse_flags = storage_snapshot->metadata->getSortingKeyReverseFlags();
        SortDescription sort_description;
        sort_description.compile_sort_description = settings[Setting::compile_sort_description];
        sort_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];

        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);

        for (size_t i = 0; i < sort_columns_size; ++i)
        {
            if (!reverse_flags.empty() && reverse_flags[i])
                sort_description.emplace_back(sort_columns[i], -1);
            else
                sort_description.emplace_back(sort_columns[i], 1);
        }

        for (auto & pipe : pipes)
            addMergingFinal(
                pipe,
                sort_description,
                data.merging_params,
                storage_snapshot->metadata,
                block_size.max_block_size_rows,
                enable_vertical_final);

        merging_pipes.emplace_back(Pipe::unitePipes(std::move(pipes)));
    }

    if (!non_intersecting_parts_by_primary_key.empty())
    {
        Pipe pipe;

        /// Collapsing algorithm doesn't expose non-matched rows with a negative sign in queries with FINAL.
        /// To support this logic without merging data, add a filtering by sign column for non-intersecting ranges.
        if (data.merging_params.mode == MergeTreeData::MergingParams::Collapsing)
        {
            auto columns_with_sign = origin_column_names;
            if (std::ranges::find(columns_with_sign, data.merging_params.sign_column) == columns_with_sign.end())
                columns_with_sign.push_back(data.merging_params.sign_column);

            pipe = spreadMarkRangesAmongStreams(std::move(non_intersecting_parts_by_primary_key), num_streams, columns_with_sign);
            auto [expression, filter_name] = createExpressionForPositiveSign(data.merging_params.sign_column, pipe.getHeader(), context);

            pipe.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FilterTransform>(header, expression, filter_name, true);
            });
        }
        else
        {
            pipe = spreadMarkRangesAmongStreams(std::move(non_intersecting_parts_by_primary_key), num_streams, origin_column_names);
        }

        no_merging_pipes.emplace_back(std::move(pipe));
    }

    if (!merging_pipes.empty() && !no_merging_pipes.empty())
    {
        out_projection = {}; /// We do projection here
        Pipes pipes;
        pipes.resize(2);
        pipes[0] = Pipe::unitePipes(std::move(merging_pipes));
        pipes[1] = Pipe::unitePipes(std::move(no_merging_pipes));
        auto conversion_action = ActionsDAG::makeConvertingActions(
            pipes[0].getHeader().getColumnsWithTypeAndName(),
            pipes[1].getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);
        auto converting_expr = std::make_shared<ExpressionActions>(std::move(conversion_action));
        pipes[0].addSimpleTransform(
            [converting_expr](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, converting_expr);
            });
        return Pipe::unitePipes(std::move(pipes));
    }
    return merging_pipes.empty() ? Pipe::unitePipes(std::move(no_merging_pipes)) : Pipe::unitePipes(std::move(merging_pipes));
}

ReadFromMergeTree::AnalysisResultPtr ReadFromMergeTree::selectRangesToRead(bool find_exact_ranges) const
{
    analyzed_result_ptr = selectRangesToRead(
        getParts(),
        mutations_snapshot,
        vector_search_parameters,
        storage_snapshot->metadata,
        query_info,
        context,
        requested_num_streams,
        max_block_numbers_to_read,
        data,
        all_column_names,
        log,
        indexes,
        find_exact_ranges);
    return analyzed_result_ptr;
}

static void buildIndexes(
    std::optional<ReadFromMergeTree::Indexes> & indexes,
    const ActionsDAG * filter_actions_dag,
    const MergeTreeData & data,
    const RangesInDataParts & parts,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
    [[maybe_unused]] const std::optional<VectorSearchParameters> & vector_search_parameters,
    const ContextPtr & context,
    const SelectQueryInfo & query_info,
    const StorageMetadataPtr & metadata_snapshot,
    const LoggerPtr & log)
{
    indexes.reset();

    // Build and check if primary key is used when necessary
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    const Names & primary_key_column_names = primary_key.column_names;

    const auto & settings = context->getSettingsRef();

    ActionsDAGWithInversionPushDown filter_dag((filter_actions_dag ? filter_actions_dag->getOutputs().front() : nullptr), context);

    indexes.emplace(
        ReadFromMergeTree::Indexes{KeyCondition{filter_dag, context, primary_key_column_names, primary_key.expression}});

    if (metadata_snapshot->hasPartitionKey())
    {
        const auto & partition_key = metadata_snapshot->getPartitionKey();
        auto minmax_columns_names = MergeTreeData::getMinMaxColumnsNames(partition_key);
        auto minmax_expression_actions = MergeTreeData::getMinMaxExpr(partition_key, ExpressionActionsSettings(context));

        indexes->minmax_idx_condition.emplace(filter_dag, context, minmax_columns_names, minmax_expression_actions);
        indexes->partition_pruner.emplace(metadata_snapshot, filter_dag, context, false /* strict */);
    }

    indexes->part_values
        = MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(metadata_snapshot, data, parts, filter_dag.predicate, context);

    /// Perform virtual column key analysis only when no corresponding physical columns exist.
    const auto & columns = metadata_snapshot->getColumns();
    if (!columns.has("_part_offset") && !columns.has("_part"))
        MergeTreeDataSelectExecutor::buildKeyConditionFromPartOffset(indexes->part_offset_condition, filter_dag.predicate, context);
    if (!columns.has("_part_offset") && !columns.has("_part_starting_offset"))
        MergeTreeDataSelectExecutor::buildKeyConditionFromTotalOffset(indexes->total_offset_condition, filter_dag.predicate, context);

    indexes->use_skip_indexes = settings[Setting::use_skip_indexes];
    if (query_info.isFinal() && !settings[Setting::use_skip_indexes_if_final])
        indexes->use_skip_indexes = false;

    if (!indexes->use_skip_indexes)
        return;

    const auto & all_indexes = metadata_snapshot->getSecondaryIndices();

    if (all_indexes.empty())
        return;

    std::unordered_set<std::string> ignored_index_names;

    if (settings[Setting::ignore_data_skipping_indices].changed)
    {
        const auto & indices = settings[Setting::ignore_data_skipping_indices].toString();
        Tokens tokens(indices.data(), indices.data() + indices.size(), settings[Setting::max_query_size]);
        IParser::Pos pos(tokens, static_cast<unsigned>(settings[Setting::max_parser_depth]), static_cast<unsigned>(settings[Setting::max_parser_backtracks]));
        Expected expected;

        /// Use an unordered list rather than string vector
        auto parse_single_id_or_literal = [&]
        {
            String str;
            if (!parseIdentifierOrStringLiteral(pos, expected, str))
                return false;

            ignored_index_names.insert(std::move(str));
            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_single_id_or_literal, false))
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse ignore_data_skipping_indices ('{}')", indices);
    }

    auto all_updated_columns = mutations_snapshot->getAllUpdatedColumns();

    UsefulSkipIndexes skip_indexes;
    using Key = std::pair<String, size_t>;
    std::map<Key, size_t> merged;

    for (const auto & index : all_indexes)
    {
        if (ignored_index_names.contains(index.name))
            continue;

        auto index_helper = MergeTreeIndexFactory::instance().get(index);

        if (!all_updated_columns.empty())
        {
            auto required_columns = index_helper->getColumnsRequiredForIndexCalc();
            auto it = std::ranges::find_if(required_columns, [&](const auto & column_name)
            {
                return all_updated_columns.contains(column_name);
            });

            if (it != required_columns.end())
            {
                LOG_TRACE(log, "Index {} is not used because it depends on column {} which will be updated on fly", index.name, *it);
                continue;
            }
        }

        if (index_helper->isMergeable())
        {
            auto [it, inserted] = merged.emplace(Key{index_helper->index.type, index_helper->getGranularity()}, skip_indexes.merged_indices.size());
            if (inserted)
            {
                skip_indexes.merged_indices.emplace_back();
                skip_indexes.merged_indices.back().condition = index_helper->createIndexMergedCondition(query_info, metadata_snapshot);
            }

            skip_indexes.merged_indices[it->second].addIndex(index_helper);
            continue;
        }

        MergeTreeIndexConditionPtr condition;
        if (index_helper->isVectorSimilarityIndex())
        {
#if USE_USEARCH
            if (const auto * vector_similarity_index = typeid_cast<const MergeTreeIndexVectorSimilarity *>(index_helper.get()))
                condition = vector_similarity_index->createIndexCondition(filter_dag.predicate, context, vector_search_parameters);
#endif
            if (!condition)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown vector search index {}", index_helper->index.name);
        }
        else
        {
            if (!filter_dag.predicate)
                continue;

            condition = index_helper->createIndexCondition(filter_dag.predicate, context);
        }

        if (!condition->alwaysUnknownOrTrue())
            skip_indexes.useful_indices.emplace_back(index_helper, condition);
    }

    // Move minmax indices to first positions, so they will be applied first as cheapest ones
    std::stable_sort(skip_indexes.useful_indices.begin(), skip_indexes.useful_indices.end(), [](const auto & l, const auto & r)
    {
        bool l_is_minmax = typeid_cast<const MergeTreeIndexMinMax *>(l.index.get());
        bool r_is_minmax = typeid_cast<const MergeTreeIndexMinMax *>(r.index.get());
        if (l_is_minmax == r_is_minmax)
            return false;

#if USE_USEARCH
        // A vector similarity index (if present) is the most selective, hence move it to front
        bool l_is_vectorsimilarity = typeid_cast<const MergeTreeIndexVectorSimilarity *>(l.index.get());
        bool r_is_vectorsimilarity = typeid_cast<const MergeTreeIndexVectorSimilarity *>(r.index.get());
        if (l_is_vectorsimilarity)
            return true;
        if (r_is_vectorsimilarity)
            return false;
#endif
        if (l_is_minmax)
            return true; // left is min max but right is not

        return false; // right is min max but left is not
    });

    indexes->skip_indexes = std::move(skip_indexes);
}

void ReadFromMergeTree::applyFilters(ActionDAGNodes added_filter_nodes)
{
    if (!indexes)
    {
        auto dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes, query_info.buildNodeNameToInputNodeColumn());
        filter_actions_dag = dag ? std::make_shared<const ActionsDAG>(std::move(*dag)) : nullptr;

        /// NOTE: Currently we store two DAGs for analysis:
        /// (1) SourceStepWithFilter::filter_nodes, (2) query_info.filter_actions_dag. Make sure they are consistent.
        /// TODO: Get rid of filter_actions_dag in query_info after we move analysis of
        /// parallel replicas and unused shards into optimization, similar to projection analysis.
        if (filter_actions_dag)
            query_info.filter_actions_dag = filter_actions_dag;

        buildIndexes(
            indexes,
            query_info.filter_actions_dag.get(),
            data,
            getParts(),
            mutations_snapshot,
            vector_search_parameters,
            context,
            query_info,
            storage_snapshot->metadata,
            log);
    }
}

ReadFromMergeTree::AnalysisResultPtr ReadFromMergeTree::selectRangesToRead(
    RangesInDataParts parts,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
    const std::optional<VectorSearchParameters> & vector_search_parameters,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info_,
    ContextPtr context_,
    size_t num_streams,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read,
    const MergeTreeData & data,
    const Names & all_column_names,
    LoggerPtr log,
    std::optional<Indexes> & indexes,
    bool find_exact_ranges)
{
    AnalysisResult result;
    const auto & settings = context_->getSettingsRef();

    size_t total_parts = parts.size();

    result.column_names_to_read = all_column_names;

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (result.column_names_to_read.empty())
    {
        NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();
        result.column_names_to_read.push_back(ExpressionActions::getSmallestColumn(available_real_columns).name);
    }

    // Build and check if primary key is used when necessary
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    const Names & primary_key_column_names = primary_key.column_names;

    if (!indexes)
        buildIndexes(
            indexes,
            query_info_.filter_actions_dag.get(),
            data,
            parts,
            mutations_snapshot,
            vector_search_parameters,
            context_,
            query_info_,
            metadata_snapshot,
            log);

    if (indexes->part_values && indexes->part_values->empty())
        return std::make_shared<AnalysisResult>(std::move(result));

    if (indexes->key_condition.alwaysUnknownOrTrue())
    {
        if (settings[Setting::force_primary_key])
        {
            throw Exception(ErrorCodes::INDEX_NOT_USED,
                "Primary key ({}) is not used and setting 'force_primary_key' is set",
                fmt::join(primary_key_column_names, ", "));
        }
    } else
    {
        ProfileEvents::increment(ProfileEvents::SelectQueriesWithPrimaryKeyUsage);
    }

    LOG_DEBUG(log, "Key condition: {}", indexes->key_condition.toString());

    if (indexes->part_offset_condition)
        LOG_DEBUG(log, "Part offset condition: {}", indexes->part_offset_condition->toString());

    if (indexes->total_offset_condition)
        LOG_DEBUG(log, "Total offset condition: {}", indexes->total_offset_condition->toString());

    if (indexes->key_condition.alwaysFalse())
        return std::make_shared<AnalysisResult>(std::move(result));

    size_t total_marks_pk = 0;
    size_t parts_before_pk = 0;
    bool add_index_stat_row_for_pk_expand = false;

    {
        MergeTreeDataSelectExecutor::filterPartsByPartition(
            parts,
            indexes->partition_pruner,
            indexes->minmax_idx_condition,
            indexes->part_values,
            metadata_snapshot,
            data,
            context_,
            max_block_numbers_to_read.get(),
            log,
            result.index_stats);

        result.sampling = MergeTreeDataSelectExecutor::getSampling(
            query_info_,
            metadata_snapshot->getColumns().getAllPhysical(),
            parts,
            indexes->key_condition,
            data,
            metadata_snapshot,
            context_,
            log);

        if (result.sampling.read_nothing)
            return std::make_shared<AnalysisResult>(std::move(result));

        for (const auto & part : parts)
            total_marks_pk += part.data_part->index_granularity->getMarksCountWithoutFinal();
        parts_before_pk = parts.size();

        auto reader_settings = getMergeTreeReaderSettings(context_, query_info_);
        result.parts_with_ranges = MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(
            std::move(parts),
            metadata_snapshot,
            context_,
            indexes->key_condition,
            indexes->part_offset_condition,
            indexes->total_offset_condition,
            indexes->skip_indexes,
            reader_settings,
            log,
            num_streams,
            result.index_stats,
            indexes->use_skip_indexes,
            find_exact_ranges,
            query_info_.isFinal());

        MergeTreeDataSelectExecutor::filterPartsByQueryConditionCache(result.parts_with_ranges, query_info_, context_, log);

        if (indexes->use_skip_indexes && !indexes->skip_indexes.useful_indices.empty() && query_info_.isFinal()
            && settings[Setting::use_skip_indexes_if_final_exact_mode])
        {
            result.parts_with_ranges
                = findPKRangesForFinalAfterSkipIndex(primary_key, metadata_snapshot->getSortingKey(), result.parts_with_ranges, log);
            add_index_stat_row_for_pk_expand = true;
        }
    }

    size_t sum_marks_pk = total_marks_pk;
    for (const auto & stat : result.index_stats)
        if (stat.type == IndexType::PrimaryKey)
            sum_marks_pk = stat.num_granules_after;

    size_t sum_marks = 0;
    size_t sum_ranges = 0;
    size_t sum_rows = 0;

    for (const auto & part : result.parts_with_ranges)
    {
        sum_ranges += part.ranges.size();
        sum_marks += part.getMarksCount();
        sum_rows += part.getRowsCount();
    }

    if (add_index_stat_row_for_pk_expand)
    {
        result.index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::PrimaryKeyExpand,
            .description = "Selects all granules that intersect by PK values with the previous skip indexes selection",
            .num_parts_after = result.parts_with_ranges.size(),
            .num_granules_after = sum_marks});
    }

    result.total_parts = total_parts;
    result.parts_before_pk = parts_before_pk;
    result.selected_parts = result.parts_with_ranges.size();
    result.selected_ranges = sum_ranges;
    result.selected_marks = sum_marks;
    result.selected_marks_pk = sum_marks_pk;
    result.total_marks_pk = total_marks_pk;
    result.selected_rows = sum_rows;
    result.has_exact_ranges = result.selected_parts == 0 || find_exact_ranges;

    if (query_info_.input_order_info)
        result.read_type = (query_info_.input_order_info->direction > 0)
            ? ReadType::InOrder
            : ReadType::InReverseOrder;

    return std::make_shared<AnalysisResult>(std::move(result));
}

int ReadFromMergeTree::getSortDirection() const
{
    if (query_info.input_order_info)
        return query_info.input_order_info->direction;

    return 1;
}

void ReadFromMergeTree::updateSortDescription()
{
    result_sort_description = getSortDescriptionForOutputHeader(
        *output_header,
        storage_snapshot->metadata->getSortingKeyColumns(),
        storage_snapshot->metadata->getSortingKeyReverseFlags(),
        getSortDirection(),
        query_info.input_order_info,
        prewhere_info,
        enable_vertical_final);
}

bool ReadFromMergeTree::requestReadingInOrder(size_t prefix_size, int direction, size_t read_limit, std::optional<ActionsDAG> virtual_row_conversion_)
{
    /// if dirction is not set, use current one
    if (!direction)
        direction = getSortDirection();

    /// Disable read-in-order optimization for reverse order with final.
    /// Otherwise, it can lead to incorrect final behavior because the implementation may rely on the reading in direct order).
    if (direction != 1 && query_info.isFinal())
        return false;

    query_info.input_order_info = std::make_shared<InputOrderInfo>(SortDescription{}, prefix_size, direction, read_limit);
    reader_settings.read_in_order = true;

    /// In case or read-in-order, don't create too many reading streams.
    /// Almost always we are reading from a single stream at a time because of merge sort.
    if (output_streams_limit)
        requested_num_streams = output_streams_limit;

    /// All *InOrder optimization rely on an assumption that output stream is sorted, but vertical FINAL breaks this rule
    /// Let prefer in-order optimization over vertical FINAL for now
    enable_vertical_final = false;

    /// Disable virtual row for FINAL.
    if (virtual_row_conversion_ && !isQueryWithFinal() && context->getSettingsRef()[Setting::read_in_order_use_virtual_row])
        virtual_row_conversion = std::make_shared<ExpressionActions>(std::move(*virtual_row_conversion_));

    updateSortDescription();

    return true;
}

bool ReadFromMergeTree::readsInOrder() const
{
    return reader_settings.read_in_order;
}

void ReadFromMergeTree::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    query_info.prewhere_info = prewhere_info_value;
    prewhere_info = prewhere_info_value;

    output_header = MergeTreeSelectProcessor::transformHeader(
        storage_snapshot->getSampleBlockForColumns(all_column_names),
        lazily_read_info,
        prewhere_info_value);

    updateSortDescription();
}

void ReadFromMergeTree::updateLazilyReadInfo(const LazilyReadInfoPtr & lazily_read_info_value)
{
    lazily_read_info = lazily_read_info_value;

    NameSet names_set;

    for (const auto & column : lazily_read_info->lazily_read_columns)
    {
        names_set.insert(column.name);
    }
    std::erase_if(all_column_names, [&names_set] (const String & column_name)
    {
        return names_set.contains(column_name);
    });

    if (std::find_if(all_column_names.begin(), all_column_names.end(), [] (const String & column_name)
        { return column_name == "_part_offset"; }) == all_column_names.end())
    {
        lazily_read_info->remove_part_offset_column = true;
        all_column_names.emplace_back("_part_offset");
    }

    output_header = MergeTreeSelectProcessor::transformHeader(
        storage_snapshot->getSampleBlockForColumns(all_column_names),
        lazily_read_info,
        prewhere_info);

    /// if analysis has already been done (like in optimization for projections),
    /// then update columns to read in analysis result
    if (analyzed_result_ptr)
        analyzed_result_ptr->column_names_to_read = all_column_names;
}

bool ReadFromMergeTree::requestOutputEachPartitionThroughSeparatePort()
{
    if (isQueryWithFinal())
        return false;

    /// With parallel replicas we have to have only a single instance of `MergeTreeReadPoolParallelReplicas` per replica.
    /// With aggregation-by-partitions optimisation we might create a separate pool for each partition.
    if (is_parallel_reading_from_replicas)
        return false;

    const auto & settings = context->getSettingsRef();

    const auto partitions_cnt = countPartitions(getParts());
    if (!settings[Setting::force_aggregate_partitions_independently]
        && (partitions_cnt == 1 || partitions_cnt < settings[Setting::max_threads] / 2))
    {
        LOG_TRACE(
            log,
            "Independent aggregation by partitions won't be used because there are too few of them: {}. You can set "
            "force_aggregate_partitions_independently to suppress this check",
            partitions_cnt);
        return false;
    }

    if (!settings[Setting::force_aggregate_partitions_independently]
        && (partitions_cnt > settings[Setting::max_number_of_partitions_for_independent_aggregation]))
    {
        LOG_TRACE(
            log,
            "Independent aggregation by partitions won't be used because there are too many of them: {}. You can increase "
            "max_number_of_partitions_for_independent_aggregation (current value is {}) or set "
            "force_aggregate_partitions_independently to suppress this check",
            partitions_cnt,
            settings[Setting::max_number_of_partitions_for_independent_aggregation].value);
        return false;
    }

    if (!settings[Setting::force_aggregate_partitions_independently])
    {
        std::unordered_map<String, size_t> partition_rows;
        for (const auto & part : getParts())
            partition_rows[part.data_part->info.getPartitionId()] += part.data_part->rows_count;
        size_t sum_rows = 0;
        size_t max_rows = 0;
        for (const auto & [_, rows] : partition_rows)
        {
            sum_rows += rows;
            max_rows = std::max(max_rows, rows);
        }

        /// Merging shouldn't take more time than preaggregation in normal cases. And exec time is proportional to the amount of data.
        /// We assume that exec time of independent aggr is proportional to the maximum of sizes and
        /// exec time of ordinary aggr is proportional to sum of sizes divided by number of threads and multiplied by two (preaggregation + merging).
        const size_t avg_rows_in_partition = sum_rows / settings[Setting::max_threads];
        if (max_rows > avg_rows_in_partition * 2)
        {
            LOG_TRACE(
                log,
                "Independent aggregation by partitions won't be used because there are too big skew in the number of rows between "
                "partitions. You can set force_aggregate_partitions_independently to suppress this check");
            return false;
        }
    }

    return output_each_partition_through_separate_port = true;
}

ReadFromMergeTree::AnalysisResult & ReadFromMergeTree::getAnalysisResultImpl() const
{
    if (!analyzed_result_ptr)
        analyzed_result_ptr = selectRangesToRead();

    return *analyzed_result_ptr;
}

bool ReadFromMergeTree::isQueryWithSampling() const
{
    if (context->getSettingsRef()[Setting::parallel_replicas_count] > 1 && data.supportsSampling())
        return true;

    if (query_info.table_expression_modifiers)
        return query_info.table_expression_modifiers->getSampleSizeRatio() != std::nullopt;

    const auto & select = query_info.query->as<ASTSelectQuery &>();
    return select.sampleSize() != nullptr;
}

Pipe ReadFromMergeTree::spreadMarkRanges(
    RangesInDataParts && parts_with_ranges, size_t num_streams, AnalysisResult & result, std::optional<ActionsDAG> & result_projection)
{
    const bool final = isQueryWithFinal();
    Names column_names_to_read = result.column_names_to_read;
    NameSet names(column_names_to_read.begin(), column_names_to_read.end());

    if (result.sampling.use_sampling)
    {
        NameSet sampling_columns;

        /// Add columns needed for `sample_by_ast` to `column_names_to_read`.
        for (const auto & column : result.sampling.filter_expression->getRequiredColumns().getNames())
        {
            if (names.emplace(column).second)
                column_names_to_read.push_back(column);

            sampling_columns.insert(column);
        }

        if (prewhere_info)
            restorePrewhereInputs(*prewhere_info, sampling_columns);
    }

    if (final)
    {
        chassert(!is_parallel_reading_from_replicas);

        if (output_each_partition_through_separate_port)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimization isn't supposed to be used for queries with final");

        auto original_column_names = column_names_to_read;

        /// Add columns needed to calculate the sorting expression and the sign.
        for (const auto & column : storage_snapshot->metadata->getColumnsRequiredForSortingKey())
        {
            if (names.emplace(column).second)
                column_names_to_read.push_back(column);
        }

        if (!data.merging_params.is_deleted_column.empty() && names.emplace(data.merging_params.is_deleted_column).second)
            column_names_to_read.push_back(data.merging_params.is_deleted_column);
        if (!data.merging_params.sign_column.empty() && names.emplace(data.merging_params.sign_column).second)
            column_names_to_read.push_back(data.merging_params.sign_column);
        if (!data.merging_params.version_column.empty() && names.emplace(data.merging_params.version_column).second)
            column_names_to_read.push_back(data.merging_params.version_column);

        return spreadMarkRangesAmongStreamsFinal(std::move(parts_with_ranges), num_streams, original_column_names, column_names_to_read, result_projection);
    }

    if (!result.split_parts.layers.empty())
        return readByLayers(result.parts_with_ranges, std::move(result.split_parts), column_names_to_read, query_info.input_order_info);

    if (query_info.input_order_info)
    {
        return spreadMarkRangesAmongStreamsWithOrder(
            std::move(parts_with_ranges), num_streams, column_names_to_read, result_projection, query_info.input_order_info);
    }

    return spreadMarkRangesAmongStreams(std::move(parts_with_ranges), num_streams, column_names_to_read);
}

Pipe ReadFromMergeTree::groupStreamsByPartition(AnalysisResult & result, std::optional<ActionsDAG> & result_projection)
{
    auto && parts_with_ranges = std::move(result.parts_with_ranges);

    if (parts_with_ranges.empty())
        return {};

    const size_t partitions_cnt = std::max<size_t>(countPartitions(parts_with_ranges), 1);
    const size_t partitions_per_stream = std::max<size_t>(1, partitions_cnt / requested_num_streams);
    const size_t num_streams = std::max<size_t>(1, requested_num_streams / partitions_cnt);

    Pipes pipes;
    for (auto begin = parts_with_ranges.begin(), end = begin; end != parts_with_ranges.end(); begin = end)
    {
        for (size_t i = 0; i < partitions_per_stream; ++i)
            end = std::find_if(
                end,
                parts_with_ranges.end(),
                [&end](const auto & part) { return end->data_part->info.getPartitionId() != part.data_part->info.getPartitionId(); });

        RangesInDataParts partition_parts{std::make_move_iterator(begin), std::make_move_iterator(end)};

        pipes.emplace_back(spreadMarkRanges(std::move(partition_parts), num_streams, result, result_projection));
        if (!pipes.back().empty())
            pipes.back().resize(1);
    }

    return Pipe::unitePipes(std::move(pipes));
}

QueryPlanStepPtr ReadFromMergeTree::clone() const
{
    return std::make_unique<ReadFromMergeTree>(*this);
}

void ReadFromMergeTree::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto & result = getAnalysisResult();

    if (enable_remove_parts_from_snapshot_optimization)
    {
        /// Do not keep data parts in snapshot.
        /// They are stored separately, and some could be released after PK analysis.
        storage_snapshot->data = std::make_unique<MergeTreeData::SnapshotData>();
    }

    result.checkLimits(context->getSettingsRef(), query_info);
    shared_virtual_fields.emplace("_sample_factor", result.sampling.used_sample_factor);

    LOG_DEBUG(
        log,
        "Selected {}/{} parts by partition key, {} parts by primary key, {}/{} marks by primary key, {} marks to read from {} ranges",
        result.parts_before_pk,
        result.total_parts,
        result.selected_parts,
        result.selected_marks_pk,
        result.total_marks_pk,
        result.selected_marks,
        result.selected_ranges);

    // Adding partition info to QueryAccessInfo.
    if (context->hasQueryContext() && !query_info.is_internal)
    {
        Names partition_names;
        for (const auto & part : result.parts_with_ranges)
        {
            partition_names.emplace_back(
                fmt::format("{}.{}", data.getStorageID().getFullNameNotQuoted(), part.data_part->info.getPartitionId()));
        }
        context->getQueryContext()->addQueryAccessInfo(partition_names);
    }

    ProfileEvents::increment(ProfileEvents::SelectedParts, result.selected_parts);
    ProfileEvents::increment(ProfileEvents::SelectedPartsTotal, result.total_parts);
    ProfileEvents::increment(ProfileEvents::SelectedRanges, result.selected_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, result.selected_marks);
    ProfileEvents::increment(ProfileEvents::SelectedMarksTotal, result.total_marks_pk);

    auto query_id_holder = MergeTreeDataSelectExecutor::checkLimits(data, result, context);

    /// If we have neither a WHERE nor a PREWHERE condition, the query condition cache doesn't save anything --> disable it.
    if (reader_settings.use_query_condition_cache && !query_info.prewhere_info && !query_info.filter_actions_dag)
        reader_settings.use_query_condition_cache = false;

    if (result.parts_with_ranges.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputHeader())));
        return;
    }

    selected_marks = result.selected_marks;
    selected_rows = result.selected_rows;
    selected_parts = result.selected_parts;
    /// Projection, that needed to drop columns, which have appeared by execution
    /// of some extra expressions, and to allow execute the same expressions later.
    /// NOTE: It may lead to double computation of expressions.
    std::optional<ActionsDAG> result_projection;

    if (lazily_read_info)
    {
        for (const auto & ranges_in_data_part : result.parts_with_ranges)
        {
            auto alter_conversions = MergeTreeData::getAlterConversionsForPart(ranges_in_data_part.data_part, mutations_snapshot, getContext());
            auto part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(ranges_in_data_part.data_part, std::move(alter_conversions));
            lazily_read_info->data_part_infos->emplace(ranges_in_data_part.part_index_in_query, std::move(part_info));
        }
    }

    Pipe pipe = output_each_partition_through_separate_port
        ? groupStreamsByPartition(result, result_projection)
        : spreadMarkRanges(std::move(result.parts_with_ranges), requested_num_streams, result, result_projection);

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(query_info.storage_limits);

    if (pipe.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputHeader())));
        return;
    }

    if (result.sampling.use_sampling)
    {
        auto sampling_actions = std::make_shared<ExpressionActions>(result.sampling.filter_expression->clone());
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(
                header,
                sampling_actions,
                result.sampling.filter_function->getColumnName(),
                false);
        });
    }

    Block cur_header = pipe.getHeader();

    auto append_actions = [&result_projection](ActionsDAG actions)
    {
        if (!result_projection)
            result_projection = std::move(actions);
        else
            result_projection = ActionsDAG::merge(std::move(*result_projection), std::move(actions));
    };

    if (result_projection)
        cur_header = result_projection->updateHeader(cur_header);

    /// Extra columns may be returned (for example, if sampling is used).
    /// Convert pipe to step header structure.
    if (!isCompatibleHeader(cur_header, getOutputHeader()))
    {
        auto converting = ActionsDAG::makeConvertingActions(
            cur_header.getColumnsWithTypeAndName(),
            getOutputHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        append_actions(std::move(converting));
    }

    if (result_projection)
    {
        auto projection_actions = std::make_shared<ExpressionActions>(std::move(*result_projection));
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, projection_actions);
        });
    }

    /// Some extra columns could be added by sample/final/in-order/etc
    /// Remove them from header if not needed.
    if (!blocksHaveEqualStructure(pipe.getHeader(), getOutputHeader()))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            getOutputHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            true);

        auto converting_dag_expr = std::make_shared<ExpressionActions>(std::move(convert_actions_dag));

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, converting_dag_expr);
        });
    }

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
    pipeline.addContext(context);
    // Attach QueryIdHolder if needed
    if (query_id_holder)
        pipeline.setQueryIdHolder(std::move(query_id_holder));
}

static const char * indexTypeToString(ReadFromMergeTree::IndexType type)
{
    switch (type)
    {
        case ReadFromMergeTree::IndexType::None:
            return "None";
        case ReadFromMergeTree::IndexType::MinMax:
            return "MinMax";
        case ReadFromMergeTree::IndexType::Partition:
            return "Partition";
        case ReadFromMergeTree::IndexType::PrimaryKey:
            return "PrimaryKey";
        case ReadFromMergeTree::IndexType::Skip:
            return "Skip";
        case ReadFromMergeTree::IndexType::PrimaryKeyExpand:
            return "PrimaryKeyExpand";
    }
}

static const char * readTypeToString(ReadFromMergeTree::ReadType type)
{
    switch (type)
    {
        case ReadFromMergeTree::ReadType::Default:
            return "Default";
        case ReadFromMergeTree::ReadType::InOrder:
            return "InOrder";
        case ReadFromMergeTree::ReadType::InReverseOrder:
            return "InReverseOrder";
        case ReadFromMergeTree::ReadType::ParallelReplicas:
            return "Parallel";
    }
}

void ReadFromMergeTree::describeActions(FormatSettings & format_settings) const
{
    const auto & result = getAnalysisResult();
    std::string prefix(format_settings.offset, format_settings.indent_char);
    format_settings.out << prefix << "ReadType: " << readTypeToString(result.read_type) << '\n';

    if (!result.index_stats.empty())
    {
        format_settings.out << prefix << "Parts: " << result.index_stats.back().num_parts_after << '\n';
        format_settings.out << prefix << "Granules: " << result.index_stats.back().num_granules_after << '\n';
    }

    if (prewhere_info)
    {
        format_settings.out << prefix << "Prewhere info" << '\n';
        format_settings.out << prefix << "Need filter: " << prewhere_info->need_filter << '\n';

        prefix.push_back(format_settings.indent_char);
        prefix.push_back(format_settings.indent_char);

        {
            format_settings.out << prefix << "Prewhere filter" << '\n';
            format_settings.out << prefix << "Prewhere filter column: " << prewhere_info->prewhere_column_name;
            if (prewhere_info->remove_prewhere_column)
               format_settings.out << " (removed)";
            format_settings.out << '\n';

            auto expression = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions.clone());
            expression->describeActions(format_settings.out, prefix);
        }

        if (prewhere_info->row_level_filter)
        {
            format_settings.out << prefix << "Row level filter" << '\n';
            format_settings.out << prefix << "Row level filter column: " << prewhere_info->row_level_column_name << '\n';

            auto expression = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter->clone());
            expression->describeActions(format_settings.out, prefix);
        }
    }

    if (virtual_row_conversion)
    {
        format_settings.out << prefix << "Virtual row conversions" << '\n';
        virtual_row_conversion->describeActions(format_settings.out, prefix);
    }
}

void ReadFromMergeTree::describeActions(JSONBuilder::JSONMap & map) const
{
    const auto & result = getAnalysisResult();
    map.add("Read Type", readTypeToString(result.read_type));
    if (!result.index_stats.empty())
    {
        map.add("Parts", result.index_stats.back().num_parts_after);
        map.add("Granules", result.index_stats.back().num_granules_after);
    }

    if (prewhere_info)
    {
        std::unique_ptr<JSONBuilder::JSONMap> prewhere_info_map = std::make_unique<JSONBuilder::JSONMap>();
        prewhere_info_map->add("Need filter", prewhere_info->need_filter);

        {
            std::unique_ptr<JSONBuilder::JSONMap> prewhere_filter_map = std::make_unique<JSONBuilder::JSONMap>();
            prewhere_filter_map->add("Prewhere filter column", prewhere_info->prewhere_column_name);
            prewhere_filter_map->add("Prewhere filter remove filter column", prewhere_info->remove_prewhere_column);
            auto expression = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions.clone());
            prewhere_filter_map->add("Prewhere filter expression", expression->toTree());

            prewhere_info_map->add("Prewhere filter", std::move(prewhere_filter_map));
        }

        if (prewhere_info->row_level_filter)
        {
            std::unique_ptr<JSONBuilder::JSONMap> row_level_filter_map = std::make_unique<JSONBuilder::JSONMap>();
            row_level_filter_map->add("Row level filter column", prewhere_info->row_level_column_name);
            auto expression = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter->clone());
            row_level_filter_map->add("Row level filter expression", expression->toTree());

            prewhere_info_map->add("Row level filter", std::move(row_level_filter_map));
        }

        map.add("Prewhere info", std::move(prewhere_info_map));
    }

    if (virtual_row_conversion)
        map.add("Virtual row conversions", virtual_row_conversion->toTree());
}

namespace
{
    std::string_view searchAlgorithmToString(const MarkRanges::SearchAlgorithm search_algorithm)
    {
        switch (search_algorithm)
        {
        case MarkRanges::SearchAlgorithm::BinarySearch:
            return "binary search";
        case MarkRanges::SearchAlgorithm::GenericExclusionSearch:
            return "generic exclusion search";
        default:
            return "";
        }
    };
}

void ReadFromMergeTree::describeIndexes(FormatSettings & format_settings) const
{
    const auto & result = getAnalysisResult();
    const auto & index_stats = result.index_stats;

    std::string prefix(format_settings.offset, format_settings.indent_char);
    if (!index_stats.empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
            return;

        std::string indent(format_settings.indent, format_settings.indent_char);
        format_settings.out << prefix << "Indexes:\n";

        for (size_t i = 0; i < index_stats.size(); ++i)
        {
            const auto & stat = index_stats[i];
            if (stat.type == IndexType::None)
                continue;

            format_settings.out << prefix << indent << indexTypeToString(stat.type) << '\n';

            if (!stat.name.empty())
                format_settings.out << prefix << indent << indent << "Name: " << stat.name << '\n';

            if (!stat.description.empty())
                format_settings.out << prefix << indent << indent << "Description: " << stat.description << '\n';

            if (!stat.used_keys.empty())
            {
                format_settings.out << prefix << indent << indent << "Keys:" << '\n';
                for (const auto & used_key : stat.used_keys)
                    format_settings.out << prefix << indent << indent << indent << used_key << '\n';
            }

            if (!stat.condition.empty())
                format_settings.out << prefix << indent << indent << "Condition: " << stat.condition << '\n';

            format_settings.out << prefix << indent << indent << "Parts: " << stat.num_parts_after;
            if (i)
                format_settings.out << '/' << index_stats[i - 1].num_parts_after;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Granules: " << stat.num_granules_after;
            if (i)
                format_settings.out << '/' << index_stats[i - 1].num_granules_after;
            format_settings.out << '\n';

            auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
            if (!search_algorithm.empty())
                format_settings.out << prefix << indent << indent << "Search Algorithm: " << search_algorithm << "\n";
        }
    }
}

void ReadFromMergeTree::describeIndexes(JSONBuilder::JSONMap & map) const
{
    const auto & result = getAnalysisResult();
    const auto & index_stats = result.index_stats;

    if (!index_stats.empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
            return;

        auto indexes_array = std::make_unique<JSONBuilder::JSONArray>();

        for (size_t i = 0; i < index_stats.size(); ++i)
        {
            const auto & stat = index_stats[i];
            if (stat.type == IndexType::None)
                continue;

            auto index_map = std::make_unique<JSONBuilder::JSONMap>();

            index_map->add("Type", indexTypeToString(stat.type));

            if (!stat.name.empty())
                index_map->add("Name", stat.name);

            if (!stat.description.empty())
                index_map->add("Description", stat.description);

            if (!stat.used_keys.empty())
            {
                auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

                for (const auto & used_key : stat.used_keys)
                    keys_array->add(used_key);

                index_map->add("Keys", std::move(keys_array));
            }

            if (!stat.condition.empty())
                index_map->add("Condition", stat.condition);

            auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
            if (!search_algorithm.empty())
                index_map->add("Search Algorithm", search_algorithm);

            if (i)
                index_map->add("Initial Parts", index_stats[i - 1].num_parts_after);
            index_map->add("Selected Parts", stat.num_parts_after);

            if (i)
                index_map->add("Initial Granules", index_stats[i - 1].num_granules_after);
            index_map->add("Selected Granules", stat.num_granules_after);

            indexes_array->add(std::move(index_map));
        }

        map.add("Indexes", std::move(indexes_array));
    }
}

void ReadFromMergeTree::describeProjections(FormatSettings & format_settings) const
{
    const auto & result = getAnalysisResult();
    const auto & projection_stats = result.projection_stats;

    std::string prefix(format_settings.offset, format_settings.indent_char);
    if (!projection_stats.empty())
    {
        std::string indent(format_settings.indent, format_settings.indent_char);
        format_settings.out << prefix << "Projections:\n";

        for (const auto & stat : projection_stats)
        {
            format_settings.out << prefix << indent << "Name: " << stat.name << '\n';

            if (!stat.description.empty())
                format_settings.out << prefix << indent << indent << "Description: " << stat.description << '\n';

            if (!stat.condition.empty())
                format_settings.out << prefix << indent << indent << "Condition: " << stat.condition << '\n';

            auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
            if (!search_algorithm.empty())
                format_settings.out << prefix << indent << indent << "Search Algorithm: " << search_algorithm << "\n";

            format_settings.out << prefix << indent << indent << "Parts: " << stat.selected_parts;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Marks: " << stat.selected_marks;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Ranges: " << stat.selected_ranges;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Rows: " << stat.selected_rows;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Filtered Parts: " << stat.filtered_parts;
            format_settings.out << '\n';
        }
    }
}

void ReadFromMergeTree::describeProjections(JSONBuilder::JSONMap & map) const
{
    const auto & result = getAnalysisResult();
    const auto & projection_stats = result.projection_stats;

    if (!projection_stats.empty())
    {
        auto projections_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & stat : projection_stats)
        {
             auto projection_map = std::make_unique<JSONBuilder::JSONMap>();
            projection_map->add("Name", stat.name);

            if (!stat.description.empty())
                projection_map->add("Description", stat.description);

            if (!stat.condition.empty())
                projection_map->add("Condition", stat.condition);

            auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
            if (!search_algorithm.empty())
                projection_map->add("Search Algorithm", search_algorithm);

            projection_map->add("Selected Parts", stat.selected_parts);
            projection_map->add("Selected Marks", stat.selected_marks);
            projection_map->add("Selected Ranges", stat.selected_ranges);
            projection_map->add("Selected Rows", stat.selected_rows);
            projection_map->add("Filtered Parts", stat.filtered_parts);

            projections_array->add(std::move(projection_map));
        }

        map.add("Projections", std::move(projections_array));
    }
}

}
