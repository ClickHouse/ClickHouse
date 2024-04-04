#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeInOrderSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeReverseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreePrefetchedReadPool.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeThreadSelectProcessor.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/VirtualColumnUtils.h>
#include <base/sort.h>
#include <Poco/Logger.h>
#include <Common/JSONBuilder.h>
#include <Common/isLocalAddress.h>
#include <Common/logger_useful.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ExpressionListParsers.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <queue>
#include <stdexcept>
#include <unordered_map>

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
    auto get_partition_id = [](const RangesInDataPart & rng) { return rng.data_part->info.partition_id; };
    return countPartitions(parts_with_ranges, get_partition_id);
}

size_t countPartitions(const MergeTreeData::DataPartsVector & prepared_parts)
{
    auto get_partition_id = [](const MergeTreeData::DataPartPtr data_part) { return data_part->info.partition_id; };
    return countPartitions(prepared_parts, get_partition_id);
}

}

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
}

namespace DB
{

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
    return
    {
        .read_settings = context->getReadSettings(),
        .save_marks_in_cache = true,
        .checksum_on_read = settings.checksum_on_read,
        .read_in_order = query_info.input_order_info != nullptr,
        .use_asynchronous_read_from_pool = settings.allow_asynchronous_read_from_io_pool_for_merge_tree
            && (settings.max_streams_to_max_threads_ratio > 1 || settings.max_streams_for_merge_tree_reading > 1),
        .enable_multiple_prewhere_read_steps = settings.enable_multiple_prewhere_read_steps,
    };
}

static const PrewhereInfoPtr & getPrewhereInfoFromQueryInfo(const SelectQueryInfo & query_info)
{
    return query_info.projection ? query_info.projection->prewhere_info
                                 : query_info.prewhere_info;
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
static void updateSortDescriptionForOutputStream(
    DataStream & output_stream, const Names & sorting_key_columns, const int sort_direction, InputOrderInfoPtr input_order_info, PrewhereInfoPtr prewhere_info)
{
    /// Updating sort description can be done after PREWHERE actions are applied to the header.
    /// Aftert PREWHERE actions are applied, column names in header can differ from storage column names due to aliases
    /// To mitigate it, we're trying to build original header and use it to deduce sorting description
    /// TODO: this approach is fragile, it'd be more robust to update sorting description for the whole plan during plan optimization
    Block original_header = output_stream.header.cloneEmpty();
    if (prewhere_info)
    {
        if (prewhere_info->prewhere_actions)
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
            FindOriginalNodeForOutputName original_column_finder(prewhere_info->row_level_filter);
            for (auto & column : original_header)
            {
                const auto * original_node = original_column_finder.find(column.name);
                if (original_node)
                    column.name = original_node->result_name;
            }
        }
    }

    SortDescription sort_description;
    const Block & header = output_stream.header;
    for (const auto & sorting_key : sorting_key_columns)
    {
        const auto it = std::find_if(
            original_header.begin(), original_header.end(), [&sorting_key](const auto & column) { return column.name == sorting_key; });
        if (it == original_header.end())
            break;

        const size_t column_pos = std::distance(original_header.begin(), it);
        sort_description.emplace_back((header.begin() + column_pos)->name, sort_direction);
    }

    if (!sort_description.empty())
    {
        if (input_order_info)
        {
            output_stream.sort_scope = DataStream::SortScope::Stream;
            const size_t used_prefix_of_sorting_key_size = input_order_info->used_prefix_of_sorting_key_size;
            if (sort_description.size() > used_prefix_of_sorting_key_size)
                sort_description.resize(used_prefix_of_sorting_key_size);
        }
        else
            output_stream.sort_scope = DataStream::SortScope::Chunk;
    }

    output_stream.sort_description = std::move(sort_description);
}

void ReadFromMergeTree::AnalysisResult::checkLimits(const Settings & settings, const SelectQueryInfo & query_info_) const
{

    /// Do not check number of read rows if we have reading
    /// in order of sorting key with limit.
    /// In general case, when there exists WHERE clause
    /// it's impossible to estimate number of rows precisely,
    /// because we can stop reading at any time.

    SizeLimits limits;
    if (settings.read_overflow_mode == OverflowMode::THROW
        && settings.max_rows_to_read
        && !query_info_.input_order_info)
        limits = SizeLimits(settings.max_rows_to_read, 0, settings.read_overflow_mode);

    SizeLimits leaf_limits;
    if (settings.read_overflow_mode_leaf == OverflowMode::THROW
        && settings.max_rows_to_read_leaf
        && !query_info_.input_order_info)
        leaf_limits = SizeLimits(settings.max_rows_to_read_leaf, 0, settings.read_overflow_mode_leaf);

    if (limits.max_rows || leaf_limits.max_rows)
    {
        /// Fail fast if estimated number of rows to read exceeds the limit
        size_t total_rows_estimate = selected_rows;
        if (query_info_.limit > 0 && total_rows_estimate > query_info_.limit)
        {
            total_rows_estimate = query_info_.limit;
        }
        limits.check(total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
        leaf_limits.check(
            total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
    }
}

ReadFromMergeTree::ReadFromMergeTree(
    MergeTreeData::DataPartsVector parts_,
    std::vector<AlterConversionsPtr> alter_conversions_,
    Names real_column_names_,
    Names virt_column_names_,
    const MergeTreeData & data_,
    const SelectQueryInfo & query_info_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_,
    bool sample_factor_column_queried_,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
    Poco::Logger * log_,
    MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr_,
    bool enable_parallel_reading)
    : SourceStepWithFilter(DataStream{.header = IMergeTreeSelectAlgorithm::transformHeader(
        storage_snapshot_->getSampleBlockForColumns(real_column_names_),
        getPrewhereInfoFromQueryInfo(query_info_),
        data_.getPartitionValueType(),
        virt_column_names_)})
    , reader_settings(getMergeTreeReaderSettings(context_, query_info_))
    , prepared_parts(std::move(parts_))
    , alter_conversions_for_parts(std::move(alter_conversions_))
    , real_column_names(std::move(real_column_names_))
    , virt_column_names(std::move(virt_column_names_))
    , data(data_)
    , query_info(query_info_)
    , prewhere_info(getPrewhereInfoFromQueryInfo(query_info))
    , actions_settings(ExpressionActionsSettings::fromContext(context_))
    , storage_snapshot(std::move(storage_snapshot_))
    , metadata_for_reading(storage_snapshot->getMetadataForQuery())
    , context(std::move(context_))
    , max_block_size(max_block_size_)
    , requested_num_streams(num_streams_)
    , preferred_block_size_bytes(context->getSettingsRef().preferred_block_size_bytes)
    , preferred_max_column_in_block_size_bytes(context->getSettingsRef().preferred_max_column_in_block_size_bytes)
    , sample_factor_column_queried(sample_factor_column_queried_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , log(log_)
    , analyzed_result_ptr(analyzed_result_ptr_)
    , is_parallel_reading_from_replicas(enable_parallel_reading)
{
    if (sample_factor_column_queried)
    {
        /// Only _sample_factor virtual column is added by ReadFromMergeTree
        /// Other virtual columns are added by MergeTreeBaseSelectProcessor.
        auto type = std::make_shared<DataTypeFloat64>();
        output_stream->header.insert({type->createColumn(), type, "_sample_factor"});
    }

    if (is_parallel_reading_from_replicas)
    {
        all_ranges_callback = context->getMergeTreeAllRangesCallback();
        read_task_callback = context->getMergeTreeReadTaskCallback();
    }

    const auto & settings = context->getSettingsRef();
    if (settings.max_streams_for_merge_tree_reading)
    {
        if (settings.allow_asynchronous_read_from_io_pool_for_merge_tree)
        {
            /// When async reading is enabled, allow to read using more streams.
            /// Will add resize to output_streams_limit to reduce memory usage.
            output_streams_limit = std::min<size_t>(requested_num_streams, settings.max_streams_for_merge_tree_reading);
            /// We intentionally set `max_streams` to 1 in InterpreterSelectQuery in case of small limit.
            /// Changing it here to `max_streams_for_merge_tree_reading` proven itself as a threat for performance.
            if (requested_num_streams != 1)
                requested_num_streams = std::max<size_t>(requested_num_streams, settings.max_streams_for_merge_tree_reading);
        }
        else
            /// Just limit requested_num_streams otherwise.
            requested_num_streams = std::min<size_t>(requested_num_streams, settings.max_streams_for_merge_tree_reading);
    }

    /// Add explicit description.
    setStepDescription(data.getStorageID().getFullNameNotQuoted());

    updateSortDescriptionForOutputStream(
        *output_stream,
        storage_snapshot->getMetadataForQuery()->getSortingKeyColumns(),
        getSortDirection(),
        query_info.getInputOrderInfo(),
        prewhere_info);
}


Pipe ReadFromMergeTree::readFromPoolParallelReplicas(
    RangesInDataParts parts_with_range,
    Names required_columns,
    size_t max_streams,
    size_t min_marks_for_concurrent_read,
    bool use_uncompressed_cache
)
{
    const auto & client_info = context->getClientInfo();
    auto extension = ParallelReadingExtension
    {
        .all_callback = all_ranges_callback.value(),
        .callback = read_task_callback.value(),
        .count_participating_replicas = client_info.count_participating_replicas,
        .number_of_current_replica = client_info.number_of_current_replica,
        .columns_to_read = required_columns
    };

    /// We have a special logic for local replica. It has to read less data, because in some cases it should
    /// merge states of aggregate functions or do some other important stuff other than reading from Disk.
    min_marks_for_concurrent_read = static_cast<size_t>(min_marks_for_concurrent_read * context->getSettingsRef().parallel_replicas_single_task_marks_count_multiplier);

    auto pool = std::make_shared<MergeTreeReadPoolParallelReplicas>(
        storage_snapshot,
        max_streams,
        extension,
        parts_with_range,
        prewhere_info,
        actions_settings,
        reader_settings,
        required_columns,
        virt_column_names,
        min_marks_for_concurrent_read);

    Pipes pipes;
    const auto & settings = context->getSettingsRef();
    size_t total_rows = parts_with_range.getRowsCountAllParts();

    for (size_t i = 0; i < max_streams; ++i)
    {
        auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(
            i, pool, min_marks_for_concurrent_read, max_block_size,
            settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
            data, storage_snapshot, use_uncompressed_cache,
            prewhere_info, actions_settings, reader_settings, virt_column_names);

        auto source = std::make_shared<MergeTreeSource>(std::move(algorithm));

        /// Set the approximate number of rows for the first source only
        /// In case of parallel processing on replicas do not set approximate rows at all.
        /// Because the value will be identical on every replicas and will be accounted
        /// multiple times (settings.max_parallel_replicas times more)
        if (i == 0 && !client_info.collaborate_with_initiator)
            source->addTotalRowsApprox(total_rows);

        pipes.emplace_back(std::move(source));
    }

    return Pipe::unitePipes(std::move(pipes));
}


Pipe ReadFromMergeTree::readFromPool(
    RangesInDataParts parts_with_range,
    Names required_columns,
    size_t max_streams,
    size_t min_marks_for_concurrent_read,
    bool use_uncompressed_cache)
{
    Pipes pipes;
    size_t sum_marks = parts_with_range.getMarksCountAllParts();
    size_t total_rows = parts_with_range.getRowsCountAllParts();

    if (query_info.limit > 0 && query_info.limit < total_rows)
        total_rows = query_info.limit;

    const auto & settings = context->getSettingsRef();

    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    /// If granularity is adaptive it doesn't make sense
    /// Maybe it will make sense to add settings `max_block_size_bytes`
    if (max_block_size && !data.canUseAdaptiveGranularity())
    {
        size_t fixed_index_granularity = data.getSettings()->index_granularity;
        min_marks_for_concurrent_read = (min_marks_for_concurrent_read * fixed_index_granularity + max_block_size - 1)
            / max_block_size * max_block_size / fixed_index_granularity;
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

    if ((all_parts_are_remote && settings.allow_prefetched_read_pool_for_remote_filesystem
         && MergeTreePrefetchedReadPool::checkReadMethodAllowed(reader_settings.read_settings.remote_fs_method))
        || (all_parts_are_local && settings.allow_prefetched_read_pool_for_local_filesystem
            && MergeTreePrefetchedReadPool::checkReadMethodAllowed(reader_settings.read_settings.local_fs_method)))
    {
        pool = std::make_shared<MergeTreePrefetchedReadPool>(
            max_streams,
            sum_marks,
            min_marks_for_concurrent_read,
            std::move(parts_with_range),
            storage_snapshot,
            prewhere_info,
            actions_settings,
            required_columns,
            virt_column_names,
            settings.preferred_block_size_bytes,
            reader_settings,
            context,
            use_uncompressed_cache,
            all_parts_are_remote,
            *data.getSettings());
    }
    else
    {
        pool = std::make_shared<MergeTreeReadPool>(
            max_streams,
            sum_marks,
            min_marks_for_concurrent_read,
            std::move(parts_with_range),
            storage_snapshot,
            prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            virt_column_names,
            context,
            false);
    }

    auto * logger = &Poco::Logger::get(data.getLogName() + " (SelectExecutor)");
    LOG_DEBUG(logger, "Reading approx. {} rows with {} streams", total_rows, max_streams);

    for (size_t i = 0; i < max_streams; ++i)
    {
        auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(
            i, pool, min_marks_for_concurrent_read, max_block_size,
            settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
            data, storage_snapshot, use_uncompressed_cache,
            prewhere_info, actions_settings, reader_settings, virt_column_names);

        auto source = std::make_shared<MergeTreeSource>(std::move(algorithm));

        if (i == 0)
            source->addTotalRowsApprox(total_rows);

        pipes.emplace_back(std::move(source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (output_streams_limit && output_streams_limit < pipe.numOutputPorts())
        pipe.resize(output_streams_limit);
    return pipe;
}

template<typename Algorithm>
ProcessorPtr ReadFromMergeTree::createSource(
    const RangesInDataPart & part,
    const Names & required_columns,
    bool use_uncompressed_cache,
    bool has_limit_below_one_block,
    MergeTreeInOrderReadPoolParallelReplicasPtr pool)
{
    auto total_rows = part.getRowsCount();
    if (query_info.limit > 0 && query_info.limit < total_rows)
        total_rows = query_info.limit;

    /// Actually it means that parallel reading from replicas enabled
    /// and we have to collaborate with initiator.
    /// In this case we won't set approximate rows, because it will be accounted multiple times.
    /// Also do not count amount of read rows if we read in order of sorting key,
    /// because we don't know actual amount of read rows in case when limit is set.
    bool set_rows_approx = !is_parallel_reading_from_replicas && !reader_settings.read_in_order;

    auto algorithm = std::make_unique<Algorithm>(
            data, storage_snapshot, part.data_part, part.alter_conversions, max_block_size, preferred_block_size_bytes,
            preferred_max_column_in_block_size_bytes, required_columns, part.ranges, use_uncompressed_cache, prewhere_info,
            actions_settings, reader_settings, pool, virt_column_names, part.part_index_in_query, has_limit_below_one_block);

    auto source = std::make_shared<MergeTreeSource>(std::move(algorithm));

    if (set_rows_approx)
        source->addTotalRowsApprox(total_rows);

    return source;
}

Pipe ReadFromMergeTree::readInOrder(
    RangesInDataParts parts_with_range,
    Names required_columns,
    ReadType read_type,
    bool use_uncompressed_cache,
    UInt64 limit,
    MergeTreeInOrderReadPoolParallelReplicasPtr pool)
{
    Pipes pipes;
    /// For reading in order it makes sense to read only
    /// one range per task to reduce number of read rows.
    bool has_limit_below_one_block = read_type != ReadType::Default && limit && limit < max_block_size;

    for (const auto & part : parts_with_range)
    {
        auto source = read_type == ReadType::InReverseOrder
                    ? createSource<MergeTreeReverseSelectAlgorithm>(part, required_columns, use_uncompressed_cache, has_limit_below_one_block, pool)
                    : createSource<MergeTreeInOrderSelectAlgorithm>(part, required_columns, use_uncompressed_cache, has_limit_below_one_block, pool);

        pipes.emplace_back(std::move(source));
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
    RangesInDataParts parts_with_range, Names required_columns, ReadType read_type,
    size_t max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache)
{
    if (read_type == ReadType::ParallelReplicas)
        return readFromPoolParallelReplicas(parts_with_range, required_columns, max_streams, min_marks_for_concurrent_read, use_uncompressed_cache);

    if (read_type == ReadType::Default && max_streams > 1)
        return readFromPool(parts_with_range, required_columns, max_streams, min_marks_for_concurrent_read, use_uncompressed_cache);

    auto pipe = readInOrder(parts_with_range, required_columns, read_type, use_uncompressed_cache, /*limit  */0, /*pool*/nullptr);

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
            index_granularity_bytes = data_settings.index_granularity_bytes;

        max_marks_to_use_cache = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
            settings.merge_tree_max_rows_to_use_cache,
            settings.merge_tree_max_bytes_to_use_cache,
            data_settings.index_granularity,
            index_granularity_bytes);

        auto all_parts_on_remote_disk = checkAllPartsOnRemoteFS(parts);

        size_t min_rows_for_concurrent_read;
        size_t min_bytes_for_concurrent_read;
        if (all_parts_on_remote_disk)
        {
            min_rows_for_concurrent_read = settings.merge_tree_min_rows_for_concurrent_read_for_remote_filesystem;
            min_bytes_for_concurrent_read = settings.merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem;
        }
        else
        {
            min_rows_for_concurrent_read = settings.merge_tree_min_rows_for_concurrent_read;
            min_bytes_for_concurrent_read = settings.merge_tree_min_bytes_for_concurrent_read;
        }

        min_marks_for_concurrent_read = MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
            min_rows_for_concurrent_read, min_bytes_for_concurrent_read,
            data_settings.index_granularity, index_granularity_bytes, sum_marks);

        use_uncompressed_cache = settings.use_uncompressed_cache;
        if (sum_marks > max_marks_to_use_cache)
            use_uncompressed_cache = false;
    }
};

}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreams(RangesInDataParts && parts_with_ranges, size_t num_streams, const Names & column_names)
{
    const auto & settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();

    LOG_TRACE(log, "Spreading mark ranges among streams (default reading)");

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);

    if (0 == info.sum_marks)
        return {};

    if (num_streams > 1)
    {
        /// Reduce the number of num_streams if the data is small.
        if (info.sum_marks < num_streams * info.min_marks_for_concurrent_read && parts_with_ranges.size() < num_streams)
            num_streams = std::max((info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read, parts_with_ranges.size());
    }

    auto read_type = is_parallel_reading_from_replicas ? ReadType::ParallelReplicas : ReadType::Default;

    return read(std::move(parts_with_ranges), column_names, read_type,
                num_streams, info.min_marks_for_concurrent_read, info.use_uncompressed_cache);
}

static ActionsDAGPtr createProjection(const Block & header)
{
    auto projection = std::make_shared<ActionsDAG>(header.getNamesAndTypesList());
    projection->removeUnusedActions(header.getNames());
    projection->projectInput();
    return projection;
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsWithOrder(
    RangesInDataParts && parts_with_ranges,
    size_t num_streams,
    const Names & column_names,
    ActionsDAGPtr & out_projection,
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
    if (prewhere_info && prewhere_info->prewhere_actions)
    {
        auto & outputs = prewhere_info->prewhere_actions->getOutputs();
        std::unordered_set<const ActionsDAG::Node *> outputs_set(outputs.begin(), outputs.end());
        for (const auto * input : prewhere_info->prewhere_actions->getInputs())
        {
            if (!outputs_set.contains(input))
            {
                outputs.push_back(input);
                have_input_columns_removed_after_prewhere = true;
            }
        }
    }

    /// Let's split ranges to avoid reading much data.
    auto split_ranges
        = [rows_granularity = data_settings->index_granularity, my_max_block_size = max_block_size](const auto & ranges, int direction)
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

    const size_t min_marks_per_stream = (info.sum_marks - 1) / num_streams + 1;
    bool need_preliminary_merge = (parts_with_ranges.size() > settings.read_in_order_two_level_merge_threshold);

    std::vector<RangesInDataParts> splitted_parts_and_ranges;
    splitted_parts_and_ranges.reserve(num_streams);

    const auto read_type = input_order_info->direction == 1
                       ? ReadFromMergeTree::ReadType::InOrder
                       : ReadFromMergeTree::ReadType::InReverseOrder;

    MergeTreeInOrderReadPoolParallelReplicasPtr pool;

    if (is_parallel_reading_from_replicas)
    {
        const auto & client_info = context->getClientInfo();
        auto extension = ParallelReadingExtension
        {
            .all_callback = all_ranges_callback.value(),
            .callback = read_task_callback.value(),
            .count_participating_replicas = client_info.count_participating_replicas,
            .number_of_current_replica = client_info.number_of_current_replica,
            .columns_to_read = column_names
        };

        auto min_marks_for_concurrent_read = info.min_marks_for_concurrent_read;
        min_marks_for_concurrent_read = static_cast<size_t>(min_marks_for_concurrent_read * settings.parallel_replicas_single_task_marks_count_multiplier);

        pool = std::make_shared<MergeTreeInOrderReadPoolParallelReplicas>(
            parts_with_ranges,
            extension,
            read_type == ReadFromMergeTree::ReadType::InOrder ? CoordinationMode::WithOrder : CoordinationMode::ReverseOrder,
            min_marks_for_concurrent_read);
    }


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
            new_parts.emplace_back(part.data_part, part.alter_conversions, part.part_index_in_query, std::move(ranges_to_get_from_part));
        }

        splitted_parts_and_ranges.emplace_back(std::move(new_parts));
    }

    Pipes pipes;
    for (auto & item : splitted_parts_and_ranges)
    {
        pipes.emplace_back(readInOrder(std::move(item), column_names, read_type,
                                        info.use_uncompressed_cache, input_order_info->limit, pool));
    }

    Block pipe_header;
    if (!pipes.empty())
        pipe_header = pipes.front().getHeader();

    if (need_preliminary_merge || output_each_partition_through_separate_port)
    {
        size_t prefix_size = input_order_info->used_prefix_of_sorting_key_size;
        auto order_key_prefix_ast = metadata_for_reading->getSortingKey().expression_list_ast->clone();
        order_key_prefix_ast->children.resize(prefix_size);

        auto syntax_result = TreeRewriter(context).analyze(order_key_prefix_ast, metadata_for_reading->getColumns().getAllPhysical());
        auto sorting_key_prefix_expr = ExpressionAnalyzer(order_key_prefix_ast, syntax_result, context).getActionsDAG(false);
        const auto & sorting_columns = metadata_for_reading->getSortingKey().column_names;

        SortDescription sort_description;
        sort_description.compile_sort_description = settings.compile_sort_description;
        sort_description.min_count_to_compile_sort_description = settings.min_count_to_compile_sort_description;

        for (size_t j = 0; j < prefix_size; ++j)
            sort_description.emplace_back(sorting_columns[j], input_order_info->direction);

        auto sorting_key_expr = std::make_shared<ExpressionActions>(sorting_key_prefix_expr);

        auto merge_streams = [&](Pipe & pipe)
        {
            pipe.addSimpleTransform([sorting_key_expr](const Block & header)
                                    { return std::make_shared<ExpressionTransform>(header, sorting_key_expr); });

            if (pipe.numOutputPorts() > 1)
            {
                auto transform = std::make_shared<MergingSortedTransform>(
                    pipe.getHeader(), pipe.numOutputPorts(), sort_description, max_block_size, /*max_block_size_bytes=*/0, SortingQueueStrategy::Batch);

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
            out_projection = createProjection(pipe_header);
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
    Names partition_key_columns,
    size_t max_block_size)
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
                            sort_description, max_block_size, /*max_block_size_bytes=*/0, SortingQueueStrategy::Batch);

            case MergeTreeData::MergingParams::Collapsing:
                return std::make_shared<CollapsingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.sign_column, true, max_block_size, /*max_block_size_bytes=*/0);

            case MergeTreeData::MergingParams::Summing:
                return std::make_shared<SummingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.columns_to_sum, partition_key_columns, max_block_size, /*max_block_size_bytes=*/0);

            case MergeTreeData::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedTransform>(header, num_outputs,
                            sort_description, max_block_size, /*max_block_size_bytes=*/0);

            case MergeTreeData::MergingParams::Replacing:
                return std::make_shared<ReplacingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.is_deleted_column, merging_params.version_column, max_block_size, /*max_block_size_bytes=*/0, /*out_row_sources_buf_*/ nullptr, /*use_average_block_sizes*/ false, /*cleanup*/ !merging_params.is_deleted_column.empty());

            case MergeTreeData::MergingParams::VersionedCollapsing:
                return std::make_shared<VersionedCollapsingTransform>(header, num_outputs,
                            sort_description, merging_params.sign_column, max_block_size, /*max_block_size_bytes=*/0);

            case MergeTreeData::MergingParams::Graphite:
                return std::make_shared<GraphiteRollupSortedTransform>(header, num_outputs,
                            sort_description, max_block_size, /*max_block_size_bytes=*/0, merging_params.graphite_params, now);
        }

        UNREACHABLE();
    };

    pipe.addTransform(get_merging_processor());
}


Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsFinal(
    RangesInDataParts && parts_with_ranges, size_t num_streams, const Names & column_names, ActionsDAGPtr & out_projection)
{
    const auto & settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);

    assert(num_streams == requested_num_streams);
    if (num_streams > settings.max_final_threads)
        num_streams = settings.max_final_threads;

    /// If setting do_not_merge_across_partitions_select_final is true than we won't merge parts from different partitions.
    /// We have all parts in parts vector, where parts with same partition are nearby.
    /// So we will store iterators pointed to the beginning of each partition range (and parts.end()),
    /// then we will create a pipe for each partition that will run selecting processor and merging processor
    /// for the parts with this partition. In the end we will unite all the pipes.
    std::vector<RangesInDataParts::iterator> parts_to_merge_ranges;
    auto it = parts_with_ranges.begin();
    parts_to_merge_ranges.push_back(it);

    if (settings.do_not_merge_across_partitions_select_final)
    {
        while (it != parts_with_ranges.end())
        {
            it = std::find_if(
                it, parts_with_ranges.end(), [&it](auto & part) { return it->data_part->info.partition_id != part.data_part->info.partition_id; });
            parts_to_merge_ranges.push_back(it);
        }
        /// We divide threads for each partition equally. But we will create at least the number of partitions threads.
        /// (So, the total number of threads could be more than initial num_streams.
        num_streams /= (parts_to_merge_ranges.size() - 1);
    }
    else
    {
        /// If do_not_merge_across_partitions_select_final is false we just merge all the parts.
        parts_to_merge_ranges.push_back(parts_with_ranges.end());
    }

    Pipes partition_pipes;

    /// If do_not_merge_across_partitions_select_final is true and num_streams > 1
    /// we will store lonely parts with level > 0 to use parallel select on them.
    RangesInDataParts lonely_parts;
    size_t sum_marks_in_lonely_parts = 0;

    auto sorting_expr = std::make_shared<ExpressionActions>(metadata_for_reading->getSortingKey().expression->getActionsDAG().clone());

    for (size_t range_index = 0; range_index < parts_to_merge_ranges.size() - 1; ++range_index)
    {
        Pipes pipes;
        {
            RangesInDataParts new_parts;

            /// If do_not_merge_across_partitions_select_final is true and there is only one part in partition
            /// with level > 0 then we won't postprocess this part and if num_streams > 1 we
            /// can use parallel select on such parts. We save such parts in one vector and then use
            /// MergeTreeReadPool and MergeTreeThreadSelectProcessor for parallel select.
            if (num_streams > 1 && settings.do_not_merge_across_partitions_select_final &&
                std::distance(parts_to_merge_ranges[range_index], parts_to_merge_ranges[range_index + 1]) == 1 &&
                parts_to_merge_ranges[range_index]->data_part->info.level > 0
                && data.merging_params.is_deleted_column.empty())
            {
                sum_marks_in_lonely_parts += parts_to_merge_ranges[range_index]->getMarksCount();
                lonely_parts.push_back(std::move(*parts_to_merge_ranges[range_index]));
                continue;
            }
            else
            {
                for (auto part_it = parts_to_merge_ranges[range_index]; part_it != parts_to_merge_ranges[range_index + 1]; ++part_it)
                {
                    new_parts.emplace_back(part_it->data_part, part_it->alter_conversions, part_it->part_index_in_query, part_it->ranges);
                }
            }

            if (new_parts.empty())
                continue;

            if (num_streams > 1 && metadata_for_reading->hasPrimaryKey())
            {
                // Let's split parts into layers to ensure data parallelism of FINAL.
                auto reading_step_getter = [this, &column_names, &info](auto parts)
                {
                    return this->read(
                        std::move(parts),
                        column_names,
                        ReadFromMergeTree::ReadType::InOrder,
                        1 /* num_streams */,
                        0 /* min_marks_for_concurrent_read */,
                        info.use_uncompressed_cache);
                };
                pipes = buildPipesForReadingByPKRanges(
                    metadata_for_reading->getPrimaryKey(),
                    sorting_expr,
                    std::move(new_parts),
                    num_streams,
                    context,
                    std::move(reading_step_getter));
            }
            else
            {
                pipes.emplace_back(read(
                    std::move(new_parts), column_names, ReadFromMergeTree::ReadType::InOrder, num_streams, 0, info.use_uncompressed_cache));

                pipes.back().addSimpleTransform([sorting_expr](const Block & header)
                                                { return std::make_shared<ExpressionTransform>(header, sorting_expr); });
            }

            /// Drop temporary columns, added by 'sorting_key_expr'
            if (!out_projection)
                out_projection = createProjection(pipes.front().getHeader());
        }

        /// If do_not_merge_across_partitions_select_final is true and there is only one part in partition
        /// with level > 0 then we won't postprocess this part
        if (settings.do_not_merge_across_partitions_select_final &&
            std::distance(parts_to_merge_ranges[range_index], parts_to_merge_ranges[range_index + 1]) == 1 &&
            parts_to_merge_ranges[range_index]->data_part->info.level > 0 &&
            data.merging_params.is_deleted_column.empty())
        {
            partition_pipes.emplace_back(Pipe::unitePipes(std::move(pipes)));
            continue;
        }

        Names sort_columns = metadata_for_reading->getSortingKeyColumns();
        SortDescription sort_description;
        sort_description.compile_sort_description = settings.compile_sort_description;
        sort_description.min_count_to_compile_sort_description = settings.min_count_to_compile_sort_description;

        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);

        Names partition_key_columns = metadata_for_reading->getPartitionKey().column_names;

        for (size_t i = 0; i < sort_columns_size; ++i)
            sort_description.emplace_back(sort_columns[i], 1, 1);

        for (auto & pipe : pipes)
            addMergingFinal(
                pipe,
                sort_description,
                data.merging_params,
                partition_key_columns,
                max_block_size);

        partition_pipes.emplace_back(Pipe::unitePipes(std::move(pipes)));
    }

    if (!lonely_parts.empty())
    {
        size_t num_streams_for_lonely_parts = num_streams * lonely_parts.size();

        const size_t min_marks_for_concurrent_read = MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
            settings.merge_tree_min_rows_for_concurrent_read,
            settings.merge_tree_min_bytes_for_concurrent_read,
            data_settings->index_granularity,
            info.index_granularity_bytes,
            sum_marks_in_lonely_parts);

        /// Reduce the number of num_streams_for_lonely_parts if the data is small.
        if (sum_marks_in_lonely_parts < num_streams_for_lonely_parts * min_marks_for_concurrent_read && lonely_parts.size() < num_streams_for_lonely_parts)
            num_streams_for_lonely_parts = std::max((sum_marks_in_lonely_parts + min_marks_for_concurrent_read - 1) / min_marks_for_concurrent_read, lonely_parts.size());

        auto pipe = read(std::move(lonely_parts), column_names, ReadFromMergeTree::ReadType::Default,
                num_streams_for_lonely_parts, min_marks_for_concurrent_read, info.use_uncompressed_cache);

        /// Drop temporary columns, added by 'sorting_key_expr'
        if (!out_projection)
            out_projection = createProjection(pipe.getHeader());

        pipe.addSimpleTransform([sorting_expr](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, sorting_expr);
        });

        partition_pipes.emplace_back(std::move(pipe));
    }

    return Pipe::unitePipes(std::move(partition_pipes));
}

MergeTreeDataSelectAnalysisResultPtr ReadFromMergeTree::selectRangesToRead(
    MergeTreeData::DataPartsVector parts,
    std::vector<AlterConversionsPtr> alter_conversions) const
{
    return selectRangesToRead(
        std::move(parts),
        std::move(alter_conversions),
        prewhere_info,
        filter_nodes,
        storage_snapshot->metadata,
        metadata_for_reading,
        query_info,
        context,
        requested_num_streams,
        max_block_numbers_to_read,
        data,
        real_column_names,
        sample_factor_column_queried,
        log,
        indexes);
}

static ActionsDAGPtr buildFilterDAG(
    const ContextPtr & context,
    const PrewhereInfoPtr & prewhere_info,
    const ActionDAGNodes & added_filter_nodes,
    const SelectQueryInfo & query_info)
{
    const auto & settings = context->getSettingsRef();
    ActionsDAG::NodeRawConstPtrs nodes;

    if (prewhere_info)
    {
        {
            const auto & node = prewhere_info->prewhere_actions->findInOutputs(prewhere_info->prewhere_column_name);
            nodes.push_back(&node);
        }

        if (prewhere_info->row_level_filter)
        {
            const auto & node = prewhere_info->row_level_filter->findInOutputs(prewhere_info->row_level_column_name);
            nodes.push_back(&node);
        }
    }

    for (const auto & node : added_filter_nodes.nodes)
        nodes.push_back(node);

    std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;

    if (settings.allow_experimental_analyzer && query_info.planner_context)
    {
        const auto & table_expression_data = query_info.planner_context->getTableExpressionDataOrThrow(query_info.table_expression);
        for (const auto & [column_identifier, column_name] : table_expression_data.getColumnIdentifierToColumnName())
        {
            const auto & column = table_expression_data.getColumnOrThrow(column_name);
            node_name_to_input_node_column.emplace(column_identifier, ColumnWithTypeAndName(column.type, column_name));
        }
    }

    return ActionsDAG::buildFilterActionsDAG(nodes, node_name_to_input_node_column, context);
}

static void buildIndexes(
    std::optional<ReadFromMergeTree::Indexes> & indexes,
    ActionsDAGPtr filter_actions_dag,
    const MergeTreeData & data,
    const ContextPtr & context,
    const SelectQueryInfo & query_info,
    const StorageMetadataPtr & metadata_snapshot)
{
    indexes.reset();

    // Build and check if primary key is used when necessary
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    const Names & primary_key_column_names = primary_key.column_names;

    const auto & settings = context->getSettingsRef();
    if (settings.query_plan_optimize_primary_key)
    {
        NameSet array_join_name_set;
        if (query_info.syntax_analyzer_result)
            array_join_name_set = query_info.syntax_analyzer_result->getArrayJoinSourceNameSet();

        indexes.emplace(ReadFromMergeTree::Indexes{{
            filter_actions_dag,
            context,
            primary_key_column_names,
            primary_key.expression,
            array_join_name_set}, {}, {}, {}, false});
    }
    else
    {
        indexes.emplace(ReadFromMergeTree::Indexes{{
            query_info,
            context,
            primary_key_column_names,
            primary_key.expression}, {}, {}, {}, false});
    }

    if (metadata_snapshot->hasPartitionKey())
    {
        const auto & partition_key = metadata_snapshot->getPartitionKey();
        auto minmax_columns_names = data.getMinMaxColumnsNames(partition_key);
        auto minmax_expression_actions = data.getMinMaxExpr(partition_key, ExpressionActionsSettings::fromContext(context));

        indexes->minmax_idx_condition.emplace(filter_actions_dag, context, minmax_columns_names, minmax_expression_actions, NameSet());
        indexes->partition_pruner.emplace(metadata_snapshot, filter_actions_dag, context, false /* strict */);
    }

    indexes->use_skip_indexes = settings.use_skip_indexes;
    bool final = query_info.isFinal();

    if (final && !settings.use_skip_indexes_if_final)
        indexes->use_skip_indexes = false;

    if (!indexes->use_skip_indexes)
        return;

    const SelectQueryInfo * info = &query_info;
    std::optional<SelectQueryInfo> info_copy;
    if (settings.allow_experimental_analyzer)
    {
        info_copy.emplace(query_info);
        info_copy->filter_actions_dag = filter_actions_dag;
        info = &*info_copy;
    }

    std::unordered_set<std::string> ignored_index_names;

    if (settings.ignore_data_skipping_indices.changed)
    {
        const auto & indices = settings.ignore_data_skipping_indices.toString();
        Tokens tokens(indices.data(), indices.data() + indices.size(), settings.max_query_size);
        IParser::Pos pos(tokens, static_cast<unsigned>(settings.max_parser_depth));
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

    UsefulSkipIndexes skip_indexes;
    using Key = std::pair<String, size_t>;
    std::map<Key, size_t> merged;

    for (const auto & index : metadata_snapshot->getSecondaryIndices())
    {
        if (!ignored_index_names.contains(index.name))
        {
            auto index_helper = MergeTreeIndexFactory::instance().get(index);
            if (index_helper->isMergeable())
            {
                auto [it, inserted] = merged.emplace(Key{index_helper->index.type, index_helper->getGranularity()}, skip_indexes.merged_indices.size());
                if (inserted)
                {
                    skip_indexes.merged_indices.emplace_back();
                    skip_indexes.merged_indices.back().condition = index_helper->createIndexMergedCondition(*info, metadata_snapshot);
                }

                skip_indexes.merged_indices[it->second].addIndex(index_helper);
            }
            else
            {
                auto condition = index_helper->createIndexCondition(*info, context);
                if (!condition->alwaysUnknownOrTrue())
                    skip_indexes.useful_indices.emplace_back(index_helper, condition);
            }
        }
    }

    indexes->skip_indexes = std::move(skip_indexes);
}

void ReadFromMergeTree::applyFilters()
{
    auto filter_actions_dag = buildFilterDAG(context, prewhere_info, filter_nodes, query_info);
    buildIndexes(indexes, filter_actions_dag, data, context, query_info, metadata_for_reading);
}

MergeTreeDataSelectAnalysisResultPtr ReadFromMergeTree::selectRangesToRead(
    MergeTreeData::DataPartsVector parts,
    std::vector<AlterConversionsPtr> alter_conversions,
    const PrewhereInfoPtr & prewhere_info,
    const ActionDAGNodes & added_filter_nodes,
    const StorageMetadataPtr & metadata_snapshot_base,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    size_t num_streams,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read,
    const MergeTreeData & data,
    const Names & real_column_names,
    bool sample_factor_column_queried,
    Poco::Logger * log,
    std::optional<Indexes> & indexes)
{
    const auto & settings = context->getSettingsRef();
    if (settings.allow_experimental_analyzer || settings.query_plan_optimize_primary_key)
    {
        auto updated_query_info_with_filter_dag = query_info;
        updated_query_info_with_filter_dag.filter_actions_dag = buildFilterDAG(context, prewhere_info, added_filter_nodes, query_info);

        return selectRangesToReadImpl(
            std::move(parts),
            std::move(alter_conversions),
            metadata_snapshot_base,
            metadata_snapshot,
            updated_query_info_with_filter_dag,
            context,
            num_streams,
            max_block_numbers_to_read,
            data,
            real_column_names,
            sample_factor_column_queried,
            log,
            indexes);
    }

    return selectRangesToReadImpl(
        std::move(parts),
        std::move(alter_conversions),
        metadata_snapshot_base,
        metadata_snapshot,
        query_info,
        context,
        num_streams,
        max_block_numbers_to_read,
        data,
        real_column_names,
        sample_factor_column_queried,
        log,
        indexes);
}

MergeTreeDataSelectAnalysisResultPtr ReadFromMergeTree::selectRangesToReadImpl(
    MergeTreeData::DataPartsVector parts,
    std::vector<AlterConversionsPtr> alter_conversions,
    const StorageMetadataPtr & metadata_snapshot_base,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    size_t num_streams,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read,
    const MergeTreeData & data,
    const Names & real_column_names,
    bool sample_factor_column_queried,
    Poco::Logger * log,
    std::optional<Indexes> & indexes)
{
    AnalysisResult result;
    const auto & settings = context->getSettingsRef();

    size_t total_parts = parts.size();

    /// TODO Support row_policy_filter and additional_filters
    auto part_values = MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(data, parts, query_info.query, context);
    if (part_values && part_values->empty())
        return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{.result = std::move(result)});

    result.column_names_to_read = real_column_names;

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
        buildIndexes(indexes, query_info.filter_actions_dag, data, context, query_info, metadata_snapshot);

    if (settings.force_primary_key && indexes->key_condition.alwaysUnknownOrTrue())
    {
        return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{
            .result = std::make_exception_ptr(Exception(
                ErrorCodes::INDEX_NOT_USED,
                "Primary key ({}) is not used and setting 'force_primary_key' is set",
                fmt::join(primary_key_column_names, ", ")))});
    }
    LOG_DEBUG(log, "Key condition: {}", indexes->key_condition.toString());

    if (indexes->key_condition.alwaysFalse())
        return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{.result = std::move(result)});

    size_t total_marks_pk = 0;
    size_t parts_before_pk = 0;
    try
    {
        MergeTreeDataSelectExecutor::filterPartsByPartition(
            indexes->partition_pruner,
            indexes->minmax_idx_condition,
            parts,
            alter_conversions,
            part_values,
            metadata_snapshot_base,
            data,
            context,
            max_block_numbers_to_read.get(),
            log,
            result.index_stats);

        result.sampling = MergeTreeDataSelectExecutor::getSampling(
            query_info,
            metadata_snapshot->getColumns().getAllPhysical(),
            parts,
            indexes->key_condition,
            data,
            metadata_snapshot,
            context,
            sample_factor_column_queried,
            log);

        if (result.sampling.read_nothing)
            return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{.result = std::move(result)});

        for (const auto & part : parts)
            total_marks_pk += part->index_granularity.getMarksCountWithoutFinal();
        parts_before_pk = parts.size();

        auto reader_settings = getMergeTreeReaderSettings(context, query_info);

        result.parts_with_ranges = MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(
            std::move(parts),
            std::move(alter_conversions),
            metadata_snapshot,
            context,
            indexes->key_condition,
            indexes->skip_indexes,
            reader_settings,
            log,
            num_streams,
            result.index_stats,
            indexes->use_skip_indexes);
    }
    catch (...)
    {
        return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{.result = std::current_exception()});
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

    result.total_parts = total_parts;
    result.parts_before_pk = parts_before_pk;
    result.selected_parts = result.parts_with_ranges.size();
    result.selected_ranges = sum_ranges;
    result.selected_marks = sum_marks;
    result.selected_marks_pk = sum_marks_pk;
    result.total_marks_pk = total_marks_pk;
    result.selected_rows = sum_rows;

    const auto & input_order_info = query_info.getInputOrderInfo();
    if (input_order_info)
        result.read_type = (input_order_info->direction > 0) ? ReadType::InOrder
                                                             : ReadType::InReverseOrder;

    return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{.result = std::move(result)});
}

bool ReadFromMergeTree::requestReadingInOrder(size_t prefix_size, int direction, size_t limit)
{
    /// if dirction is not set, use current one
    if (!direction)
        direction = getSortDirection();

    /// Disable read-in-order optimization for reverse order with final.
    /// Otherwise, it can lead to incorrect final behavior because the implementation may rely on the reading in direct order).
    if (direction != 1 && query_info.isFinal())
        return false;

    auto order_info = std::make_shared<InputOrderInfo>(SortDescription{}, prefix_size, direction, limit);
    if (query_info.projection)
        query_info.projection->input_order_info = order_info;
    else
        query_info.input_order_info = order_info;

    reader_settings.read_in_order = true;

    /// In case or read-in-order, don't create too many reading streams.
    /// Almost always we are reading from a single stream at a time because of merge sort.
    if (output_streams_limit)
        requested_num_streams = output_streams_limit;

    /// update sort info for output stream
    SortDescription sort_description;
    const Names & sorting_key_columns = metadata_for_reading->getSortingKeyColumns();
    const Block & header = output_stream->header;
    const int sort_direction = getSortDirection();
    for (const auto & column_name : sorting_key_columns)
    {
        if (std::find_if(header.begin(), header.end(), [&](ColumnWithTypeAndName const & col) { return col.name == column_name; })
            == header.end())
            break;
        sort_description.emplace_back(column_name, sort_direction);
    }
    if (!sort_description.empty())
    {
        const size_t used_prefix_of_sorting_key_size = order_info->used_prefix_of_sorting_key_size;
        if (sort_description.size() > used_prefix_of_sorting_key_size)
            sort_description.resize(used_prefix_of_sorting_key_size);
        output_stream->sort_description = std::move(sort_description);
        output_stream->sort_scope = DataStream::SortScope::Stream;
    }

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
    output_stream = DataStream{.header = IMergeTreeSelectAlgorithm::transformHeader(
        storage_snapshot->getSampleBlockForColumns(real_column_names),
        prewhere_info_value,
        data.getPartitionValueType(),
        virt_column_names)};
    updateSortDescriptionForOutputStream(
        *output_stream,
        storage_snapshot->getMetadataForQuery()->getSortingKeyColumns(),
        getSortDirection(),
        query_info.getInputOrderInfo(),
        prewhere_info);
}

bool ReadFromMergeTree::requestOutputEachPartitionThroughSeparatePort()
{
    if (isQueryWithFinal())
        return false;

    const auto & settings = context->getSettingsRef();

    const auto partitions_cnt = countPartitions(prepared_parts);
    if (!settings.force_aggregate_partitions_independently && (partitions_cnt == 1 || partitions_cnt < settings.max_threads / 2))
    {
        LOG_TRACE(
            log,
            "Independent aggregation by partitions won't be used because there are too few of them: {}. You can set "
            "force_aggregate_partitions_independently to suppress this check",
            partitions_cnt);
        return false;
    }

    if (!settings.force_aggregate_partitions_independently
        && (partitions_cnt > settings.max_number_of_partitions_for_independent_aggregation))
    {
        LOG_TRACE(
            log,
            "Independent aggregation by partitions won't be used because there are too many of them: {}. You can increase "
            "max_number_of_partitions_for_independent_aggregation (current value is {}) or set "
            "force_aggregate_partitions_independently to suppress this check",
            partitions_cnt,
            settings.max_number_of_partitions_for_independent_aggregation);
        return false;
    }

    if (!settings.force_aggregate_partitions_independently)
    {
        std::unordered_map<String, size_t> partition_rows;
        for (const auto & part : prepared_parts)
            partition_rows[part->info.partition_id] += part->rows_count;
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
        const size_t avg_rows_in_partition = sum_rows / settings.max_threads;
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

ReadFromMergeTree::AnalysisResult ReadFromMergeTree::getAnalysisResult() const
{
    auto result_ptr = analyzed_result_ptr ? analyzed_result_ptr : selectRangesToRead(prepared_parts, alter_conversions_for_parts);
    if (std::holds_alternative<std::exception_ptr>(result_ptr->result))
        std::rethrow_exception(std::get<std::exception_ptr>(result_ptr->result));

    return std::get<AnalysisResult>(result_ptr->result);
}

bool ReadFromMergeTree::isQueryWithFinal() const
{
    return query_info.isFinal();
}

bool ReadFromMergeTree::isQueryWithSampling() const
{
    if (context->getSettingsRef().parallel_replicas_count > 1 && data.supportsSampling())
        return true;

    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (query_info.table_expression_modifiers)
        return query_info.table_expression_modifiers->getSampleSizeRatio() != std::nullopt;
    else
        return select.sampleSize() != nullptr;
}

Pipe ReadFromMergeTree::spreadMarkRanges(
    RangesInDataParts && parts_with_ranges, size_t num_streams, AnalysisResult & result, ActionsDAGPtr & result_projection)
{
    const bool final = isQueryWithFinal();
    const auto & input_order_info = query_info.getInputOrderInfo();

    Names column_names_to_read = result.column_names_to_read;

    if (!final && result.sampling.use_sampling)
    {
        /// Add columns needed for `sample_by_ast` to `column_names_to_read`.
        /// Skip this if final was used, because such columns were already added from PK.
        std::vector<String> add_columns = result.sampling.filter_expression->getRequiredColumns().getNames();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());
        ::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());
    }

    if (final)
    {
        chassert(!is_parallel_reading_from_replicas);

        if (output_each_partition_through_separate_port)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimisation isn't supposed to be used for queries with final");

        /// Add columns needed to calculate the sorting expression and the sign.
        std::vector<String> add_columns = metadata_for_reading->getColumnsRequiredForSortingKey();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());

        if (!data.merging_params.is_deleted_column.empty())
            column_names_to_read.push_back(data.merging_params.is_deleted_column);
        if (!data.merging_params.sign_column.empty())
            column_names_to_read.push_back(data.merging_params.sign_column);
        if (!data.merging_params.version_column.empty())
            column_names_to_read.push_back(data.merging_params.version_column);

        ::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());

        return spreadMarkRangesAmongStreamsFinal(std::move(parts_with_ranges), num_streams, column_names_to_read, result_projection);
    }
    else if (input_order_info)
    {
        return spreadMarkRangesAmongStreamsWithOrder(
            std::move(parts_with_ranges), num_streams, column_names_to_read, result_projection, input_order_info);
    }
    else
    {
        return spreadMarkRangesAmongStreams(std::move(parts_with_ranges), num_streams, column_names_to_read);
    }
}

Pipe ReadFromMergeTree::groupStreamsByPartition(AnalysisResult & result, ActionsDAGPtr & result_projection)
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
                [&end](const auto & part) { return end->data_part->info.partition_id != part.data_part->info.partition_id; });

        RangesInDataParts partition_parts{std::make_move_iterator(begin), std::make_move_iterator(end)};

        pipes.emplace_back(spreadMarkRanges(std::move(partition_parts), num_streams, result, result_projection));
        if (!pipes.back().empty())
            pipes.back().resize(1);
    }

    return Pipe::unitePipes(std::move(pipes));
}

void ReadFromMergeTree::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto result = getAnalysisResult();
    result.checkLimits(context->getSettingsRef(), query_info);

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
                fmt::format("{}.{}", data.getStorageID().getFullNameNotQuoted(), part.data_part->info.partition_id));
        }
        context->getQueryContext()->addQueryAccessInfo(partition_names);

        if (storage_snapshot->projection)
            context->getQueryContext()->addQueryAccessInfo(
                Context::QualifiedProjectionName{.storage_id = data.getStorageID(), .projection_name = storage_snapshot->projection->name});
    }

    ProfileEvents::increment(ProfileEvents::SelectedParts, result.selected_parts);
    ProfileEvents::increment(ProfileEvents::SelectedRanges, result.selected_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, result.selected_marks);

    auto query_id_holder = MergeTreeDataSelectExecutor::checkLimits(data, result, context);

    if (result.parts_with_ranges.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    selected_marks = result.selected_marks;
    selected_rows = result.selected_rows;
    selected_parts = result.selected_parts;
    /// Projection, that needed to drop columns, which have appeared by execution
    /// of some extra expressions, and to allow execute the same expressions later.
    /// NOTE: It may lead to double computation of expressions.
    ActionsDAGPtr result_projection;

    Pipe pipe = output_each_partition_through_separate_port
        ? groupStreamsByPartition(result, result_projection)
        : spreadMarkRanges(std::move(result.parts_with_ranges), requested_num_streams, result, result_projection);

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(query_info.storage_limits);

    if (pipe.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    if (result.sampling.use_sampling)
    {
        auto sampling_actions = std::make_shared<ExpressionActions>(result.sampling.filter_expression);
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

    auto append_actions = [&result_projection](ActionsDAGPtr actions)
    {
        if (!result_projection)
            result_projection = std::move(actions);
        else
            result_projection = ActionsDAG::merge(std::move(*result_projection), std::move(*actions));
    };

    /// By the way, if a distributed query or query to a Merge table is made, then the `_sample_factor` column can have different values.
    if (sample_factor_column_queried)
    {
        ColumnWithTypeAndName column;
        column.name = "_sample_factor";
        column.type = std::make_shared<DataTypeFloat64>();
        column.column = column.type->createColumnConst(0, Field(result.sampling.used_sample_factor));

        auto adding_column = ActionsDAG::makeAddingColumnActions(std::move(column));
        append_actions(std::move(adding_column));
    }

    if (result_projection)
        cur_header = result_projection->updateHeader(cur_header);

    /// Extra columns may be returned (for example, if sampling is used).
    /// Convert pipe to step header structure.
    if (!isCompatibleHeader(cur_header, getOutputStream().header))
    {
        auto converting = ActionsDAG::makeConvertingActions(
            cur_header.getColumnsWithTypeAndName(),
            getOutputStream().header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        append_actions(std::move(converting));
    }

    if (result_projection)
    {
        auto projection_actions = std::make_shared<ExpressionActions>(result_projection);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, projection_actions);
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
    }

    UNREACHABLE();
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

    UNREACHABLE();
}

void ReadFromMergeTree::describeActions(FormatSettings & format_settings) const
{
    auto result = getAnalysisResult();
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

        if (prewhere_info->prewhere_actions)
        {
            format_settings.out << prefix << "Prewhere filter" << '\n';
            format_settings.out << prefix << "Prewhere filter column: " << prewhere_info->prewhere_column_name;
            if (prewhere_info->remove_prewhere_column)
               format_settings.out << " (removed)";
            format_settings.out << '\n';

            auto expression = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions);
            expression->describeActions(format_settings.out, prefix);
        }

        if (prewhere_info->row_level_filter)
        {
            format_settings.out << prefix << "Row level filter" << '\n';
            format_settings.out << prefix << "Row level filter column: " << prewhere_info->row_level_column_name << '\n';

            auto expression = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter);
            expression->describeActions(format_settings.out, prefix);
        }
    }
}

void ReadFromMergeTree::describeActions(JSONBuilder::JSONMap & map) const
{
    auto result = getAnalysisResult();
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

        if (prewhere_info->prewhere_actions)
        {
            std::unique_ptr<JSONBuilder::JSONMap> prewhere_filter_map = std::make_unique<JSONBuilder::JSONMap>();
            prewhere_filter_map->add("Prewhere filter column", prewhere_info->prewhere_column_name);
            prewhere_filter_map->add("Prewhere filter remove filter column", prewhere_info->remove_prewhere_column);
            auto expression = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions);
            prewhere_filter_map->add("Prewhere filter expression", expression->toTree());

            prewhere_info_map->add("Prewhere filter", std::move(prewhere_filter_map));
        }

        if (prewhere_info->row_level_filter)
        {
            std::unique_ptr<JSONBuilder::JSONMap> row_level_filter_map = std::make_unique<JSONBuilder::JSONMap>();
            row_level_filter_map->add("Row level filter column", prewhere_info->row_level_column_name);
            auto expression = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter);
            row_level_filter_map->add("Row level filter expression", expression->toTree());

            prewhere_info_map->add("Row level filter", std::move(row_level_filter_map));
        }

        map.add("Prewhere info", std::move(prewhere_info_map));
    }
}

void ReadFromMergeTree::describeIndexes(FormatSettings & format_settings) const
{
    auto result = getAnalysisResult();
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
                format_settings.out << prefix << indent << indent << "Keys: " << stat.name << '\n';
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
        }
    }
}

void ReadFromMergeTree::describeIndexes(JSONBuilder::JSONMap & map) const
{
    auto result = getAnalysisResult();
    auto index_stats = std::move(result.index_stats);

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

bool MergeTreeDataSelectAnalysisResult::error() const
{
    return std::holds_alternative<std::exception_ptr>(result);
}

size_t MergeTreeDataSelectAnalysisResult::marks() const
{
    if (std::holds_alternative<std::exception_ptr>(result))
        std::rethrow_exception(std::get<std::exception_ptr>(result));

    const auto & index_stats = std::get<ReadFromMergeTree::AnalysisResult>(result).index_stats;
    if (index_stats.empty())
        return 0;
    return index_stats.back().num_granules_after;
}

}
