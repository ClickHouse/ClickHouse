#include "Common/ActionBlocker.h"
#include "Storages/MergeTree/MergeTreeData.h"
#include "Storages/MergeTree/IMergeTreeDataPart.h"
#include "Storages/MergeTree/MergeTask.h"
#include "Storages/MergeTree/MergeProgress.h"
#include "Storages/MergeTree/MergeTreeSequentialSource.h"
#include "Processors/Transforms/ExpressionTransform.h"
#include "Processors/Merges/MergingSortedTransform.h"
#include "Processors/Merges/CollapsingSortedTransform.h"
#include "Processors/Merges/SummingSortedTransform.h"
#include "Processors/Merges/ReplacingSortedTransform.h"
#include "Processors/Merges/GraphiteRollupSortedTransform.h"
#include "Processors/Merges/AggregatingSortedTransform.h"
#include "Processors/Merges/VersionedCollapsingTransform.h"
#include "Processors/Executors/PipelineExecutingBlockInputStream.h"
#include "Processors/Executors/DistinctSortedBlockInputStream.h"
#include "Processors/Executors/TTLBlockInputStream.h"


#include <memory>


namespace DB
{

static ActionBlocker merges_blocker;
static ActionBlocker ttl_merges_blocker;

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int DIRECTORY_ALREADY_EXISTS;

}


/// PK columns are sorted and merged, ordinary columns are gathered using info from merge step
static void extractMergingAndGatheringColumns(
    const NamesAndTypesList & storage_columns,
    const ExpressionActionsPtr & sorting_key_expr,
    const IndicesDescription & indexes,
    const ProjectionsDescription & projections,
    const MergeTreeData::MergingParams & merging_params,
    NamesAndTypesList & gathering_columns, Names & gathering_column_names,
    NamesAndTypesList & merging_columns, Names & merging_column_names)
{
    Names sort_key_columns_vec = sorting_key_expr->getRequiredColumns();
    std::set<String> key_columns(sort_key_columns_vec.cbegin(), sort_key_columns_vec.cend());
    for (const auto & index : indexes)
    {
        Names index_columns_vec = index.expression->getRequiredColumns();
        std::copy(index_columns_vec.cbegin(), index_columns_vec.cend(),
                  std::inserter(key_columns, key_columns.end()));
    }

    for (const auto & projection : projections)
    {
        Names projection_columns_vec = projection.required_columns;
        std::copy(projection_columns_vec.cbegin(), projection_columns_vec.cend(),
                  std::inserter(key_columns, key_columns.end()));
    }

    /// Force sign column for Collapsing mode
    if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
        key_columns.emplace(merging_params.sign_column);

    /// Force version column for Replacing mode
    if (merging_params.mode == MergeTreeData::MergingParams::Replacing)
        key_columns.emplace(merging_params.version_column);

    /// Force sign column for VersionedCollapsing mode. Version is already in primary key.
    if (merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
        key_columns.emplace(merging_params.sign_column);

    /// Force to merge at least one column in case of empty key
    if (key_columns.empty())
        key_columns.emplace(storage_columns.front().name);

    /// TODO: also force "summing" and "aggregating" columns to make Horizontal merge only for such columns

    for (const auto & column : storage_columns)
    {
        if (key_columns.count(column.name))
        {
            merging_columns.emplace_back(column);
            merging_column_names.emplace_back(column.name);
        }
        else
        {
            gathering_columns.emplace_back(column);
            gathering_column_names.emplace_back(column.name);
        }
    }
}

bool MergeTaskChain::execute()
{
    if (tasks.front()->execute())
        return true;

    tasks.pop_front();
    return !tasks.empty();
}


void MergeTask::prepare()
{
    const String tmp_prefix = parent_part ? prefix : "tmp_merge_";

    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    /// We don't want to perform merge assigned with TTL as normal merge, so
    /// throw exception
    if (isTTLMergeType(future_part.merge_type) && ttl_merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts with TTL", ErrorCodes::ABORTED);

    LOG_DEBUG(log, "Merging {} parts: from {} to {} into {}",
        future_part.parts.size(),
        future_part.parts.front()->name,
        future_part.parts.back()->name,
        future_part.type.toString());

    if (deduplicate)
    {
        if (deduplicate_by_columns.empty())
            LOG_DEBUG(log, "DEDUPLICATE BY all columns");
        else
            LOG_DEBUG(log, "DEDUPLICATE BY ('{}')", fmt::join(deduplicate_by_columns, "', '"));
    }

    disk = space_reservation->getDisk();
    new_part_tmp_path = data.relative_data_path + tmp_prefix + future_part.name + (parent_part ? ".proj" : "") + "/";
    if (disk->exists(new_part_tmp_path))
        throw Exception("Directory " + fullPath(disk, new_part_tmp_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);


    all_column_names = metadata_snapshot->getColumns().getNamesOfPhysical();
    storage_columns = metadata_snapshot->getColumns().getAllPhysical();


    extractMergingAndGatheringColumns(
        storage_columns,
        metadata_snapshot->getSortingKey().expression,
        metadata_snapshot->getSecondaryIndices(),
        metadata_snapshot->getProjections(),
        merging_params,
        gathering_columns,
        gathering_column_names,
        merging_columns,
        merging_column_names);


    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + future_part.name, disk, 0);
    MergeTreeData::MutableDataPartPtr new_data_part = data.createPart(
        future_part.name,
        future_part.type,
        future_part.part_info,
        single_disk_volume,
        tmp_prefix + future_part.name + (parent_part ? ".proj" : ""),
        parent_part);

    new_data_part->uuid = future_part.uuid;
    new_data_part->setColumns(storage_columns);
    new_data_part->partition.assign(future_part.getPartition());
    new_data_part->is_temp = parent_part == nullptr;

    need_remove_expired_values = false;
    force_ttl = false;
    for (const auto & part : future_part.parts)
    {
        new_data_part->ttl_infos.update(part->ttl_infos);
        if (metadata_snapshot->hasAnyTTL() && !part->checkAllTTLCalculated(metadata_snapshot))
        {
            LOG_INFO(log, "Some TTL values were not calculated for part {}. Will calculate them forcefully during merge.", part->name);
            need_remove_expired_values = true;
            force_ttl = true;
        }
    }

    const auto & part_min_ttl = new_data_part->ttl_infos.part_min_ttl;
    if (part_min_ttl && part_min_ttl <= time_of_merge)
        need_remove_expired_values = true;

    if (need_remove_expired_values && ttl_merges_blocker.isCancelled())
    {
        LOG_INFO(log, "Part {} has values with expired TTL, but merges with TTL are cancelled.", new_data_part->name);
        need_remove_expired_values = false;
    }

    implementation = createMergeAlgorithmImplementation();

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes())
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    auto compression_codec = data.getCompressionCodecForPart(merge_entry->total_size_bytes_compressed, new_data_part->ttl_infos, time_of_merge);

    tmp_disk = context->getTemporaryVolume()->getDisk();

    /// If merge is vertical we cannot calculate it
    blocks_are_granules_size = (chosen_merge_algorithm == MergeAlgorithm::Vertical);

    merged_stream = createMergedStream();
    to = std::make_shared<IMergedBlockOutputStream>(
        new_data_part,
        metadata_snapshot,
        merging_columns,
        MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices()),
        compression_codec,
        blocks_are_granules_size);

    merged_stream->readPrefix();
    to.writePrefix();

    rows_written = 0;
    initial_reservation = space_reservation ? space_reservation->getSize() : 0;

    is_cancelled = [&]() -> bool
    {
        return merges_blocker.isCancelled() || (need_remove_expired_values && ttl_merges_blocker.isCancelled());
    };

    /// This is the end of preparation. Execution will be per block.
}


bool MergeTask::executeHorizontalForBlock()
{
    Block block;
    if (!is_cancelled() && (block = merged_stream->read()))
    {
        rows_written += block.rows();

        to.write(block);

        merge_entry->rows_written = merged_stream->getProfileInfo().rows;
        merge_entry->bytes_written_uncompressed = merged_stream->getProfileInfo().bytes;

        /// Reservation updates is not performed yet, during the merge it may lead to higher free space requirements
        if (space_reservation && sum_input_rows_upper_bound)
        {
            /// The same progress from merge_entry could be used for both algorithms (it should be more accurate)
            /// But now we are using inaccurate row-based estimation in Horizontal case for backward compatibility
            Float64 progress = (chosen_merge_algorithm == MergeAlgorithm::Horizontal)
                ? std::min(1., 1. * rows_written / sum_input_rows_upper_bound)
                : std::min(1., merge_entry->progress.load(std::memory_order_relaxed));

            space_reservation->update(static_cast<size_t>((1. - progress) * initial_reservation));
        }
    }
}



bool MergeTask::execute()
{
    switch (state)
    {
        case MergeTaskState::NEED_PREPARE:
        {
            prepare();
            state = MergeTaskState::NEED_EXECUTE_HORIZONTAL;
            return true;
        }
        case MergeTaskState::NEED_EXECUTE_HORIZONTAL:
        {
            if (executeHorizontalForBlock())
                return true;

            state = MergeTaskState::NEED_EXECUTE_VERTICAL:
        }
        case MergeTaskState::NEED_EXECUTE_VERTICAL:
        {
            /// Check for the choosen algorithm
            return true;
        }
        case MergeTaskState::NEED_FINISH:
        {
            return true;
        }
    }
}




BlockInputStreamPtr MergeTask::createMergedStream()
{
    /** Read from all parts, merge and write into a new one.
      * In passing, we calculate expression for sorting.
      */
    Pipes pipes;
    UInt64 watch_prev_elapsed = 0;

    /// We count total amount of bytes in parts
    /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
    bool read_with_direct_io = false;
    const auto data_settings = data.getSettings();
    if (data_settings->min_merge_bytes_to_use_direct_io != 0)
    {
        size_t total_size = 0;
        for (const auto & part : future_part.parts)
        {
            total_size += part->getBytesOnDisk();
            if (total_size >= data_settings->min_merge_bytes_to_use_direct_io)
            {
                LOG_DEBUG(log, "Will merge parts reading files in O_DIRECT");
                read_with_direct_io = true;

                break;
            }
        }
    }

    MergeStageProgress horizontal_stage_progress(
        column_sizes ? column_sizes->keyColumnsWeight() : 1.0);

    for (const auto & part : future_part.parts)
    {
        auto input = std::make_unique<MergeTreeSequentialSource>(
            data, metadata_snapshot, part, merging_column_names, read_with_direct_io, true);

        input->setProgressCallback(
            MergeProgressCallback(merge_entry, watch_prev_elapsed, horizontal_stage_progress));

        Pipe pipe(std::move(input));

        if (metadata_snapshot->hasSortingKey())
        {
            pipe.addSimpleTransform([this](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, metadata_snapshot->getSortingKey().expression);
            });
        }

        pipes.emplace_back(std::move(pipe));
    }


    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    Names partition_key_columns = metadata_snapshot->getPartitionKey().column_names;

    Block header = pipes.at(0).getHeader();
    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(header.getPositionByName(sort_columns[i]), 1, 1);

    /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
    ///  that is going in insertion order.
    ProcessorPtr merged_transform;

    /// If merge is vertical we cannot calculate it
    blocks_are_granules_size = (chosen_merge_algorithm == MergeAlgorithm::Vertical);

    UInt64 merge_block_size = data_settings->merge_max_block_size;
    switch (merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            return std::make_unique<MergingSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size, 0, rows_sources_write_buf.get(), true, blocks_are_granules_size);

        case MergeTreeData::MergingParams::Collapsing:
            return std::make_unique<CollapsingSortedTransform>(
                header, pipes.size(), sort_description, merging_params.sign_column, false,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);

        case MergeTreeData::MergingParams::Summing:
            return std::make_unique<SummingSortedTransform>(
                header, pipes.size(), sort_description, merging_params.columns_to_sum, partition_key_columns, merge_block_size);

        case MergeTreeData::MergingParams::Aggregating:
            return std::make_shared<AggregatingSortedTransform>(header, pipes.size(), sort_description, merge_block_size);

        case MergeTreeData::MergingParams::Replacing:
            return std::make_unique<ReplacingSortedTransform>(
                header, pipes.size(), sort_description, merging_params.version_column,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);

        case MergeTreeData::MergingParams::Graphite:
            return std::make_unique<GraphiteRollupSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size,
                merging_params.graphite_params, time_of_merge);

        case MergeTreeData::MergingParams::VersionedCollapsing:
            return std::make_unique<VersionedCollapsingTransform>(
                header, pipes.size(), sort_description, merging_params.sign_column,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
    }

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    pipeline.addTransform(std::move(merged_transform));
    pipeline.setMaxThreads(1);
    BlockInputStreamPtr merged_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

    if (deduplicate)
        merged_stream = std::make_shared<DistinctSortedBlockInputStream>(merged_stream, sort_description, SizeLimits(), 0 /*limit_hint*/, deduplicate_by_columns);

    if (need_remove_expired_values)
        merged_stream = std::make_shared<TTLBlockInputStream>(merged_stream, data, metadata_snapshot, new_data_part, time_of_merge, force_ttl);

    if (metadata_snapshot->hasSecondaryIndices())
    {
        const auto & indices = metadata_snapshot->getSecondaryIndices();
        merged_stream = std::make_shared<ExpressionBlockInputStream>(
            merged_stream, indices.getSingleExpressionForIndices(metadata_snapshot->getColumns(), data.getContext()));
        merged_stream = std::make_shared<MaterializingBlockInputStream>(merged_stream);
    }

    return merged_stream;
}



std::unique_ptr<MergeTask::MergeImpl> MergeTask::createMergeAlgorithmImplementation()
{
    sum_input_rows_upper_bound = merge_entry->total_rows_count;
    chosen_merge_algorithm = chooseMergeAlgorithm(
        future_part.parts, sum_input_rows_upper_bound, gathering_columns, deduplicate, need_remove_expired_values, merging_params);
    merge_entry->merge_algorithm.store(chosen_merge_algorithm, std::memory_order_relaxed);

    LOG_DEBUG(log, "Selected MergeAlgorithm: {}", toString(chosen_merge_algorithm));

    switch (chosen_merge_algorithm)
    {
        case MergeAlgorithm::Horizontal :
            return std::make_unique<HorizontalMergeImpl>(*this);
        case MergeAlgorithm::Vertical :
            return std::make_unique<VerticalMergeImpl>(*this);
        default :
            throw Exception("Merge algorithm must be chosen", ErrorCodes::LOGICAL_ERROR);
    }

}


void MergeTask::VerticalMergeImpl::begin()
{
    task.tmp_disk->createDirectories(task.new_part_tmp_path);
    task.rows_sources_file_path = task.new_part_tmp_path + "rows_sources";
    task.rows_sources_uncompressed_write_buf = task.tmp_disk->writeFile(task.rows_sources_file_path);
    task.rows_sources_write_buf = std::make_unique<CompressedWriteBuffer>(*task.rows_sources_uncompressed_write_buf);

    ColumnToSize merged_column_to_size;
    for (const MergeTreeData::DataPartPtr & part : task.future_part.parts)
        part->accumulateColumnSizes(merged_column_to_size);

    task.column_sizes = ColumnSizeEstimator(
        task.merged_column_to_size,
        task.merging_column_names,
        task.gathering_column_names);

    if (task.data.getSettings()->fsync_part_directory)
        task.sync_guard = task.disk->getDirectorySyncGuard(task.new_part_tmp_path);
}



void MergeTask::HorizontalMergeImpl::begin()
{
    task.merging_columns = task.storage_columns;
    task.merging_column_names = task.all_column_names;
    task.gathering_columns.clear();
    task.gathering_column_names.clear();
}


void MergeTaskChain::add(MergeTaskPtr task)
{
    tasks.emplace_back(task);
}

}
