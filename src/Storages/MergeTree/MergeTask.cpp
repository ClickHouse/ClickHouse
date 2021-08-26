#include "Storages/MergeTree/MergeTask.h"

#include <memory>
#include <fmt/format.h>

#include <common/logger_useful.h>
#include "Common/ActionBlocker.h"

#include "Storages/MergeTree/MergeTreeData.h"
#include "Storages/MergeTree/IMergeTreeDataPart.h"
#include "Storages/MergeTree/MergeTreeSequentialSource.h"
#include "Storages/MergeTree/FutureMergedMutatedPart.h"
#include "Processors/Transforms/ExpressionTransform.h"
#include "Processors/Merges/MergingSortedTransform.h"
#include "Processors/Merges/CollapsingSortedTransform.h"
#include "Processors/Merges/SummingSortedTransform.h"
#include "Processors/Merges/ReplacingSortedTransform.h"
#include "Processors/Merges/GraphiteRollupSortedTransform.h"
#include "Processors/Merges/AggregatingSortedTransform.h"
#include "Processors/Merges/VersionedCollapsingTransform.h"
#include "Processors/Executors/PipelineExecutingBlockInputStream.h"
#include "DataStreams/DistinctSortedBlockInputStream.h"
#include "DataStreams/TTLBlockInputStream.h"
#include <DataStreams/TTLCalcInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/DistinctSortedBlockInputStream.h>

namespace DB
{

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


void MergeTask::prepare()
{
    const String tmp_prefix = parent_part ? prefix : "tmp_merge_";

    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    /// We don't want to perform merge assigned with TTL as normal merge, so
    /// throw exception
    if (isTTLMergeType(future_part->merge_type) && ttl_merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts with TTL", ErrorCodes::ABORTED);

    LOG_DEBUG(log, "Merging {} parts: from {} to {} into {}",
        future_part->parts.size(),
        future_part->parts.front()->name,
        future_part->parts.back()->name,
        future_part->type.toString());

    if (deduplicate)
    {
        if (deduplicate_by_columns.empty())
            LOG_DEBUG(log, "DEDUPLICATE BY all columns");
        else
            LOG_DEBUG(log, "DEDUPLICATE BY ('{}')", fmt::join(deduplicate_by_columns, "', '"));
    }

    disk = space_reservation->getDisk();
    new_part_tmp_path = data.relative_data_path + tmp_prefix + future_part->name + (parent_part ? ".proj" : "") + "/";
    if (disk->exists(new_part_tmp_path))
        throw Exception("Directory " + fullPath(disk, new_part_tmp_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);


    all_column_names = metadata_snapshot->getColumns().getNamesOfPhysical();
    storage_columns = metadata_snapshot->getColumns().getAllPhysical();


    extractMergingAndGatheringColumns(
        storage_columns,
        metadata_snapshot->getSortingKey().expression,
        metadata_snapshot->getSecondaryIndices(),
        merging_params,
        gathering_columns,
        gathering_column_names,
        merging_columns,
        merging_column_names);


    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + future_part->name, disk, 0);
    new_data_part = data.createPart(
        future_part->name,
        future_part->type,
        future_part->part_info,
        single_disk_volume,
        tmp_prefix + future_part->name + (parent_part ? ".proj" : ""),
        parent_part.get());

    new_data_part->uuid = future_part->uuid;
    new_data_part->setColumns(storage_columns);
    new_data_part->partition.assign(future_part->getPartition());
    new_data_part->is_temp = parent_part == nullptr;

    need_remove_expired_values = false;
    force_ttl = false;
    for (const auto & part : future_part->parts)
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

    chosen_merge_algorithm = chooseMergeAlgorithm();
    merge_entry->merge_algorithm.store(chosen_merge_algorithm, std::memory_order_relaxed);

    LOG_DEBUG(log, "Selected MergeAlgorithm: {}", toString(chosen_merge_algorithm));

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes())
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    compression_codec = data.getCompressionCodecForPart(merge_entry->total_size_bytes_compressed, new_data_part->ttl_infos, time_of_merge);

    tmp_disk = context->getTemporaryVolume()->getDisk();

    switch (chosen_merge_algorithm)
    {
        case MergeAlgorithm::Horizontal :
        {
            merging_columns = storage_columns;
            merging_column_names = all_column_names;
            gathering_columns.clear();
            gathering_column_names.clear();
            break;
        }
        case MergeAlgorithm::Vertical :
        {
            tmp_disk->createDirectories(new_part_tmp_path);
            rows_sources_file_path = new_part_tmp_path + "rows_sources";
            rows_sources_uncompressed_write_buf = tmp_disk->writeFile(rows_sources_file_path);
            rows_sources_write_buf = std::make_unique<CompressedWriteBuffer>(*rows_sources_uncompressed_write_buf);

            MergeTreeDataPartInMemory::ColumnToSize merged_column_to_size;
            for (const MergeTreeData::DataPartPtr & part : future_part->parts)
                part->accumulateColumnSizes(merged_column_to_size);

            column_sizes = ColumnSizeEstimator(
                std::move(merged_column_to_size),
                merging_column_names,
                gathering_column_names);

            if (data.getSettings()->fsync_part_directory)
                sync_guard = disk->getDirectorySyncGuard(new_part_tmp_path);

            break;
        }
        default :
            throw Exception("Merge algorithm must be chosen", ErrorCodes::LOGICAL_ERROR);
    }

    /// If merge is vertical we cannot calculate it
    blocks_are_granules_size = (chosen_merge_algorithm == MergeAlgorithm::Vertical);

    /// Merged stream will be created and available as merged_stream variable
    createMergedStream();

    to = std::make_shared<MergedBlockOutputStream>(
        new_data_part,
        metadata_snapshot,
        merging_columns,
        MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices()),
        compression_codec,
        blocks_are_granules_size);

    merged_stream->readPrefix();

    /// TODO: const
    const_cast<MergedBlockOutputStream&>(*to).writePrefix();

    rows_written = 0;
    initial_reservation = space_reservation ? space_reservation->getSize() : 0;

    is_cancelled = [this]() -> bool
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

        const_cast<MergedBlockOutputStream &>(*to).write(block);

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

        /// Need execute again
        return true;
    }

    return false;
}


void MergeTask::finalizeHorizontalPartOfTheMerge()
{
    merged_stream->readSuffix();
    merged_stream.reset();

    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    if (need_remove_expired_values && ttl_merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts with expired TTL", ErrorCodes::ABORTED);

    const auto data_settings = data.getSettings();
    const size_t sum_compressed_bytes_upper_bound = merge_entry->total_size_bytes_compressed;
    need_sync = needSyncPart(sum_input_rows_upper_bound, sum_compressed_bytes_upper_bound, *data_settings);
}


void MergeTask::prepareVertical()
{
     /// No need to execute this part if it is horizontal merge.
    if (chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return;

    size_t sum_input_rows_exact = merge_entry->rows_read;
    merge_entry->columns_written = merging_column_names.size();
    merge_entry->progress.store(column_sizes->keyColumnsWeight(), std::memory_order_relaxed);

    column_part_streams = BlockInputStreams(future_part->parts.size());

    rows_sources_write_buf->next();
    rows_sources_uncompressed_write_buf->next();
    /// Ensure data has written to disk.
    rows_sources_uncompressed_write_buf->finalize();

    size_t rows_sources_count = rows_sources_write_buf->count();
    /// In special case, when there is only one source part, and no rows were skipped, we may have
    /// skipped writing rows_sources file. Otherwise rows_sources_count must be equal to the total
    /// number of input rows.
    if ((rows_sources_count > 0 || future_part->parts.size() > 1) && sum_input_rows_exact != rows_sources_count)
        throw Exception("Number of rows in source parts (" + toString(sum_input_rows_exact)
            + ") differs from number of bytes written to rows_sources file (" + toString(rows_sources_count)
            + "). It is a bug.", ErrorCodes::LOGICAL_ERROR);

    rows_sources_read_buf = std::make_unique<CompressedReadBufferFromFile>(tmp_disk->readFile(rows_sources_file_path));

    /// For external cycle
    gathering_column_names_size = gathering_column_names.size();
    column_num_for_vertical_merge = 0;
    it_name_and_type = gathering_columns.cbegin();
}


void MergeTask::prepareVerticalMergeForOneColumn()
{
    const String & column_name = it_name_and_type->name;
    Names column_names{column_name};

    progress_before = merge_entry->progress.load(std::memory_order_relaxed);

    column_progress = std::make_unique<MergeStageProgress>(progress_before, column_sizes->columnWeight(column_name));

    for (size_t part_num = 0; part_num < future_part->parts.size(); ++part_num)
    {
        auto column_part_source = std::make_shared<MergeTreeSequentialSource>(
            data, metadata_snapshot, future_part->parts[part_num], column_names, read_with_direct_io, true);

        /// Dereference unique_ptr
        column_part_source->setProgressCallback(
            MergeProgressCallback(merge_entry, watch_prev_elapsed, *column_progress));

        QueryPipeline column_part_pipeline;
        column_part_pipeline.init(Pipe(std::move(column_part_source)));
        column_part_pipeline.setMaxThreads(1);

        column_part_streams[part_num] =
                std::make_shared<PipelineExecutingBlockInputStream>(std::move(column_part_pipeline));
    }

    rows_sources_read_buf->seek(0, 0);
    column_gathered_stream = std::make_unique<ColumnGathererStream>(column_name, column_part_streams, *rows_sources_read_buf);

    column_to = std::make_unique<MergedColumnOnlyOutputStream>(
        new_data_part,
        metadata_snapshot,
        column_gathered_stream->getHeader(),
        compression_codec,
        /// we don't need to recalc indices here
        /// because all of them were already recalculated and written
        /// as key part of vertical merge
        std::vector<MergeTreeIndexPtr>{},
        &written_offset_columns,
        to->getIndexGranularity());

    column_elems_written = 0;

    column_to->writePrefix();
}


bool MergeTask::executeVerticalMergeForOneColumn()
{
    Block block;
    if (!merges_blocker.isCancelled() && (block = column_gathered_stream->read()))
    {
        column_elems_written += block.rows();
        column_to->write(block);

        /// Need execute again
        return true;
    }
    return false;
}


void MergeTask::finalizeVerticalMergeForOneColumn()
{
    const String & column_name = it_name_and_type->name;
    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    column_gathered_stream->readSuffix();
    auto changed_checksums = column_to->writeSuffixAndGetChecksums(new_data_part, checksums_gathered_columns, need_sync);
    checksums_gathered_columns.add(std::move(changed_checksums));

    if (rows_written != column_elems_written)
    {
        throw Exception("Written " + toString(column_elems_written) + " elements of column " + column_name +
                        ", but " + toString(rows_written) + " rows of PK columns", ErrorCodes::LOGICAL_ERROR);
    }

    /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).

    merge_entry->columns_written += 1;
    merge_entry->bytes_written_uncompressed += column_gathered_stream->getProfileInfo().bytes;
    merge_entry->progress.store(progress_before + column_sizes->columnWeight(column_name), std::memory_order_relaxed);

    /// This is the external cycle increment.
    ++column_num_for_vertical_merge;
    ++it_name_and_type;
}



void MergeTask::finalizeVerticalMergeForAllColumns()
{
    /// No need to execute this part if it is horizontal merge.
    if (chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return;

    tmp_disk->removeFile(rows_sources_file_path);
}


void MergeTask::mergeMinMaxIndex()
{
    for (const auto & part : future_part->parts)
        new_data_part->minmax_idx->merge(*part->minmax_idx);

    /// Print overall profiling info. NOTE: it may duplicates previous messages
    {
        double elapsed_seconds = merge_entry->watch.elapsedSeconds();
        LOG_DEBUG(log,
            "Merge sorted {} rows, containing {} columns ({} merged, {} gathered) in {} sec., {} rows/sec., {}/sec.",
            merge_entry->rows_read,
            all_column_names.size(),
            merging_column_names.size(),
            gathering_column_names.size(),
            elapsed_seconds,
            merge_entry->rows_read / elapsed_seconds,
            ReadableSize(merge_entry->bytes_read_uncompressed / elapsed_seconds));
    }
}



void MergeTask::prepareProjections()
{
    const auto & projections = metadata_snapshot->getProjections();
    // tasks_for_projections.reserve(projections.size());

    for (const auto & projection : projections)
    {
        MergeTreeData::DataPartsVector projection_parts;
        for (const auto & part : future_part->parts)
        {
            auto it = part->getProjectionParts().find(projection.name);
            if (it != part->getProjectionParts().end())
                projection_parts.push_back(it->second);
        }
        if (projection_parts.size() < future_part->parts.size())
        {
            LOG_DEBUG(log, "Projection {} is not merged because some parts don't have it", projection.name);
            continue;
        }

        LOG_DEBUG(
            log,
            "Selected {} projection_parts from {} to {}",
            projection_parts.size(),
            projection_parts.front()->name,
            projection_parts.back()->name);

        auto projection_future_part = std::make_shared<FutureMergedMutatedPart>();
        projection_future_part->assign(std::move(projection_parts));
        projection_future_part->name = projection.name;
        projection_future_part->path = future_part->path + "/" + projection.name + ".proj/";
        projection_future_part->part_info = {"all", 0, 0, 0};

        MergeTreeData::MergingParams projection_merging_params;
        projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
        if (projection.type == ProjectionDescription::Type::Aggregate)
            projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

        // TODO Should we use a new merge_entry for projection?
        tasks_for_projections.emplace_back(std::make_shared<MergeTask>(
            projection_future_part,
            projection.metadata,
            merge_entry,
            holder,
            time_of_merge,
            context,
            space_reservation,
            deduplicate,
            deduplicate_by_columns,
            projection_merging_params,
            new_data_part,
            "", // empty string for projection
            data,
            merges_blocker,
            ttl_merges_blocker));
    }


    /// We will iterate through projections and execute them
    projections_iterator = tasks_for_projections.begin();
}


bool MergeTask::executeProjections()
{
    if (projections_iterator == tasks_for_projections.end())
        return false;

    (*projections_iterator)->execute();
    ++projections_iterator;
    return true;
}


void MergeTask::finalizeProjections()
{
    const auto & projections = metadata_snapshot->getProjections();

    size_t iter = 0;

    for (const auto & projection : projections)
    {
        auto future = tasks_for_projections[iter]->getFuture();
        ++iter;
        new_data_part->addProjectionPart(projection.name, future.get());
    }
}



void MergeTask::finalize()
{
    if (chosen_merge_algorithm != MergeAlgorithm::Vertical)
        to->writeSuffixAndFinalizePart(new_data_part, need_sync);
    else
        to->writeSuffixAndFinalizePart(new_data_part, need_sync, &storage_columns, &checksums_gathered_columns);

    promise.set_value(new_data_part);
}


bool MergeTask::executeVerticalMergeForAllColumns()
{
    /// No need to execute this part if it is horizontal merge.
    if (chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    /// This is the extenal cycle condition
    if (column_num_for_vertical_merge >= gathering_column_names_size)
        return false;

    switch (vertical_merge_one_column_state)
    {
        case VecticalMergeOneColumnState::NEED_PREPARE:
        {
            prepareVerticalMergeForOneColumn();
            vertical_merge_one_column_state = VecticalMergeOneColumnState::NEED_EXECUTE;
            return true;
        }
        case VecticalMergeOneColumnState::NEED_EXECUTE:
        {
            if (executeVerticalMergeForOneColumn())
                return true;

            vertical_merge_one_column_state = VecticalMergeOneColumnState::NEED_FINISH;
            return true;
        }
        case VecticalMergeOneColumnState::NEED_FINISH:
        {
            finalizeVerticalMergeForOneColumn();
            vertical_merge_one_column_state = VecticalMergeOneColumnState::NEED_PREPARE;
            return true;
        }
    }
    return false;
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

            state = MergeTaskState::NEED_FINALIZE_HORIZONTAL;
            return true;
        }
        case MergeTaskState::NEED_FINALIZE_HORIZONTAL:
        {
            finalizeHorizontalPartOfTheMerge();

            state = MergeTaskState::NEED_PREPARE_VERTICAL;
            return true;
        }
        case MergeTaskState::NEED_PREPARE_VERTICAL:
        {
            prepareVertical();

            state = MergeTaskState::NEED_EXECUTE_VERTICAL;
            return true;
        }
        case MergeTaskState::NEED_EXECUTE_VERTICAL:
        {
            if (executeVerticalMergeForAllColumns())
                return true;

            state = MergeTaskState::NEED_FINISH_VERTICAL;
            return true;
        }
        case MergeTaskState::NEED_FINISH_VERTICAL:
        {
            finalizeVerticalMergeForAllColumns();

            state = MergeTaskState::NEED_MERGE_MIN_MAX_INDEX;
            return true;
        }
        case MergeTaskState::NEED_MERGE_MIN_MAX_INDEX:
        {
            mergeMinMaxIndex();

            state = MergeTaskState::NEED_PREPARE_PROJECTIONS;
            return true;
        }
        case MergeTaskState::NEED_PREPARE_PROJECTIONS:
        {
            prepareProjections();

            state = MergeTaskState::NEED_EXECUTE_PROJECTIONS;
            return true;
        }
        case MergeTaskState::NEED_EXECUTE_PROJECTIONS:
        {
            if (executeProjections())
                return true;

            state = MergeTaskState::NEED_FINISH_PROJECTIONS;
            return true;
        }
        case MergeTaskState::NEED_FINISH_PROJECTIONS:
        {
            finalizeProjections();

            state = MergeTaskState::NEED_FINISH;
            return true;
        }
        case MergeTaskState::NEED_FINISH:
        {
            finalize();

            return false;
        }
    }
    return false;
}


void MergeTask::createMergedStream()
{
    /** Read from all parts, merge and write into a new one.
      * In passing, we calculate expression for sorting.
      */
    Pipes pipes;
    watch_prev_elapsed = 0;

    /// We count total amount of bytes in parts
    /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
    read_with_direct_io = false;
    const auto data_settings = data.getSettings();
    if (data_settings->min_merge_bytes_to_use_direct_io != 0)
    {
        size_t total_size = 0;
        for (const auto & part : future_part->parts)
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

    /// Using unique_ptr, because MergeStageProgress has no default constructor
    horizontal_stage_progress = std::make_unique<MergeStageProgress>(
        column_sizes ? column_sizes->keyColumnsWeight() : 1.0);


    for (const auto & part : future_part->parts)
    {
        auto input = std::make_unique<MergeTreeSequentialSource>(
            data, metadata_snapshot, part, merging_column_names, read_with_direct_io, true);

        /// Dereference unique_ptr and pass horizontal_stage_progress by reference
        input->setProgressCallback(
            MergeProgressCallback(merge_entry, watch_prev_elapsed, *horizontal_stage_progress));

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
            merged_transform = std::make_shared<MergingSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size, 0, false, rows_sources_write_buf.get(), true, blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_transform = std::make_shared<CollapsingSortedTransform>(
                header, pipes.size(), sort_description, merging_params.sign_column, false,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Summing:
            merged_transform = std::make_shared<SummingSortedTransform>(
                header, pipes.size(), sort_description, merging_params.columns_to_sum, partition_key_columns, merge_block_size);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged_transform = std::make_shared<AggregatingSortedTransform>(header, pipes.size(), sort_description, merge_block_size);
            break;

        case MergeTreeData::MergingParams::Replacing:
            merged_transform = std::make_shared<ReplacingSortedTransform>(
                header, pipes.size(), sort_description, merging_params.version_column,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_transform = std::make_shared<GraphiteRollupSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size,
                merging_params.graphite_params, time_of_merge);
            break;

        case MergeTreeData::MergingParams::VersionedCollapsing:
            merged_transform = std::make_shared<VersionedCollapsingTransform>(
                header, pipes.size(), sort_description, merging_params.sign_column,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;
    }

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    pipeline.addTransform(std::move(merged_transform));
    pipeline.setMaxThreads(1);

    merged_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

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
}


MergeAlgorithm MergeTask::chooseMergeAlgorithm() const
{
    const size_t sum_rows_upper_bound = merge_entry->total_rows_count;
    const auto data_settings = data.getSettings();

    if (deduplicate)
        return MergeAlgorithm::Horizontal;
    if (data_settings->enable_vertical_merge_algorithm == 0)
        return MergeAlgorithm::Horizontal;
    if (need_remove_expired_values)
        return MergeAlgorithm::Horizontal;

    for (const auto & part : future_part->parts)
        if (!part->supportsVerticalMerge())
            return MergeAlgorithm::Horizontal;

    bool is_supported_storage =
        merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
        merging_params.mode == MergeTreeData::MergingParams::Replacing ||
        merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

    bool enough_ordinary_cols = gathering_columns.size() >= data_settings->vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = sum_rows_upper_bound >= data_settings->vertical_merge_algorithm_min_rows_to_activate;

    bool no_parts_overflow = future_part->parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_ordinary_cols && no_parts_overflow) ?
                        MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    return merge_alg;
}

}
