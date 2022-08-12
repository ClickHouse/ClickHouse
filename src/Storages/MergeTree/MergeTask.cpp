#include <Storages/MergeTree/MergeTask.h>

#include <memory>
#include <fmt/format.h>

#include <Common/logger_useful.h>
#include <Common/ActionBlocker.h>
#include <Storages/LightweightDeleteDescription.h>
#include <Storages/MergeTree/DataPartStorageOnDisk.h>

#include <DataTypes/ObjectUtils.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Transforms/TTLTransform.h>
#include <Processors/Transforms/TTLCalcTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Transforms/DistinctTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
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
        if (key_columns.contains(column.name))
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


bool MergeTask::ExecuteAndFinalizeHorizontalPart::prepare()
{
    // projection parts have different prefix and suffix compared to normal parts.
    // E.g. `proj_a.proj` for a normal projection merge and `proj_a.tmp_proj` for a projection materialization merge.
    const String local_tmp_prefix = global_ctx->parent_part ? "" : "tmp_merge_";
    const String local_tmp_suffix = global_ctx->parent_part ? ctx->suffix : "";

    if (global_ctx->merges_blocker->isCancelled() || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed))
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    /// We don't want to perform merge assigned with TTL as normal merge, so
    /// throw exception
    if (isTTLMergeType(global_ctx->future_part->merge_type) && global_ctx->ttl_merges_blocker->isCancelled())
        throw Exception("Cancelled merging parts with TTL", ErrorCodes::ABORTED);

    LOG_DEBUG(ctx->log, "Merging {} parts: from {} to {} into {}",
        global_ctx->future_part->parts.size(),
        global_ctx->future_part->parts.front()->name,
        global_ctx->future_part->parts.back()->name,
        global_ctx->future_part->type.toString());

    if (global_ctx->deduplicate)
    {
        if (global_ctx->deduplicate_by_columns.empty())
            LOG_DEBUG(ctx->log, "DEDUPLICATE BY all columns");
        else
            LOG_DEBUG(ctx->log, "DEDUPLICATE BY ('{}')", fmt::join(global_ctx->deduplicate_by_columns, "', '"));
    }

    ctx->disk = global_ctx->space_reservation->getDisk();

    String local_tmp_part_basename = local_tmp_prefix + global_ctx->future_part->name + local_tmp_suffix;

    if (global_ctx->parent_path_storage_builder)
    {
        global_ctx->data_part_storage_builder = global_ctx->parent_path_storage_builder->getProjection(local_tmp_part_basename);
    }
    else
    {
        auto local_single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + global_ctx->future_part->name, ctx->disk, 0);

        global_ctx->data_part_storage_builder = std::make_shared<DataPartStorageBuilderOnDisk>(
            local_single_disk_volume,
            global_ctx->data->relative_data_path,
            local_tmp_part_basename);
    }

    if (global_ctx->data_part_storage_builder->exists())
        throw Exception("Directory " + global_ctx->data_part_storage_builder->getFullPath() + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    global_ctx->data->temporary_parts.add(local_tmp_part_basename);
    SCOPE_EXIT(
        global_ctx->data->temporary_parts.remove(local_tmp_part_basename);
    );

    global_ctx->all_column_names = global_ctx->metadata_snapshot->getColumns().getNamesOfPhysical();
    global_ctx->storage_columns = global_ctx->metadata_snapshot->getColumns().getAllPhysical();

    auto object_columns = MergeTreeData::getObjectColumns(global_ctx->future_part->parts, global_ctx->metadata_snapshot->getColumns());
    global_ctx->storage_snapshot = std::make_shared<StorageSnapshot>(*global_ctx->data, global_ctx->metadata_snapshot, object_columns);
    extendObjectColumns(global_ctx->storage_columns, object_columns, false);

    extractMergingAndGatheringColumns(
        global_ctx->storage_columns,
        global_ctx->metadata_snapshot->getSortingKey().expression,
        global_ctx->metadata_snapshot->getSecondaryIndices(),
        ctx->merging_params,
        global_ctx->gathering_columns,
        global_ctx->gathering_column_names,
        global_ctx->merging_columns,
        global_ctx->merging_column_names);

    auto data_part_storage = global_ctx->data_part_storage_builder->getStorage();

    global_ctx->new_data_part = global_ctx->data->createPart(
        global_ctx->future_part->name,
        global_ctx->future_part->type,
        global_ctx->future_part->part_info,
        data_part_storage,
        global_ctx->parent_part);

    global_ctx->new_data_part->uuid = global_ctx->future_part->uuid;
    global_ctx->new_data_part->partition.assign(global_ctx->future_part->getPartition());
    global_ctx->new_data_part->is_temp = global_ctx->parent_part == nullptr;

    ctx->need_remove_expired_values = false;
    ctx->force_ttl = false;

    SerializationInfo::Settings info_settings =
    {
        .ratio_of_defaults_for_sparse = global_ctx->data->getSettings()->ratio_of_defaults_for_sparse_serialization,
        .choose_kind = true,
    };

    SerializationInfoByName infos(global_ctx->storage_columns, info_settings);

    for (const auto & part : global_ctx->future_part->parts)
    {
        global_ctx->new_data_part->ttl_infos.update(part->ttl_infos);
        if (global_ctx->metadata_snapshot->hasAnyTTL() && !part->checkAllTTLCalculated(global_ctx->metadata_snapshot))
        {
            LOG_INFO(ctx->log, "Some TTL values were not calculated for part {}. Will calculate them forcefully during merge.", part->name);
            ctx->need_remove_expired_values = true;
            ctx->force_ttl = true;
        }

        infos.add(part->getSerializationInfos());
    }

    global_ctx->new_data_part->setColumns(global_ctx->storage_columns, infos);

    const auto & local_part_min_ttl = global_ctx->new_data_part->ttl_infos.part_min_ttl;
    if (local_part_min_ttl && local_part_min_ttl <= global_ctx->time_of_merge)
        ctx->need_remove_expired_values = true;

    if (ctx->need_remove_expired_values && global_ctx->ttl_merges_blocker->isCancelled())
    {
        LOG_INFO(ctx->log, "Part {} has values with expired TTL, but merges with TTL are cancelled.", global_ctx->new_data_part->name);
        ctx->need_remove_expired_values = false;
    }

    ctx->sum_input_rows_upper_bound = global_ctx->merge_list_element_ptr->total_rows_count;
    ctx->sum_compressed_bytes_upper_bound = global_ctx->merge_list_element_ptr->total_size_bytes_compressed;
    global_ctx->chosen_merge_algorithm = chooseMergeAlgorithm();
    global_ctx->merge_list_element_ptr->merge_algorithm.store(global_ctx->chosen_merge_algorithm, std::memory_order_relaxed);

    LOG_DEBUG(ctx->log, "Selected MergeAlgorithm: {}", toString(global_ctx->chosen_merge_algorithm));

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes())
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    ctx->compression_codec = global_ctx->data->getCompressionCodecForPart(
        global_ctx->merge_list_element_ptr->total_size_bytes_compressed, global_ctx->new_data_part->ttl_infos, global_ctx->time_of_merge);

    ctx->tmp_disk = global_ctx->context->getTemporaryVolume()->getDisk();

    switch (global_ctx->chosen_merge_algorithm)
    {
        case MergeAlgorithm::Horizontal :
        {
            global_ctx->merging_columns = global_ctx->storage_columns;
            global_ctx->merging_column_names = global_ctx->all_column_names;
            global_ctx->gathering_columns.clear();
            global_ctx->gathering_column_names.clear();
            break;
        }
        case MergeAlgorithm::Vertical :
        {
            ctx->rows_sources_file = createTemporaryFile(ctx->tmp_disk->getPath());
            ctx->rows_sources_uncompressed_write_buf = ctx->tmp_disk->writeFile(fileName(ctx->rows_sources_file->path()), DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, global_ctx->context->getWriteSettings());
            ctx->rows_sources_write_buf = std::make_unique<CompressedWriteBuffer>(*ctx->rows_sources_uncompressed_write_buf);

            MergeTreeDataPartInMemory::ColumnToSize local_merged_column_to_size;
            for (const MergeTreeData::DataPartPtr & part : global_ctx->future_part->parts)
                part->accumulateColumnSizes(local_merged_column_to_size);

            ctx->column_sizes = ColumnSizeEstimator(
                std::move(local_merged_column_to_size),
                global_ctx->merging_column_names,
                global_ctx->gathering_column_names);

            break;
        }
        default :
            throw Exception("Merge algorithm must be chosen", ErrorCodes::LOGICAL_ERROR);
    }

    assert(global_ctx->gathering_columns.size() == global_ctx->gathering_column_names.size());
    assert(global_ctx->merging_columns.size() == global_ctx->merging_column_names.size());

    /// If merge is vertical we cannot calculate it
    ctx->blocks_are_granules_size = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Vertical);

    /// Merged stream will be created and available as merged_stream variable
    createMergedStream();

    /// Skip fully expired columns manually, since in case of
    /// need_remove_expired_values is not set, TTLTransform will not be used,
    /// and columns that had been removed by TTL (via TTLColumnAlgorithm) will
    /// be added again with default values.
    ///
    /// Also note, that it is better to do this here, since in other places it
    /// will be too late (i.e. they will be written, and we will burn CPU/disk
    /// resources for this).
    if (!ctx->need_remove_expired_values)
    {
        size_t expired_columns = 0;

        for (auto & [column_name, ttl] : global_ctx->new_data_part->ttl_infos.columns_ttl)
        {
            if (ttl.finished())
            {
                global_ctx->new_data_part->expired_columns.insert(column_name);
                LOG_TRACE(ctx->log, "Adding expired column {} for part {}", column_name, global_ctx->new_data_part->name);
                std::erase(global_ctx->gathering_column_names, column_name);
                std::erase(global_ctx->merging_column_names, column_name);
                ++expired_columns;
            }
        }

        if (expired_columns)
        {
            global_ctx->gathering_columns = global_ctx->gathering_columns.filter(global_ctx->gathering_column_names);
            global_ctx->merging_columns = global_ctx->merging_columns.filter(global_ctx->merging_column_names);
        }
    }

    global_ctx->to = std::make_shared<MergedBlockOutputStream>(
        global_ctx->new_data_part,
        global_ctx->data_part_storage_builder,
        global_ctx->metadata_snapshot,
        global_ctx->merging_columns,
        MergeTreeIndexFactory::instance().getMany(global_ctx->metadata_snapshot->getSecondaryIndices()),
        ctx->compression_codec,
        global_ctx->txn,
        /*reset_columns=*/ true,
        ctx->blocks_are_granules_size,
        global_ctx->context->getWriteSettings());

    global_ctx->rows_written = 0;
    ctx->initial_reservation = global_ctx->space_reservation ? global_ctx->space_reservation->getSize() : 0;

    ctx->is_cancelled = [merges_blocker = global_ctx->merges_blocker,
        ttl_merges_blocker = global_ctx->ttl_merges_blocker,
        need_remove = ctx->need_remove_expired_values,
        merge_list_element = global_ctx->merge_list_element_ptr]() -> bool
    {
        return merges_blocker->isCancelled()
            || (need_remove && ttl_merges_blocker->isCancelled())
            || merge_list_element->is_cancelled.load(std::memory_order_relaxed);
    };

    /// This is the end of preparation. Execution will be per block.
    return false;
}


MergeTask::StageRuntimeContextPtr MergeTask::ExecuteAndFinalizeHorizontalPart::getContextForNextStage()
{
    auto new_ctx = std::make_shared<VerticalMergeRuntimeContext>();

    new_ctx->rows_sources_write_buf = std::move(ctx->rows_sources_write_buf);
    new_ctx->rows_sources_uncompressed_write_buf = std::move(ctx->rows_sources_uncompressed_write_buf);
    new_ctx->rows_sources_file = std::move(ctx->rows_sources_file);
    new_ctx->column_sizes = std::move(ctx->column_sizes);
    new_ctx->compression_codec = std::move(ctx->compression_codec);
    new_ctx->tmp_disk = std::move(ctx->tmp_disk);
    new_ctx->it_name_and_type = std::move(ctx->it_name_and_type);
    new_ctx->column_num_for_vertical_merge = std::move(ctx->column_num_for_vertical_merge);
    new_ctx->read_with_direct_io = std::move(ctx->read_with_direct_io);
    new_ctx->need_sync = std::move(ctx->need_sync);

    ctx.reset();
    return new_ctx;
}

MergeTask::StageRuntimeContextPtr MergeTask::VerticalMergeStage::getContextForNextStage()
{
    auto new_ctx = std::make_shared<MergeProjectionsRuntimeContext>();

    new_ctx->need_sync = std::move(ctx->need_sync);

    ctx.reset();
    return new_ctx;
}


bool MergeTask::ExecuteAndFinalizeHorizontalPart::execute()
{
    assert(subtasks_iterator != subtasks.end());
    if ((*subtasks_iterator)())
        return true;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}


bool MergeTask::ExecuteAndFinalizeHorizontalPart::executeImpl()
{
    Block block;
    if (!ctx->is_cancelled() && (global_ctx->merging_executor->pull(block)))
    {
        global_ctx->rows_written += block.rows();

        const_cast<MergedBlockOutputStream &>(*global_ctx->to).write(block);

        UInt64 result_rows = 0;
        UInt64 result_bytes = 0;
        global_ctx->merged_pipeline.tryGetResultRowsAndBytes(result_rows, result_bytes);
        global_ctx->merge_list_element_ptr->rows_written = result_rows;
        global_ctx->merge_list_element_ptr->bytes_written_uncompressed = result_bytes;

        /// Reservation updates is not performed yet, during the merge it may lead to higher free space requirements
        if (global_ctx->space_reservation && ctx->sum_input_rows_upper_bound)
        {
            /// The same progress from merge_entry could be used for both algorithms (it should be more accurate)
            /// But now we are using inaccurate row-based estimation in Horizontal case for backward compatibility
            Float64 progress = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Horizontal)
                ? std::min(1., 1. * global_ctx->rows_written / ctx->sum_input_rows_upper_bound)
                : std::min(1., global_ctx->merge_list_element_ptr->progress.load(std::memory_order_relaxed));

            global_ctx->space_reservation->update(static_cast<size_t>((1. - progress) * ctx->initial_reservation));
        }

        /// Need execute again
        return true;
    }

    global_ctx->merging_executor.reset();
    global_ctx->merged_pipeline.reset();

    if (global_ctx->merges_blocker->isCancelled() || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed))
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    if (ctx->need_remove_expired_values && global_ctx->ttl_merges_blocker->isCancelled())
        throw Exception("Cancelled merging parts with expired TTL", ErrorCodes::ABORTED);

    const auto data_settings = global_ctx->data->getSettings();
    const size_t sum_compressed_bytes_upper_bound = global_ctx->merge_list_element_ptr->total_size_bytes_compressed;
    ctx->need_sync = needSyncPart(ctx->sum_input_rows_upper_bound, sum_compressed_bytes_upper_bound, *data_settings);

    return false;
}


bool MergeTask::VerticalMergeStage::prepareVerticalMergeForAllColumns() const
{
     /// No need to execute this part if it is horizontal merge.
    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    size_t sum_input_rows_exact = global_ctx->merge_list_element_ptr->rows_read;
    global_ctx->merge_list_element_ptr->columns_written = global_ctx->merging_column_names.size();
    global_ctx->merge_list_element_ptr->progress.store(ctx->column_sizes->keyColumnsWeight(), std::memory_order_relaxed);

    ctx->rows_sources_write_buf->next();
    ctx->rows_sources_uncompressed_write_buf->next();
    /// Ensure data has written to disk.
    ctx->rows_sources_uncompressed_write_buf->finalize();

    size_t rows_sources_count = ctx->rows_sources_write_buf->count();
    /// In special case, when there is only one source part, and no rows were skipped, we may have
    /// skipped writing rows_sources file. Otherwise rows_sources_count must be equal to the total
    /// number of input rows.
    if ((rows_sources_count > 0 || global_ctx->future_part->parts.size() > 1) && sum_input_rows_exact != rows_sources_count)
        throw Exception("Number of rows in source parts (" + toString(sum_input_rows_exact)
            + ") differs from number of bytes written to rows_sources file (" + toString(rows_sources_count)
            + "). It is a bug.", ErrorCodes::LOGICAL_ERROR);

    ctx->rows_sources_read_buf = std::make_unique<CompressedReadBufferFromFile>(ctx->tmp_disk->readFile(fileName(ctx->rows_sources_file->path())));

    /// For external cycle
    global_ctx->gathering_column_names_size = global_ctx->gathering_column_names.size();
    ctx->column_num_for_vertical_merge = 0;
    ctx->it_name_and_type = global_ctx->gathering_columns.cbegin();

    return false;
}


void MergeTask::VerticalMergeStage::prepareVerticalMergeForOneColumn() const
{
    const auto & [column_name, column_type] = *ctx->it_name_and_type;
    Names column_names{column_name};

    ctx->progress_before = global_ctx->merge_list_element_ptr->progress.load(std::memory_order_relaxed);

    global_ctx->column_progress = std::make_unique<MergeStageProgress>(ctx->progress_before, ctx->column_sizes->columnWeight(column_name));

    Pipes pipes;
    for (size_t part_num = 0; part_num < global_ctx->future_part->parts.size(); ++part_num)
    {
        auto column_part_source = std::make_shared<MergeTreeSequentialSource>(
            *global_ctx->data, global_ctx->storage_snapshot, global_ctx->future_part->parts[part_num], column_names, ctx->read_with_direct_io, true);

        pipes.emplace_back(std::move(column_part_source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    ctx->rows_sources_read_buf->seek(0, 0);
    auto transform = std::make_unique<ColumnGathererTransform>(pipe.getHeader(), pipe.numOutputPorts(), *ctx->rows_sources_read_buf);
    pipe.addTransform(std::move(transform));

    ctx->column_parts_pipeline = QueryPipeline(std::move(pipe));

    /// Dereference unique_ptr
    ctx->column_parts_pipeline.setProgressCallback(MergeProgressCallback(
        global_ctx->merge_list_element_ptr,
        global_ctx->watch_prev_elapsed,
        *global_ctx->column_progress));

    /// Is calculated inside MergeProgressCallback.
    ctx->column_parts_pipeline.disableProfileEventUpdate();

    ctx->executor = std::make_unique<PullingPipelineExecutor>(ctx->column_parts_pipeline);

    ctx->column_to = std::make_unique<MergedColumnOnlyOutputStream>(
        global_ctx->data_part_storage_builder,
        global_ctx->new_data_part,
        global_ctx->metadata_snapshot,
        ctx->executor->getHeader(),
        ctx->compression_codec,
        /// we don't need to recalc indices here
        /// because all of them were already recalculated and written
        /// as key part of vertical merge
        std::vector<MergeTreeIndexPtr>{},
        &global_ctx->written_offset_columns,
        global_ctx->to->getIndexGranularity());

    ctx->column_elems_written = 0;
}


bool MergeTask::VerticalMergeStage::executeVerticalMergeForOneColumn() const
{
    Block block;
    if (!global_ctx->merges_blocker->isCancelled() && !global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed)
        && ctx->executor->pull(block))
    {
        ctx->column_elems_written += block.rows();
        ctx->column_to->write(block);

        /// Need execute again
        return true;
    }
    return false;
}


void MergeTask::VerticalMergeStage::finalizeVerticalMergeForOneColumn() const
{
    const String & column_name = ctx->it_name_and_type->name;
    if (global_ctx->merges_blocker->isCancelled() || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed))
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    ctx->executor.reset();
    auto changed_checksums = ctx->column_to->fillChecksums(global_ctx->new_data_part, global_ctx->checksums_gathered_columns);
    global_ctx->checksums_gathered_columns.add(std::move(changed_checksums));

    ctx->delayed_streams.emplace_back(std::move(ctx->column_to));

    if (global_ctx->rows_written != ctx->column_elems_written)
    {
        throw Exception("Written " + toString(ctx->column_elems_written) + " elements of column " + column_name +
                        ", but " + toString(global_ctx->rows_written) + " rows of PK columns", ErrorCodes::LOGICAL_ERROR);
    }

    UInt64 rows = 0;
    UInt64 bytes = 0;
    ctx->column_parts_pipeline.tryGetResultRowsAndBytes(rows, bytes);

    /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).

    global_ctx->merge_list_element_ptr->columns_written += 1;
    global_ctx->merge_list_element_ptr->bytes_written_uncompressed += bytes;
    global_ctx->merge_list_element_ptr->progress.store(ctx->progress_before + ctx->column_sizes->columnWeight(column_name), std::memory_order_relaxed);

    /// This is the external cycle increment.
    ++ctx->column_num_for_vertical_merge;
    ++ctx->it_name_and_type;
}


bool MergeTask::VerticalMergeStage::finalizeVerticalMergeForAllColumns() const
{
    for (auto & stream : ctx->delayed_streams)
        stream->finish(ctx->need_sync);

    return false;
}


bool MergeTask::MergeProjectionsStage::mergeMinMaxIndexAndPrepareProjections() const
{
    for (const auto & part : global_ctx->future_part->parts)
    {
        /// Skip empty parts,
        /// (that can be created in StorageReplicatedMergeTree::createEmptyPartInsteadOfLost())
        /// since they can incorrectly set min,
        /// that will be changed after one more merge/OPTIMIZE.
        if (!part->isEmpty())
            global_ctx->new_data_part->minmax_idx->merge(*part->minmax_idx);
    }

    /// Print overall profiling info. NOTE: it may duplicates previous messages
    {
        double elapsed_seconds = global_ctx->merge_list_element_ptr->watch.elapsedSeconds();
        LOG_DEBUG(ctx->log,
            "Merge sorted {} rows, containing {} columns ({} merged, {} gathered) in {} sec., {} rows/sec., {}/sec.",
            global_ctx->merge_list_element_ptr->rows_read,
            global_ctx->all_column_names.size(),
            global_ctx->merging_column_names.size(),
            global_ctx->gathering_column_names.size(),
            elapsed_seconds,
            global_ctx->merge_list_element_ptr->rows_read / elapsed_seconds,
            ReadableSize(global_ctx->merge_list_element_ptr->bytes_read_uncompressed / elapsed_seconds));
    }


    const auto & projections = global_ctx->metadata_snapshot->getProjections();

    for (const auto & projection : projections)
    {
        MergeTreeData::DataPartsVector projection_parts;
        for (const auto & part : global_ctx->future_part->parts)
        {
            auto it = part->getProjectionParts().find(projection.name);
            if (it != part->getProjectionParts().end())
                projection_parts.push_back(it->second);
        }
        if (projection_parts.size() < global_ctx->future_part->parts.size())
        {
            LOG_DEBUG(ctx->log, "Projection {} is not merged because some parts don't have it", projection.name);
            continue;
        }

        LOG_DEBUG(
            ctx->log,
            "Selected {} projection_parts from {} to {}",
            projection_parts.size(),
            projection_parts.front()->name,
            projection_parts.back()->name);

        auto projection_future_part = std::make_shared<FutureMergedMutatedPart>();
        projection_future_part->assign(std::move(projection_parts));
        projection_future_part->name = projection.name;
        // TODO (ab): path in future_part is only for merge process introspection, which is not available for merges of projection parts.
        // Let's comment this out to avoid code inconsistency and add it back after we implement projection merge introspection.
        // projection_future_part->path = global_ctx->future_part->path + "/" + projection.name + ".proj/";
        projection_future_part->part_info = {"all", 0, 0, 0};

        MergeTreeData::MergingParams projection_merging_params;
        projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
        if (projection.type == ProjectionDescription::Type::Aggregate)
            projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

        const Settings & settings = global_ctx->context->getSettingsRef();

        ctx->tasks_for_projections.emplace_back(std::make_shared<MergeTask>(
            projection_future_part,
            projection.metadata,
            global_ctx->merge_entry,
            std::make_unique<MergeListElement>((*global_ctx->merge_entry)->table_id, projection_future_part, settings),
            global_ctx->time_of_merge,
            global_ctx->context,
            global_ctx->space_reservation,
            global_ctx->deduplicate,
            global_ctx->deduplicate_by_columns,
            projection_merging_params,
            global_ctx->new_data_part.get(),
            global_ctx->data_part_storage_builder.get(),
            ".proj",
            NO_TRANSACTION_PTR,
            global_ctx->data,
            global_ctx->mutator,
            global_ctx->merges_blocker,
            global_ctx->ttl_merges_blocker));
    }

    /// We will iterate through projections and execute them
    ctx->projections_iterator = ctx->tasks_for_projections.begin();

    return false;
}


bool MergeTask::MergeProjectionsStage::executeProjections() const
{
    if (ctx->projections_iterator == ctx->tasks_for_projections.end())
        return false;

    if ((*ctx->projections_iterator)->execute())
        return true;

    ++ctx->projections_iterator;
    return true;
}


bool MergeTask::MergeProjectionsStage::finalizeProjectionsAndWholeMerge() const
{
    for (const auto & task : ctx->tasks_for_projections)
    {
        auto part = task->getFuture().get();
        global_ctx->new_data_part->addProjectionPart(part->name, std::move(part));
    }

    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        global_ctx->to->finalizePart(global_ctx->new_data_part, ctx->need_sync);
    else
        global_ctx->to->finalizePart(
            global_ctx->new_data_part, ctx->need_sync, &global_ctx->storage_columns, &global_ctx->checksums_gathered_columns);

    global_ctx->promise.set_value(global_ctx->new_data_part);

    return false;
}


bool MergeTask::VerticalMergeStage::execute()
{
    assert(subtasks_iterator != subtasks.end());
    if ((*subtasks_iterator)())
        return true;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}

bool MergeTask::MergeProjectionsStage::execute()
{
    assert(subtasks_iterator != subtasks.end());
    if ((*subtasks_iterator)())
        return true;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}


bool MergeTask::VerticalMergeStage::executeVerticalMergeForAllColumns() const
{
    /// No need to execute this part if it is horizontal merge.
    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    /// This is the external cycle condition
    if (ctx->column_num_for_vertical_merge >= global_ctx->gathering_column_names_size)
        return false;

    switch (ctx->vertical_merge_one_column_state)
    {
        case VerticalMergeRuntimeContext::State::NEED_PREPARE:
        {
            prepareVerticalMergeForOneColumn();
            ctx->vertical_merge_one_column_state = VerticalMergeRuntimeContext::State::NEED_EXECUTE;
            return true;
        }
        case VerticalMergeRuntimeContext::State::NEED_EXECUTE:
        {
            if (executeVerticalMergeForOneColumn())
                return true;

            ctx->vertical_merge_one_column_state = VerticalMergeRuntimeContext::State::NEED_FINISH;
            return true;
        }
        case VerticalMergeRuntimeContext::State::NEED_FINISH:
        {
            finalizeVerticalMergeForOneColumn();
            ctx->vertical_merge_one_column_state = VerticalMergeRuntimeContext::State::NEED_PREPARE;
            return true;
        }
    }
    return false;
}


bool MergeTask::execute()
{
    assert(stages_iterator != stages.end());
    if ((*stages_iterator)->execute())
        return true;

    /// Stage is finished, need initialize context for the next stage
    auto next_stage_context = (*stages_iterator)->getContextForNextStage();

    /// Move to the next stage in an array of stages
    ++stages_iterator;
    if (stages_iterator == stages.end())
        return false;

    (*stages_iterator)->setRuntimeContext(std::move(next_stage_context), global_ctx);
    return true;
}


void MergeTask::ExecuteAndFinalizeHorizontalPart::createMergedStream()
{
    /** Read from all parts, merge and write into a new one.
      * In passing, we calculate expression for sorting.
      */
    Pipes pipes;
    global_ctx->watch_prev_elapsed = 0;

    /// We count total amount of bytes in parts
    /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
    ctx->read_with_direct_io = false;
    const auto data_settings = global_ctx->data->getSettings();
    if (data_settings->min_merge_bytes_to_use_direct_io != 0)
    {
        size_t total_size = 0;
        for (const auto & part : global_ctx->future_part->parts)
        {
            total_size += part->getBytesOnDisk();
            if (total_size >= data_settings->min_merge_bytes_to_use_direct_io)
            {
                LOG_DEBUG(ctx->log, "Will merge parts reading files in O_DIRECT");
                ctx->read_with_direct_io = true;

                break;
            }
        }
    }

    /// Using unique_ptr, because MergeStageProgress has no default constructor
    global_ctx->horizontal_stage_progress = std::make_unique<MergeStageProgress>(
        ctx->column_sizes ? ctx->column_sizes->keyColumnsWeight() : 1.0);


    for (const auto & part : global_ctx->future_part->parts)
    {
        auto columns = global_ctx->merging_column_names;

        /// The part might have some rows masked by lightweight deletes
        const auto lightweight_delete_filter_column = LightweightDeleteDescription::FILTER_COLUMN.name;
        const bool need_to_filter_deleted_rows = part->hasLightweightDelete();
        if (need_to_filter_deleted_rows)
            columns.emplace_back(lightweight_delete_filter_column);

        auto input = std::make_unique<MergeTreeSequentialSource>(
            *global_ctx->data, global_ctx->storage_snapshot, part, columns, ctx->read_with_direct_io, true);

        Pipe pipe(std::move(input));

        /// Add filtering step that discards deleted rows
        if (need_to_filter_deleted_rows)
        {
            pipe.addSimpleTransform([lightweight_delete_filter_column](const Block & header)
            {
                return std::make_shared<FilterTransform>(header, nullptr, lightweight_delete_filter_column, true);
            });
        }

        if (global_ctx->metadata_snapshot->hasSortingKey())
        {
            pipe.addSimpleTransform([this](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, global_ctx->metadata_snapshot->getSortingKey().expression);
            });
        }

        pipes.emplace_back(std::move(pipe));
    }


    Names sort_columns = global_ctx->metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    sort_description.compile_sort_description = global_ctx->data->getContext()->getSettingsRef().compile_sort_description;
    sort_description.min_count_to_compile_sort_description = global_ctx->data->getContext()->getSettingsRef().min_count_to_compile_sort_description;

    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    Names partition_key_columns = global_ctx->metadata_snapshot->getPartitionKey().column_names;

    Block header = pipes.at(0).getHeader();
    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(sort_columns[i], 1, 1);

    /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
    ///  that is going in insertion order.
    ProcessorPtr merged_transform;

    /// If merge is vertical we cannot calculate it
    ctx->blocks_are_granules_size = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Vertical);

    UInt64 merge_block_size = data_settings->merge_max_block_size;

    switch (ctx->merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            merged_transform = std::make_shared<MergingSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size, SortingQueueStrategy::Default, 0, ctx->rows_sources_write_buf.get(), true, ctx->blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_transform = std::make_shared<CollapsingSortedTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.sign_column, false,
                merge_block_size, ctx->rows_sources_write_buf.get(), ctx->blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Summing:
            merged_transform = std::make_shared<SummingSortedTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.columns_to_sum, partition_key_columns, merge_block_size);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged_transform = std::make_shared<AggregatingSortedTransform>(header, pipes.size(), sort_description, merge_block_size);
            break;

        case MergeTreeData::MergingParams::Replacing:
            merged_transform = std::make_shared<ReplacingSortedTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.version_column,
                merge_block_size, ctx->rows_sources_write_buf.get(), ctx->blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_transform = std::make_shared<GraphiteRollupSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size,
                ctx->merging_params.graphite_params, global_ctx->time_of_merge);
            break;

        case MergeTreeData::MergingParams::VersionedCollapsing:
            merged_transform = std::make_shared<VersionedCollapsingTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.sign_column,
                merge_block_size, ctx->rows_sources_write_buf.get(), ctx->blocks_are_granules_size);
            break;
    }

    auto res_pipe = Pipe::unitePipes(std::move(pipes));
    res_pipe.addTransform(std::move(merged_transform));

    if (global_ctx->deduplicate)
    {
        if (DistinctSortedTransform::isApplicable(header, sort_description, global_ctx->deduplicate_by_columns))
            res_pipe.addTransform(std::make_shared<DistinctSortedTransform>(
                res_pipe.getHeader(), sort_description, SizeLimits(), 0 /*limit_hint*/, global_ctx->deduplicate_by_columns));
        else
            res_pipe.addTransform(std::make_shared<DistinctTransform>(
                res_pipe.getHeader(), SizeLimits(), 0 /*limit_hint*/, global_ctx->deduplicate_by_columns));
    }

    if (ctx->need_remove_expired_values)
        res_pipe.addTransform(std::make_shared<TTLTransform>(
            res_pipe.getHeader(), *global_ctx->data, global_ctx->metadata_snapshot, global_ctx->new_data_part, global_ctx->time_of_merge, ctx->force_ttl));

    if (global_ctx->metadata_snapshot->hasSecondaryIndices())
    {
        const auto & indices = global_ctx->metadata_snapshot->getSecondaryIndices();
        res_pipe.addTransform(std::make_shared<ExpressionTransform>(
            res_pipe.getHeader(), indices.getSingleExpressionForIndices(global_ctx->metadata_snapshot->getColumns(), global_ctx->data->getContext())));
        res_pipe.addTransform(std::make_shared<MaterializingTransform>(res_pipe.getHeader()));
    }

    global_ctx->merged_pipeline = QueryPipeline(std::move(res_pipe));
    /// Dereference unique_ptr and pass horizontal_stage_progress by reference
    global_ctx->merged_pipeline.setProgressCallback(MergeProgressCallback(global_ctx->merge_list_element_ptr, global_ctx->watch_prev_elapsed, *global_ctx->horizontal_stage_progress));
    /// Is calculated inside MergeProgressCallback.
    global_ctx->merged_pipeline.disableProfileEventUpdate();

    global_ctx->merging_executor = std::make_unique<PullingPipelineExecutor>(global_ctx->merged_pipeline);
}


MergeAlgorithm MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm() const
{
    const size_t sum_rows_upper_bound = global_ctx->merge_list_element_ptr->total_rows_count;
    const auto data_settings = global_ctx->data->getSettings();

    if (global_ctx->deduplicate)
        return MergeAlgorithm::Horizontal;
    if (data_settings->enable_vertical_merge_algorithm == 0)
        return MergeAlgorithm::Horizontal;
    if (ctx->need_remove_expired_values)
        return MergeAlgorithm::Horizontal;

    for (const auto & part : global_ctx->future_part->parts)
        if (!part->supportsVerticalMerge())
            return MergeAlgorithm::Horizontal;

    bool is_supported_storage =
        ctx->merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        ctx->merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
        ctx->merging_params.mode == MergeTreeData::MergingParams::Replacing ||
        ctx->merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

    bool enough_ordinary_cols = global_ctx->gathering_columns.size() >= data_settings->vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = sum_rows_upper_bound >= data_settings->vertical_merge_algorithm_min_rows_to_activate;

    bool no_parts_overflow = global_ctx->future_part->parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_ordinary_cols && no_parts_overflow) ?
                        MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    return merge_alg;
}

}
