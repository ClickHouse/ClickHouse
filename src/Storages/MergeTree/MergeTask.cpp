#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MergedPartOffsets.h>

#include <memory>
#include <fmt/format.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadBufferFromEmptyFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/createSubcolumnsExtractionActions.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CoalescingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtractColumnsStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/TemporaryFiles.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/TTLTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeProjectionPartsTask.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/TextIndexUtils.h>
#include <fmt/ranges.h>
#include <Common/DimensionalMetrics.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include "config.h"

#ifndef NDEBUG
    #include <Processors/Transforms/CheckSortedTransform.h>
#endif

#if CLICKHOUSE_CLOUD
    #include <Interpreters/Cache/FileCacheFactory.h>
    #include <Disks/DiskObjectStorage/DiskObjectStorage.h>
    #include <Storages/MergeTree/DataPartStorageOnDiskPacked.h>
    #include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#endif


namespace ProfileEvents
{
    extern const Event Merge;
    extern const Event MergeSourceParts;
    extern const Event MergeWrittenRows;
    extern const Event MergedColumns;
    extern const Event GatheredColumns;
    extern const Event MergeTotalMilliseconds;
    extern const Event MergeExecuteMilliseconds;
    extern const Event MergeHorizontalStageExecuteMilliseconds;
    extern const Event MergeVerticalStageExecuteMilliseconds;
    extern const Event MergeTextIndexStageExecuteMilliseconds;
    extern const Event MergeProjectionStageExecuteMilliseconds;
    extern const Event MergeTreeDataWriterStatisticsCalculationMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForMerge;
}

namespace DimensionalMetrics
{
    extern MetricFamily & MergeFailures;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool compile_sort_description;
    extern const SettingsUInt64 max_insert_delayed_streams_for_parallel_write;
    extern const SettingsUInt64 min_count_to_compile_sort_description;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_rows;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_experimental_replacing_merge_with_cleanup;
    extern const MergeTreeSettingsBool allow_vertical_merges_from_compact_to_wide_parts;
    extern const MergeTreeSettingsMilliseconds background_task_preferred_step_execution_time_ms;
    extern const MergeTreeSettingsDeduplicateMergeProjectionMode deduplicate_merge_projection_mode;
    extern const MergeTreeSettingsBool enable_block_number_column;
    extern const MergeTreeSettingsBool enable_block_offset_column;
    extern const MergeTreeSettingsUInt64 enable_vertical_merge_algorithm;
    extern const MergeTreeSettingsUInt64 merge_max_block_size_bytes;
    extern const MergeTreeSettingsNonZeroUInt64 merge_max_block_size;
    extern const MergeTreeSettingsUInt64 min_merge_bytes_to_use_direct_io;
    extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
    extern const MergeTreeSettingsUInt64 vertical_merge_algorithm_min_bytes_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_merge_algorithm_min_columns_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_merge_algorithm_min_rows_to_activate;
    extern const MergeTreeSettingsBool vertical_merge_remote_filesystem_prefetch;
    extern const MergeTreeSettingsBool materialize_skip_indexes_on_merge;
    extern const MergeTreeSettingsString exclude_materialize_skip_indexes_on_merge;
    extern const MergeTreeSettingsBool prewarm_mark_cache;
    extern const MergeTreeSettingsBool use_const_adaptive_granularity;
    extern const MergeTreeSettingsUInt64 max_merge_delayed_streams_for_parallel_write;
    extern const MergeTreeSettingsBool ttl_only_drop_parts;
    extern const MergeTreeSettingsBool vertical_merge_optimize_lightweight_delete;
    extern const MergeTreeSettingsUInt64Auto merge_max_dynamic_subcolumns_in_wide_part;
    extern const MergeTreeSettingsUInt64Auto merge_max_dynamic_subcolumns_in_compact_part;
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
    extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
    extern const MergeTreeSettingsMergeTreeNullableSerializationVersion nullable_serialization_version;
    extern const MergeTreeSettingsBool materialize_statistics_on_merge;
}

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

/// Transform that builds statistics for columns and doesn't change the chunk.
class BuildStatisticsTransform : public ISimpleTransform
{
public:
    BuildStatisticsTransform(
        SharedHeader header,
        ColumnsStatistics statistics_to_build_)
        : ISimpleTransform(header, header, false)
        , statistics_to_build(std::move(statistics_to_build_))
    {
    }

    String getName() const override { return "BuildStatisticsTransform"; }

    void transform(Chunk & chunk) override
    {
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterStatisticsCalculationMicroseconds);
        statistics_to_build.buildIfExists(block);
    }

    const ColumnsStatistics & getStatistics() const { return statistics_to_build; }

private:
    ColumnsStatistics statistics_to_build;
};

class BuildStatisticsStep : public ITransformingStep
{
public:
    BuildStatisticsStep(SharedHeader input_header_, std::shared_ptr<BuildStatisticsTransform> transform_)
        : ITransformingStep(input_header_, input_header_, getTraits())
        , transform(std::move(transform_))
    {
    }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addTransform(transform);
    }

    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    String getName() const override { return "BuildStatistics"; }

private:
    static Traits getTraits()
    {
        return Traits
        {
            {
                .returns_single_stream = true,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = true,
            }
        };
    }

    std::shared_ptr<BuildStatisticsTransform> transform;
};

/// Manages the "rows_sources" temporary file that is used during vertical merge.
class RowsSourcesTemporaryFile : public ITemporaryFileLookup
{
public:
    /// A logical name of the temporary file under which it will be known to the plan steps that use it.
    static constexpr auto FILE_ID = "rows_sources";

    explicit RowsSourcesTemporaryFile(TemporaryDataOnDiskScopePtr temporary_data_on_disk_)
        : temporary_data_on_disk(temporary_data_on_disk_->childScope({CurrentMetrics::TemporaryFilesForMerge}))
    {
    }

    WriteBuffer & getTemporaryFileForWriting(const String & name) override
    {
        if (name != FILE_ID)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected temporary file name requested: {}", name);

        if (tmp_data_buffer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file was already requested for writing, there musto be only one writer");

        tmp_data_buffer = std::make_unique<TemporaryDataBuffer>(temporary_data_on_disk);
        return *tmp_data_buffer;
    }

    std::unique_ptr<ReadBuffer> getTemporaryFileForReading(const String & name) override
    {
        if (name != FILE_ID)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected temporary file name requested: {}", name);

        if (!finalized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file is not finalized yet");

        /// tmp_disk might not create real file if no data was written to it.
        if (final_size == 0)
            return std::make_unique<ReadBufferFromEmptyFile>();

        /// Reopen the file for each read so that multiple reads can be performed in parallel and there is no need to seek to the beginning.
        return tmp_data_buffer->read();
    }

    /// Returns written data size in bytes
    size_t finalizeWriting()
    {
        if (!tmp_data_buffer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file was not requested for writing");

        auto stat = tmp_data_buffer->finishWriting();
        finalized = true;
        final_size = stat.uncompressed_size;
        return final_size;
    }

private:
    std::unique_ptr<TemporaryDataBuffer> tmp_data_buffer;
    TemporaryDataOnDiskScopePtr temporary_data_on_disk;
    bool finalized = false;
    size_t final_size = 0;
};

static void addMissedColumnsToSerializationInfos(
    size_t num_rows_in_parts,
    const Names & part_columns,
    const ColumnsDescription & storage_columns,
    const SerializationInfo::Settings & info_settings,
    SerializationInfoByName & new_infos)
{
    NameSet part_columns_set(part_columns.begin(), part_columns.end());

    for (const auto & column : storage_columns)
    {
        if (part_columns_set.contains(column.name))
            continue;

        if (column.default_desc.kind != ColumnDefaultKind::Default)
            continue;

        if (column.default_desc.expression)
            continue;

        auto new_info = column.type->createSerializationInfo(info_settings);
        new_info->addDefaults(num_rows_in_parts);
        new_infos.emplace(column.name, std::move(new_info));
    }
}

bool MergeTask::GlobalRuntimeContext::isCancelled() const
{
    return (future_part ? merges_blocker->isCancelledForPartition(future_part->part_info.getPartitionId()) : merges_blocker->isCancelled())
        || merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed);
}

void MergeTask::GlobalRuntimeContext::checkOperationIsNotCanceled() const
{
    if (isCancelled())
    {
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");
    }
}

static String getColumnNameInStorage(const String & column_name, const NameSet & storage_columns)
{
    if (storage_columns.contains(column_name))
        return column_name;

    /// If we don't have this column in storage columns, it must be a subcolumn of one of the storage columns.
    return String(Nested::getColumnFromSubcolumn(column_name, storage_columns));
}

/// PK columns are sorted and merged, ordinary columns are gathered using info from merge step
void MergeTask::ExecuteAndFinalizeHorizontalPart::extractMergingAndGatheringColumns(const std::unordered_set<String> & exclude_index_names) const
{
    const auto & sorting_key_expr = global_ctx->metadata_snapshot->getSortingKey().expression;
    Names sort_key_columns_vec = sorting_key_expr->getRequiredColumns();

    /// Collect columns used in the sorting key expressions.
    NameSet key_columns;
    auto storage_columns = global_ctx->storage_columns.getNameSet();
    for (const auto & name : sort_key_columns_vec)
        key_columns.insert(getColumnNameInStorage(name, storage_columns));

    /// Force sign column for Collapsing mode and VersionedCollapsing mode
    if (!global_ctx->merging_params.sign_column.empty())
        key_columns.emplace(global_ctx->merging_params.sign_column);

    /// Force is_deleted column for Replacing mode
    if (!global_ctx->merging_params.is_deleted_column.empty())
        key_columns.emplace(global_ctx->merging_params.is_deleted_column);

    /// Force version column for Replacing mode and VersionedCollapsing mode
    if (!global_ctx->merging_params.version_column.empty())
        key_columns.emplace(global_ctx->merging_params.version_column);

    /// Force all columns params of Graphite mode
    if (global_ctx->merging_params.mode == MergeTreeData::MergingParams::Graphite)
    {
        key_columns.emplace(global_ctx->merging_params.graphite_params.path_column_name);
        key_columns.emplace(global_ctx->merging_params.graphite_params.time_column_name);
        key_columns.emplace(global_ctx->merging_params.graphite_params.value_column_name);
        key_columns.emplace(global_ctx->merging_params.graphite_params.version_column_name);
    }

    /// Force to merge at least one column in case of empty key
    if (key_columns.empty())
        key_columns.emplace(global_ctx->storage_columns.front().name);

    /// Recalculate the min-max index for partition columns if the merge might reduce rows.
    if (global_ctx->merge_may_reduce_rows)
    {
        auto minmax_columns = MergeTreeData::getMinMaxColumnsNames(global_ctx->metadata_snapshot->getPartitionKey());
        key_columns.insert(minmax_columns.begin(), minmax_columns.end());
    }

    key_columns.insert(global_ctx->deduplicate_by_columns.begin(), global_ctx->deduplicate_by_columns.end());

    /// Key columns required for merge, must not be expired early.
    global_ctx->merge_required_key_columns = key_columns;
    const auto & skip_indexes = global_ctx->metadata_snapshot->getSecondaryIndices();

    for (const auto & index : skip_indexes)
    {
        if (exclude_index_names.contains(index.name)) /// user requested to skip this index during merge
            continue;

        auto index_columns = index.expression->getRequiredColumns();

        /// Calculate indexes that depend only on one column on vertical
        /// stage and other indexes on horizonatal stage of merge.
        if (index.type == "text")
        {
            global_ctx->text_indexes_to_merge.push_back(index);

            if (index_columns.size() > 1)
            {
                for (const auto & index_column : index_columns)
                    key_columns.insert(getColumnNameInStorage(index_column, storage_columns));
            }
        }
        else if (index_columns.size() == 1)
        {
            auto column_name = getColumnNameInStorage(index_columns.front(), storage_columns);
            global_ctx->skip_indexes_by_column[column_name].push_back(index);
        }
        else
        {
            global_ctx->merging_skip_indexes.push_back(index);

            for (const auto & index_column : index_columns)
                key_columns.insert(getColumnNameInStorage(index_column, storage_columns));
        }
    }

    for (const auto * projection : global_ctx->projections_to_rebuild)
    {
        for (const auto & column : projection->getRequiredColumns())
        {
            if (projection->with_parent_part_offset && column == "_part_offset")
                continue;

            key_columns.insert(getColumnNameInStorage(column, storage_columns));
        }
    }

    /// TODO: also force "summing" and "aggregating" columns to make Horizontal merge only for such columns

    for (const auto & column : global_ctx->storage_columns)
    {
        if (key_columns.contains(column.name))
        {
            global_ctx->merging_columns.emplace_back(column);

            /// If column is in horizontal stage we need to calculate its indexes on horizontal stage as well
            auto it = global_ctx->skip_indexes_by_column.find(column.name);
            if (it != global_ctx->skip_indexes_by_column.end())
            {
                for (auto & index : it->second)
                    global_ctx->merging_skip_indexes.push_back(std::move(index));

                global_ctx->skip_indexes_by_column.erase(it);
            }
        }
        else
        {
            global_ctx->gathering_columns.emplace_back(column);
        }
    }
}

bool MergeTask::ExecuteAndFinalizeHorizontalPart::prepare() const
{
    ProfileEvents::increment(ProfileEvents::Merge);
    ProfileEvents::increment(ProfileEvents::MergeSourceParts, global_ctx->future_part->parts.size());

    String local_tmp_prefix;
    if (global_ctx->need_prefix)
    {
        // projection parts have different prefix and suffix compared to normal parts.
        // E.g. `proj_a.proj` for a normal projection merge and `proj_a.tmp_proj` for a projection materialization merge.
        local_tmp_prefix = global_ctx->parent_part ? "" : TEMP_DIRECTORY_PREFIX;
    }

    const String local_tmp_suffix = global_ctx->parent_part ? global_ctx->suffix : "";

    global_ctx->checkOperationIsNotCanceled();

    /// We don't want to perform merge assigned with TTL as normal merge, so
    /// throw exception
    if (isTTLMergeType(global_ctx->future_part->merge_type) && global_ctx->ttl_merges_blocker->isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts with TTL");

    LOG_DEBUG(ctx->log, "Merging {} parts: from {} to {} into {} with storage {}",
        global_ctx->future_part->parts.size(),
        global_ctx->future_part->parts.front()->name,
        global_ctx->future_part->parts.back()->name,
        global_ctx->future_part->part_format.part_type.toString(),
        global_ctx->future_part->part_format.storage_type.toString());

    if (global_ctx->deduplicate)
    {
        if (global_ctx->deduplicate_by_columns.empty())
            LOG_DEBUG(ctx->log, "DEDUPLICATE BY all columns");
        else
            LOG_DEBUG(ctx->log, "DEDUPLICATE BY ('{}')", fmt::join(global_ctx->deduplicate_by_columns, "', '"));
    }

    global_ctx->disk = global_ctx->space_reservation->getDisk();
    auto local_tmp_part_basename = local_tmp_prefix + global_ctx->future_part->name + local_tmp_suffix;

    std::optional<MergeTreeDataPartBuilder> builder;
    if (global_ctx->parent_part)
    {
        auto data_part_storage = global_ctx->parent_part->getDataPartStorage().getProjection(local_tmp_part_basename,  /* use parent transaction */ false);
        builder.emplace(*global_ctx->data, global_ctx->future_part->name, data_part_storage, getReadSettings());
        builder->withParentPart(global_ctx->parent_part);
    }
    else
    {
        auto local_single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + global_ctx->future_part->name, global_ctx->disk, 0);
        builder.emplace(global_ctx->data->getDataPartBuilder(global_ctx->future_part->name, local_single_disk_volume, local_tmp_part_basename, getReadSettings()));
        builder->withPartStorageType(global_ctx->future_part->part_format.storage_type);
    }

    builder->withPartInfo(global_ctx->future_part->part_info);
    builder->withPartType(global_ctx->future_part->part_format.part_type);

    global_ctx->new_data_part = std::move(*builder).build();
    auto data_part_storage = global_ctx->new_data_part->getDataPartStoragePtr();

    if (data_part_storage->exists())
        throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Directory {} already exists", data_part_storage->getFullPath());

    data_part_storage->beginTransaction();

    /// Background temp dirs cleaner will not touch tmp projection directory because
    /// it's located inside part's directory
    if (!global_ctx->parent_part)
        global_ctx->temporary_directory_lock = global_ctx->data->getTemporaryPartDirectoryHolder(local_tmp_part_basename);

    global_ctx->storage_columns = global_ctx->metadata_snapshot->getColumns().getAllPhysical();
    global_ctx->storage_snapshot = std::make_shared<StorageSnapshot>(*global_ctx->data, global_ctx->metadata_snapshot);

    ctx->need_remove_expired_values = false;
    ctx->force_ttl = false;
    for (const auto & part : global_ctx->future_part->parts)
    {
        global_ctx->new_data_part->ttl_infos.update(part->ttl_infos);

        if (global_ctx->metadata_snapshot->hasAnyTTL() && !part->checkAllTTLCalculated(global_ctx->metadata_snapshot))
        {
            LOG_INFO(ctx->log, "Some TTL values were not calculated for part {}. Will calculate them forcefully during merge.", part->name);
            ctx->need_remove_expired_values = true;
            ctx->force_ttl = true;
        }
    }

    const auto & local_part_min_ttl = global_ctx->new_data_part->ttl_infos.part_min_ttl;
    if (global_ctx->metadata_snapshot->hasAnyTTL() && local_part_min_ttl && local_part_min_ttl <= global_ctx->time_of_merge)
        ctx->need_remove_expired_values = true;

    if (ctx->need_remove_expired_values && global_ctx->ttl_merges_blocker->isCancelled())
    {
        LOG_INFO(ctx->log, "Part {} has values with expired TTL, but merges with TTL are cancelled.", global_ctx->new_data_part->name);
        ctx->need_remove_expired_values = false;
    }

    const auto & patch_parts = global_ctx->future_part->patch_parts;

    /// Determine columns that are absent in all source parts—either fully expired or never written—and mark them as
    /// expired to avoid unnecessary reads or writes during merges.
    ///
    /// NOTE:
    /// Handling missing columns that have default expressions is non-trivial and currently unresolved
    /// (see https://github.com/ClickHouse/ClickHouse/issues/91127).
    /// For now, we conservatively avoid expiring such columns.
    ///
    /// The main challenges include:
    /// 1. A default expression may depend on other columns, which themselves may be missing or expired,
    ///    making it unclear whether the default should be materialized or recomputed.
    /// 2. Default expressions may introduce semantic changes if re-evaluated during merges, leading to
    ///    non-deterministic results across parts.
    {
        NameSet columns_present_in_parts;
        columns_present_in_parts.reserve(global_ctx->storage_columns.size());

        /// Collect all column names that actually exist in the source parts
        for (const auto & part : global_ctx->future_part->parts)
        {
            for (const auto & col : part->getColumns())
                columns_present_in_parts.emplace(col.name);
        }

        const auto & columns_desc = global_ctx->metadata_snapshot->getColumns();

        /// Any storage column not present in any part and without a default expression is considered expired
        for (const auto & storage_column : global_ctx->storage_columns)
        {
            if (!columns_present_in_parts.contains(storage_column.name) && !columns_desc.getDefault(storage_column.name))
                global_ctx->new_data_part->expired_columns.emplace(storage_column.name);
        }
    }

    /// Determine whether projections and minmax indexes need to be updated during merge,
    /// which typically happens when the number of rows may be reduced.
    ///
    /// This is necessary in cases such as TTL expiration, cleanup merges, deduplication,
    /// or special merge modes like Collapsing/Replacing.
    global_ctx->merge_may_reduce_rows =
        ctx->need_remove_expired_values ||
        !patch_parts.empty() ||
        global_ctx->cleanup ||
        global_ctx->deduplicate ||
        hasLightweightDelete(global_ctx->future_part) ||
        global_ctx->merging_params.mode != MergeTreeData::MergingParams::Ordinary;

    prepareProjectionsToMergeAndRebuild();

    const auto & merge_tree_settings = global_ctx->data_settings;

    /// Get list of skip indexes to exclude from merge
    std::unordered_set<String> exclude_index_names;
    if ((*merge_tree_settings)[MergeTreeSetting::materialize_skip_indexes_on_merge])
    {
        auto exclude_indexes_string = (*merge_tree_settings)[MergeTreeSetting::exclude_materialize_skip_indexes_on_merge].toString();
        if (!exclude_indexes_string.empty())
            exclude_index_names = parseIdentifiersOrStringLiteralsToSet(exclude_indexes_string, global_ctx->context->getSettingsRef());
    }

    extractMergingAndGatheringColumns(exclude_index_names);

    const auto & expired_columns = global_ctx->new_data_part->expired_columns;
    if (!expired_columns.empty())
    {
        global_ctx->gathering_columns = global_ctx->gathering_columns.eraseNames(expired_columns);

        auto filter_columns = [&](const NamesAndTypesList & input, NamesAndTypesList & expired_out)
        {
            NamesAndTypesList result;
            for (const auto & column : input)
            {
                bool is_expired = expired_columns.contains(column.name);
                bool is_required_for_merge = global_ctx->merge_required_key_columns.contains(column.name);

                if (is_expired)
                    expired_out.push_back(column);

                if (!is_expired || is_required_for_merge)
                    result.push_back(column);
            }

            return result;
        };

        global_ctx->merging_columns = filter_columns(global_ctx->merging_columns, global_ctx->merging_columns_expired_by_ttl);
        global_ctx->storage_columns = filter_columns(global_ctx->storage_columns, global_ctx->storage_columns_expired_by_ttl);
    }

    global_ctx->new_data_part->uuid = global_ctx->future_part->uuid;
    global_ctx->new_data_part->partition.assign(global_ctx->future_part->getPartition());
    global_ctx->new_data_part->is_temp = global_ctx->parent_part == nullptr;

    /// In case of replicated merge tree with zero copy replication
    /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
    /// The blobs have to be removed along with the part, this temporary part owns them and does not share them yet.
    global_ctx->new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;

    if (enabledBlockNumberColumn(global_ctx))
        addGatheringColumn(global_ctx, BlockNumberColumn::name, BlockNumberColumn::type);

    if (enabledBlockOffsetColumn(global_ctx))
        addGatheringColumn(global_ctx, BlockOffsetColumn::name, BlockOffsetColumn::type);

    MergeTreeData::IMutationsSnapshot::Params params
    {
        .metadata_version = global_ctx->metadata_snapshot->getMetadataVersion(),
        .min_part_metadata_version = MergeTreeData::getMinMetadataVersion(global_ctx->future_part->parts),
        .min_part_data_versions = nullptr,
        .max_mutation_versions = nullptr,
        .need_data_mutations = false,
        .need_alter_mutations = !patch_parts.empty(),
        .need_patch_parts = false,
    };

    auto mutations_snapshot = global_ctx->data->getMutationsSnapshot(params);

    if (!patch_parts.empty())
    {
        LOG_DEBUG(ctx->log, "Will apply {} patches up to version {}", patch_parts.size(), global_ctx->future_part->part_info.getMutationVersion());

        for (const auto & patch : patch_parts)
            LOG_TRACE(ctx->log, "Applying patch part {} with max data version {}", patch->name, patch->getSourcePartsSet().getMaxDataVersion());

        auto & mutable_snapshot = const_cast<MergeTreeData::IMutationsSnapshot &>(*mutations_snapshot);
        mutable_snapshot.addPatches(global_ctx->future_part->patch_parts);
    }

    if ((*merge_tree_settings)[MergeTreeSetting::materialize_statistics_on_merge])
    {
        global_ctx->gathered_data.statistics = ColumnsStatistics(global_ctx->metadata_snapshot->getColumns());
    }

    if (global_ctx->merge_may_reduce_rows)
    {
        /// If merge may reduce rows, we need to build statistics for the new part
        /// at the end of the merge pipeline. See usage of addBuildStatisticsStep.
        global_ctx->statistics_to_build_by_part[global_ctx->new_data_part->name] = global_ctx->gathered_data.statistics.cloneEmpty();
    }
    else
    {
        for (const auto & part : global_ctx->future_part->parts)
        {
            /// Skip empty parts,
            /// (that can be created in StorageReplicatedMergeTree::createEmptyPartInsteadOfLost())
            /// since they can incorrectly set min,
            /// that will be changed after one more merge/OPTIMIZE.
            if (part->isEmpty())
                continue;

            global_ctx->new_data_part->minmax_idx->merge(*part->minmax_idx);
            const auto & result_statistics = global_ctx->gathered_data.statistics;

            if (result_statistics.empty())
                continue;

            auto part_statistics = part->loadStatistics();

            for (const auto & [column_name, column_stats] : result_statistics)
            {
                auto it = part_statistics.find(column_name);

                if (it == part_statistics.end() || !column_stats->structureEquals(*it->second))
                    global_ctx->statistics_to_build_by_part[part->name].emplace(column_name, column_stats->cloneEmpty());
                else
                    column_stats->merge(it->second);
            }
        }
    }

    SerializationInfo::Settings info_settings
    {
        (*merge_tree_settings)[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
        true,
        (*merge_tree_settings)[MergeTreeSetting::serialization_info_version],
        (*merge_tree_settings)[MergeTreeSetting::string_serialization_version],
        (*merge_tree_settings)[MergeTreeSetting::nullable_serialization_version],
    };

    SerializationInfoByName infos(global_ctx->storage_columns, info_settings);
    global_ctx->alter_conversions.reserve(global_ctx->future_part->parts.size());

    for (const auto & part : global_ctx->future_part->parts)
    {
        if (!info_settings.isAlwaysDefault())
        {
            auto part_infos = part->getSerializationInfos();

            addMissedColumnsToSerializationInfos(
                part->rows_count,
                part->getColumns().getNames(),
                global_ctx->metadata_snapshot->getColumns(),
                info_settings,
                part_infos);

            infos.add(part_infos);
        }

        global_ctx->alter_conversions.push_back(MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, global_ctx->context));
    }

    if (global_ctx->new_data_part->info.isPatch())
    {
        auto set = SourcePartsSetForPatch::merge(global_ctx->future_part->parts);
        global_ctx->new_data_part->setSourcePartsSet(std::move(set));
    }

    global_ctx->new_data_part->setColumns(global_ctx->storage_columns, infos, global_ctx->metadata_snapshot->getMetadataVersion());

    ctx->sum_input_rows_upper_bound = global_ctx->merge_list_element_ptr->total_rows_count;
    ctx->sum_compressed_bytes_upper_bound = global_ctx->merge_list_element_ptr->total_size_bytes_compressed;
    ctx->sum_uncompressed_bytes_upper_bound = global_ctx->merge_list_element_ptr->total_size_bytes_uncompressed;

    global_ctx->chosen_merge_algorithm = chooseMergeAlgorithm();
    global_ctx->merge_list_element_ptr->merge_algorithm.store(global_ctx->chosen_merge_algorithm, std::memory_order_relaxed);

    LOG_DEBUG(ctx->log, "Selected MergeAlgorithm: {}", toString(global_ctx->chosen_merge_algorithm));

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes())
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    global_ctx->compression_codec = global_ctx->data->getCompressionCodecForPart(
        global_ctx->merge_list_element_ptr->total_size_bytes_compressed,
        global_ctx->new_data_part->ttl_infos,
        global_ctx->time_of_merge);

    switch (global_ctx->chosen_merge_algorithm)
    {
        case MergeAlgorithm::Horizontal:
        {
            global_ctx->merging_columns = global_ctx->storage_columns;
            global_ctx->merging_columns_expired_by_ttl = global_ctx->storage_columns_expired_by_ttl;

            global_ctx->gathering_columns.clear();
            global_ctx->merging_skip_indexes.clear();
            global_ctx->skip_indexes_by_column.clear();
            global_ctx->text_indexes_to_merge.clear();

            auto all_skip_indexes = global_ctx->metadata_snapshot->getSecondaryIndices();

            for (const auto & index : all_skip_indexes)
            {
                if (!exclude_index_names.contains(index.name))
                {
                    if (index.type == "text")
                        global_ctx->text_indexes_to_merge.push_back(index);
                    else
                        global_ctx->merging_skip_indexes.push_back(index);
                }
            }
            break;
        }
        case MergeAlgorithm::Vertical:
        {
            ctx->rows_sources_temporary_file = std::make_shared<RowsSourcesTemporaryFile>(global_ctx->context->getTempDataOnDisk());

            std::map<String, UInt64> local_merged_column_to_size;
            for (const auto & part : global_ctx->future_part->parts)
                part->accumulateColumnSizes(local_merged_column_to_size);

            ctx->column_sizes = ColumnSizeEstimator(
                std::move(local_merged_column_to_size),
                global_ctx->merging_columns,
                global_ctx->gathering_columns);

            break;
        }
        default :
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge algorithm must be chosen");
    }

    /// If setting 'materialize_skip_indexes_on_merge' is false, forget about skip indexes.
    if (!(*merge_tree_settings)[MergeTreeSetting::materialize_skip_indexes_on_merge])
    {
        global_ctx->merging_skip_indexes.clear();
        global_ctx->skip_indexes_by_column.clear();
    }

    bool use_adaptive_granularity = global_ctx->new_data_part->index_granularity_info.mark_type.adaptive;
    bool use_const_adaptive_granularity = (*merge_tree_settings)[MergeTreeSetting::use_const_adaptive_granularity];

    /// If merge is vertical we cannot calculate it.
    /// If granularity is constant we don't need to calculate it.
    ctx->blocks_are_granules_size = use_adaptive_granularity
        && !use_const_adaptive_granularity
        && global_ctx->chosen_merge_algorithm == MergeAlgorithm::Vertical;

    /// Merged stream will be created and available as merged_stream variable
    createMergedStream();

    auto index_granularity_ptr = createMergeTreeIndexGranularity(
        ctx->sum_input_rows_upper_bound,
        ctx->sum_uncompressed_bytes_upper_bound,
        *merge_tree_settings,
        global_ctx->new_data_part->index_granularity_info,
        ctx->blocks_are_granules_size);

    global_ctx->to = std::make_shared<MergedBlockOutputStream>(
        global_ctx->new_data_part,
        merge_tree_settings,
        global_ctx->metadata_snapshot,
        global_ctx->merging_columns,
        MergeTreeIndexFactory::instance().getMany(global_ctx->merging_skip_indexes),
        global_ctx->compression_codec,
        std::move(index_granularity_ptr),
        global_ctx->txn ? global_ctx->txn->tid : Tx::PrehistoricTID,
        global_ctx->merge_list_element_ptr->total_size_bytes_compressed,
        /*reset_columns=*/ true,
        ctx->blocks_are_granules_size,
        global_ctx->context->getWriteSettings(),
        &global_ctx->written_offset_substreams);

    global_ctx->rows_written = 0;
    ctx->initial_reservation = global_ctx->space_reservation ? global_ctx->space_reservation->getSize() : 0;

    ctx->is_cancelled = [merges_blocker = global_ctx->merges_blocker,
        ttl_merges_blocker = global_ctx->ttl_merges_blocker,
        need_remove = ctx->need_remove_expired_values,
        merge_list_element = global_ctx->merge_list_element_ptr,
        partition_id = global_ctx->future_part->part_info.getPartitionId()]() -> bool
    {
        return merges_blocker->isCancelledForPartition(partition_id)
            || (need_remove && ttl_merges_blocker->isCancelled())
            || merge_list_element->is_cancelled.load(std::memory_order_relaxed);
    };

    /// This is the end of preparation. Execution will be per block.
    return false;
}

bool MergeTask::enabledBlockNumberColumn(GlobalRuntimeContextPtr global_ctx)
{
    return (*global_ctx->data_settings)[MergeTreeSetting::enable_block_number_column]
        && global_ctx->metadata_snapshot->getGroupByTTLs().empty();
}

bool MergeTask::enabledBlockOffsetColumn(GlobalRuntimeContextPtr global_ctx)
{
    return (*global_ctx->data_settings)[MergeTreeSetting::enable_block_offset_column]
        && global_ctx->metadata_snapshot->getGroupByTTLs().empty();
}

void MergeTask::addGatheringColumn(GlobalRuntimeContextPtr global_ctx, const String & name, const DataTypePtr & type)
{
    if (global_ctx->storage_columns.contains(name))
        return;

    global_ctx->storage_columns.emplace_back(name, type);
    global_ctx->gathering_columns.emplace_back(name, type);
}

bool MergeTask::hasLightweightDelete(const FutureMergedMutatedPartPtr & future_part)
{
    for (const auto & part : future_part->parts)
    {
        if (part->hasLightweightDelete())
            return true;
    }

    for (const auto & patch_part : future_part->patch_parts)
    {
        if (patch_part->hasLightweightDelete())
            return true;
    }

    return false;
}

bool MergeTask::isVerticalLightweightDelete(const GlobalRuntimeContext & global_ctx)
{
    if (global_ctx.merging_params.mode != MergeTreeData::MergingParams::Ordinary)
        return false;

    if (global_ctx.chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    if (!(*global_ctx.data_settings)[MergeTreeSetting::vertical_merge_optimize_lightweight_delete])
        return false;

    return hasLightweightDelete(global_ctx.future_part);
}


MergeTask::StageRuntimeContextPtr MergeTask::ExecuteAndFinalizeHorizontalPart::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeHorizontalStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    auto new_ctx = std::make_shared<VerticalMergeRuntimeContext>();

    new_ctx->rows_sources_temporary_file = std::move(ctx->rows_sources_temporary_file);
    new_ctx->column_sizes = std::move(ctx->column_sizes);
    new_ctx->it_name_and_type = std::move(ctx->it_name_and_type);
    new_ctx->read_with_direct_io = std::move(ctx->read_with_direct_io);
    new_ctx->need_sync = std::move(ctx->need_sync);

    ctx.reset();
    return new_ctx;
}

MergeTask::StageRuntimeContextPtr MergeTask::VerticalMergeStage::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeVerticalStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    auto new_ctx = std::make_shared<MergeTextIndexRuntimeContext>();
    new_ctx->need_sync = ctx->need_sync;
    ctx.reset();
    return new_ctx;
}


bool MergeTask::ExecuteAndFinalizeHorizontalPart::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}

void MergeTask::ExecuteAndFinalizeHorizontalPart::cancel() noexcept
{
    if (ctx->merge_projection_parts_task_ptr)
        ctx->merge_projection_parts_task_ptr->cancel();
}


void MergeTask::ExecuteAndFinalizeHorizontalPart::prepareProjectionsToMergeAndRebuild() const
{
    const auto mode = (*global_ctx->data_settings)[MergeTreeSetting::deduplicate_merge_projection_mode];
    /// Under throw mode, we still choose to drop projections due to backward compatibility since some
    /// users might have projections before this change.
    if (global_ctx->data->merging_params.mode != MergeTreeData::MergingParams::Ordinary
        && (mode == DeduplicateMergeProjectionMode::THROW || mode == DeduplicateMergeProjectionMode::DROP))
        return;

    const auto & projections = global_ctx->metadata_snapshot->getProjections();

    for (const auto & projection : projections)
    {
        const auto & required_columns = projection.getRequiredColumns();
        bool some_source_column_expired = std::any_of(
            required_columns.begin(),
            required_columns.end(),
            [&](const String & name) { return global_ctx->new_data_part->expired_columns.contains(name); });

        /// The IGNORE mode is checked here purely for backward compatibility.
        /// However, if the projection contains `_parent_part_offset`, it must still be rebuilt,
        /// since offset correctness cannot be ignored even in IGNORE mode.
        if (global_ctx->merge_may_reduce_rows && (mode != DeduplicateMergeProjectionMode::IGNORE || projection.with_parent_part_offset))
        {
            global_ctx->projections_to_rebuild.push_back(&projection);
            continue;
        }
        else if (some_source_column_expired && mode != DeduplicateMergeProjectionMode::IGNORE)
        {
            global_ctx->projections_to_rebuild.push_back(&projection);
            continue;
        }

        MergeTreeData::DataPartsVector projection_parts;
        for (const auto & part : global_ctx->future_part->parts)
        {
            auto it = part->getProjectionParts().find(projection.name);
            if (it != part->getProjectionParts().end() && !it->second->is_broken)
                projection_parts.push_back(it->second);
        }
        if (projection_parts.size() == global_ctx->future_part->parts.size())
        {
            global_ctx->projections_to_merge.push_back(&projection);
            global_ctx->projections_to_merge_parts[projection.name].assign(projection_parts.begin(), projection_parts.end());
        }
        else
        {
            chassert(projection_parts.size() < global_ctx->future_part->parts.size());
            LOG_DEBUG(ctx->log, "Projection {} is not merged because some parts don't have it", projection.name);
            continue;
        }
    }

    const auto & settings = global_ctx->context->getSettingsRef();

    for (const auto * projection : global_ctx->projections_to_rebuild)
        ctx->projection_squashes.emplace_back(std::make_shared<const Block>(projection->sample_block.cloneEmpty()),
            settings[Setting::min_insert_block_size_rows], settings[Setting::min_insert_block_size_bytes]);
}


void MergeTask::ExecuteAndFinalizeHorizontalPart::calculateProjections(const Block & block, UInt64 starting_offset) const
{
    for (size_t i = 0, size = global_ctx->projections_to_rebuild.size(); i < size; ++i)
    {
        const auto & projection = *global_ctx->projections_to_rebuild[i];
        Block block_to_squash = projection.calculate(block, starting_offset, global_ctx->context);
        /// Avoid replacing the projection squash header if nothing was generated (it used to return an empty block)
        if (block_to_squash.rows() == 0)
            continue;

        auto & projection_squash_plan = ctx->projection_squashes[i];
        projection_squash_plan.setHeader(block_to_squash.cloneEmpty());
        Chunk squashed_chunk = Squashing::squash(
            projection_squash_plan.add({block_to_squash.getColumns(), block_to_squash.rows()}),
            projection_squash_plan.getHeader());

        if (squashed_chunk)
        {
            auto result = projection_squash_plan.getHeader()->cloneWithColumns(squashed_chunk.detachColumns());
            auto tmp_part = MergeTreeDataWriter::writeTempProjectionPart(
                *global_ctx->data, ctx->log, result, projection, global_ctx->new_data_part.get(), ++ctx->projection_block_num);

            tmp_part->finalize();
            tmp_part->part->getDataPartStorage().commitTransaction();
            ctx->projection_parts[projection.name].emplace_back(std::move(tmp_part->part));
        }
    }
}


void MergeTask::ExecuteAndFinalizeHorizontalPart::finalizeProjections() const
{
    for (size_t i = 0, size = global_ctx->projections_to_rebuild.size(); i < size; ++i)
    {
        const auto & projection = *global_ctx->projections_to_rebuild[i];
        auto & projection_squash_plan = ctx->projection_squashes[i];
        auto squashed_chunk = Squashing::squash(
            projection_squash_plan.flush(),
            projection_squash_plan.getHeader());

        if (squashed_chunk)
        {
            auto result = projection_squash_plan.getHeader()->cloneWithColumns(squashed_chunk.detachColumns());
            auto temp_part = MergeTreeDataWriter::writeTempProjectionPart(
                *global_ctx->data, ctx->log, result, projection, global_ctx->new_data_part.get(), ++ctx->projection_block_num);

            temp_part->finalize();
            temp_part->part->getDataPartStorage().commitTransaction();
            ctx->projection_parts[projection.name].emplace_back(std::move(temp_part->part));
        }
    }

    ctx->projection_parts_iterator = std::make_move_iterator(ctx->projection_parts.begin());
    if (ctx->projection_parts_iterator != std::make_move_iterator(ctx->projection_parts.end()))
        constructTaskForProjectionPartsMerge();
}


void MergeTask::ExecuteAndFinalizeHorizontalPart::constructTaskForProjectionPartsMerge() const
{
    auto && [name, parts] = *ctx->projection_parts_iterator;
    const auto & projection = global_ctx->metadata_snapshot->projections.get(name);

    ctx->merge_projection_parts_task_ptr = std::make_unique<MergeProjectionPartsTask>
    (
        name,
        std::move(parts),
        projection,
        ctx->projection_block_num,
        global_ctx->context,
        global_ctx->holder,
        global_ctx->mutator,
        global_ctx->merge_entry,
        global_ctx->time_of_merge,
        global_ctx->new_data_part,
        global_ctx->space_reservation
    );
}


bool MergeTask::ExecuteAndFinalizeHorizontalPart::executeMergeProjections() const
{
    /// In case if there are no projections we didn't construct a task
    if (!ctx->merge_projection_parts_task_ptr)
        return false;

    if (ctx->merge_projection_parts_task_ptr->executeStep())
        return true;

    ++ctx->projection_parts_iterator;

    if (ctx->projection_parts_iterator == std::make_move_iterator(ctx->projection_parts.end()))
        return false;

    constructTaskForProjectionPartsMerge();

    return true;
}

bool MergeTask::ExecuteAndFinalizeHorizontalPart::executeImpl() const
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    UInt64 step_time_ms
        = (*global_ctx->data_settings)[MergeTreeSetting::background_task_preferred_step_execution_time_ms].totalMilliseconds();

    do
    {
        Block block;

        if (ctx->is_cancelled() || !global_ctx->merging_executor->pull(block))
        {
            finalize();
            return false;
        }

        /// Record _part_offset mapping and remove unneeded column
        if (global_ctx->merged_part_offsets && global_ctx->parent_part == nullptr)
        {
            if (global_ctx->merged_part_offsets->isMappingEnabled())
            {
                chassert(block.has("_part_index"));
                auto part_index_column = block.getByName("_part_index").column->convertToFullColumnIfSparse();
                const auto & index_data = assert_cast<const ColumnUInt64 &>(*part_index_column).getData();
                global_ctx->merged_part_offsets->insert(index_data.begin(), index_data.end());
                block.erase("_part_index");
            }
        }

        size_t starting_offset = global_ctx->rows_written;
        global_ctx->rows_written += block.rows();
        ProfileEvents::increment(ProfileEvents::MergeWrittenRows, block.rows());
        const_cast<MergedBlockOutputStream &>(*global_ctx->to).write(block);

        if (global_ctx->merge_may_reduce_rows)
        {
            global_ctx->new_data_part->minmax_idx->update(
                block, MergeTreeData::getMinMaxColumnsNames(global_ctx->metadata_snapshot->getPartitionKey()));
        }

        calculateProjections(block, starting_offset);

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
                ? std::min(1., 1. * static_cast<double>(global_ctx->rows_written) / static_cast<double>(ctx->sum_input_rows_upper_bound))
                : std::min(1., global_ctx->merge_list_element_ptr->progress.load(std::memory_order_relaxed));

            global_ctx->space_reservation->update(static_cast<size_t>((1. - progress) * static_cast<double>(ctx->initial_reservation)));
        }
    } while (watch.elapsedMilliseconds() < step_time_ms);

    /// Need execute again
    return true;
}


void MergeTask::ExecuteAndFinalizeHorizontalPart::finalize() const
{
    mergeBuiltStatistics(std::move(ctx->build_statistics_transforms), global_ctx);
    finalizeProjections();
    global_ctx->to->finalizeIndexGranularity();
    global_ctx->merging_executor.reset();
    global_ctx->merged_pipeline.reset();
    ctx->build_statistics_transforms.clear();

    global_ctx->checkOperationIsNotCanceled();

    if (ctx->need_remove_expired_values && global_ctx->ttl_merges_blocker->isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts with expired TTL");

    const size_t sum_compressed_bytes_upper_bound = global_ctx->merge_list_element_ptr->total_size_bytes_compressed;
    ctx->need_sync = global_ctx->data_settings->needSyncPart(ctx->sum_input_rows_upper_bound, sum_compressed_bytes_upper_bound);
}

bool MergeTask::VerticalMergeStage::prepareVerticalMergeForAllColumns() const
{
    /// No need to execute this part if it is horizontal merge.
    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    size_t sum_input_rows_exact = global_ctx->merge_list_element_ptr->rows_read;
    size_t input_rows_filtered = *global_ctx->input_rows_filtered;
    global_ctx->merge_list_element_ptr->columns_written = global_ctx->merging_columns.size();
    global_ctx->merge_list_element_ptr->progress.store(ctx->column_sizes->keyColumnsWeight(), std::memory_order_relaxed);

    /// Ensure data has written to disk.
    size_t rows_sources_count = ctx->rows_sources_temporary_file->finalizeWriting();
    /// In special case, when there is only one source part, and no rows were skipped, we may have
    /// skipped writing rows_sources file. Otherwise rows_sources_count must be equal to the total
    /// number of input rows.
    /// Note that only one byte index is written for each row, so number of rows is equals to the number of bytes written.
    if ((rows_sources_count > 0 || global_ctx->future_part->parts.size() > 1) && sum_input_rows_exact != rows_sources_count + input_rows_filtered)
        throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Number of rows in source parts ({}) excluding filtered rows ({}) differs from number "
                        "of bytes written to rows_sources file ({}). It is a bug.",
                        sum_input_rows_exact, input_rows_filtered, rows_sources_count);


    ctx->it_name_and_type = global_ctx->gathering_columns.cbegin();

    const auto & storage_settings = *global_ctx->data_settings;

    if (global_ctx->new_data_part->getDataPartStorage().supportParallelWrite())
        ctx->max_delayed_streams = storage_settings[MergeTreeSetting::max_merge_delayed_streams_for_parallel_write];

    bool all_parts_on_remote_disks = std::ranges::all_of(global_ctx->future_part->parts, [](const auto & part) { return part->isStoredOnRemoteDisk(); });
    ctx->use_prefetch = all_parts_on_remote_disks && storage_settings[MergeTreeSetting::vertical_merge_remote_filesystem_prefetch];

    if (ctx->use_prefetch && ctx->it_name_and_type != global_ctx->gathering_columns.end())
        ctx->prepared_pipeline = createPipelineForReadingOneColumn(ctx->it_name_and_type->name);

    return false;
}

/// Gathers values from all parts for one column using rows sources temporary file
class ColumnGathererStep : public ITransformingStep
{
public:
    ColumnGathererStep(
        const SharedHeader & input_header_,
        const String & rows_sources_temporary_file_name_,
        UInt64 merge_block_size_rows_,
        UInt64 merge_block_size_bytes_,
        std::optional<size_t> max_dynamic_subcolumns_,
        bool is_result_sparse_)
        : ITransformingStep(input_header_, input_header_, getTraits())
        , rows_sources_temporary_file_name(rows_sources_temporary_file_name_)
        , merge_block_size_rows(merge_block_size_rows_)
        , merge_block_size_bytes(merge_block_size_bytes_)
        , max_dynamic_subcolumns(max_dynamic_subcolumns_)
        , is_result_sparse(is_result_sparse_)
    {}

    String getName() const override { return "ColumnGatherer"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & pipeline_settings) override
    {
        const auto & header = pipeline.getSharedHeader();
        const auto input_streams_count = pipeline.getNumStreams();

        if (!pipeline_settings.temporary_file_lookup)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file lookup is not set in pipeline settings for vertical merge");

        auto rows_sources_read_buf = pipeline_settings.temporary_file_lookup->getTemporaryFileForReading(rows_sources_temporary_file_name);

        auto transform = std::make_unique<ColumnGathererTransform>(
            header,
            input_streams_count,
            std::move(rows_sources_read_buf),
            merge_block_size_rows,
            merge_block_size_bytes,
            max_dynamic_subcolumns,
            is_result_sparse);

        pipeline.addTransform(std::move(transform));
    }

    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

private:
    static Traits getTraits()
    {
        return ITransformingStep::Traits
        {
            {
                .returns_single_stream = true,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = false,
            }
        };
    }

    MergeTreeData::MergingParams merging_params{};
    const String rows_sources_temporary_file_name;
    const UInt64 merge_block_size_rows;
    const UInt64 merge_block_size_bytes;
    const std::optional<size_t> max_dynamic_subcolumns;
    const bool is_result_sparse;
};

MergeTask::VerticalMergeRuntimeContext::PreparedColumnPipeline
MergeTask::VerticalMergeStage::createPipelineForReadingOneColumn(const String & column_name) const
{
    /// Read from all parts
    std::vector<QueryPlanPtr> plans;
    size_t part_starting_offset = 0;
    /// Do not apply mask for lightweight delete in vertical merge, because it is applied in merging algorithm
    bool apply_deleted_mask = !global_ctx->vertical_lightweight_delete;
    /// Collect statistics transforms for this column separately.
    BuildStatisticsTransformMap column_build_statistics_transforms;

    for (size_t part_num = 0; part_num < global_ctx->future_part->parts.size(); ++part_num)
    {
        auto plan_for_part = std::make_unique<QueryPlan>();

        createReadFromPartStep(
            MergeTreeSequentialSourceType::Merge,
            *plan_for_part,
            *global_ctx->data,
            global_ctx->storage_snapshot,
            RangesInDataPart(global_ctx->future_part->parts[part_num], nullptr, part_num, part_starting_offset),
            global_ctx->alter_conversions[part_num],
            global_ctx->merged_part_offsets,
            Names{column_name},
            global_ctx->input_rows_filtered,
            apply_deleted_mask,
            /*filter=*/ std::nullopt,
            ctx->read_with_direct_io,
            ctx->use_prefetch,
            global_ctx->context,
            getLogger("VerticalMergeStage"));


        /// Add step for building missed text indexes and statistics for single parts.
        /// If merge may reduce rows, we will rebuild index and statistics
        /// for the resulting part in the end of the pipeline.
        if (!global_ctx->merge_may_reduce_rows)
        {
            addBuildTextIndexesStep(*plan_for_part, *global_ctx->future_part->parts[part_num], global_ctx);

            if (auto transform = addBuildStatisticsStep(*plan_for_part, *global_ctx->future_part->parts[part_num], global_ctx))
                column_build_statistics_transforms.emplace(global_ctx->future_part->parts[part_num]->name, std::move(transform));
        }

        plans.emplace_back(std::move(plan_for_part));
        part_starting_offset += global_ctx->future_part->parts[part_num]->rows_count;
    }

    QueryPlan merge_column_query_plan;

    /// Union of all parts streams
    {
        SharedHeaders input_headers;
        input_headers.reserve(plans.size());
        for (auto & plan : plans)
            input_headers.emplace_back(plan->getCurrentHeader());

        auto union_step = std::make_unique<UnionStep>(std::move(input_headers));
        merge_column_query_plan.unitePlans(std::move(union_step), std::move(plans));
    }

    /// Add column gatherer step
    {
        const auto & merge_tree_settings = global_ctx->data_settings;
        std::optional<size_t> max_dynamic_subcolumns = std::nullopt;
        if (global_ctx->future_part->part_format.part_type == MergeTreeDataPartType::Wide)
            max_dynamic_subcolumns = (*merge_tree_settings)[MergeTreeSetting::merge_max_dynamic_subcolumns_in_wide_part].valueOrNullopt();
        else if (global_ctx->future_part->part_format.part_type == MergeTreeDataPartType::Compact)
            max_dynamic_subcolumns = (*merge_tree_settings)[MergeTreeSetting::merge_max_dynamic_subcolumns_in_compact_part].valueOrNullopt();

        bool is_result_sparse = ISerialization::hasKind(global_ctx->new_data_part->getSerialization(column_name)->getKindStack(), ISerialization::Kind::SPARSE);
        auto merge_step = std::make_unique<ColumnGathererStep>(
            merge_column_query_plan.getCurrentHeader(),
            RowsSourcesTemporaryFile::FILE_ID,
            (*merge_tree_settings)[MergeTreeSetting::merge_max_block_size],
            (*merge_tree_settings)[MergeTreeSetting::merge_max_block_size_bytes],
            max_dynamic_subcolumns,
            is_result_sparse);

        merge_step->setStepDescription("Gather column");
        merge_column_query_plan.addStep(std::move(merge_step));
    }

    /// Add expression step for indexes
    MergeTreeIndices indexes_to_recalc;
    auto indexes_it = global_ctx->skip_indexes_by_column.find(column_name);

    if (indexes_it != global_ctx->skip_indexes_by_column.end())
    {
        indexes_to_recalc = MergeTreeIndexFactory::instance().getMany(indexes_it->second);
        addSkipIndexesExpressionSteps(merge_column_query_plan, indexes_it->second, global_ctx);
    }

    /// If merge may reduce rows, rebuild text indexes and statistics for the resulting part.
    if (global_ctx->merge_may_reduce_rows)
    {
        addBuildTextIndexesStep(merge_column_query_plan, *global_ctx->new_data_part, global_ctx);

        if (auto transform = addBuildStatisticsStep(merge_column_query_plan, *global_ctx->new_data_part, global_ctx))
            column_build_statistics_transforms.emplace(global_ctx->new_data_part->name, std::move(transform));
    }

    QueryPlanOptimizationSettings optimization_settings(global_ctx->context);
    auto pipeline_settings = BuildQueryPipelineSettings(global_ctx->context);
    pipeline_settings.temporary_file_lookup = ctx->rows_sources_temporary_file;
    auto builder = merge_column_query_plan.buildQueryPipeline(optimization_settings, pipeline_settings);

    return {QueryPipelineBuilder::getPipeline(std::move(*builder)), std::move(indexes_to_recalc), std::move(column_build_statistics_transforms)};
}

void MergeTask::VerticalMergeStage::prepareVerticalMergeForOneColumn() const
{
    const auto & column_name = ctx->it_name_and_type->name;

    ctx->progress_before = global_ctx->merge_list_element_ptr->progress.load(std::memory_order_relaxed);
    global_ctx->column_progress = std::make_unique<MergeStageProgress>(ctx->progress_before, ctx->column_sizes->columnWeight(column_name));

    VerticalMergeRuntimeContext::PreparedColumnPipeline column_pipepline;
    if (ctx->prepared_pipeline)
    {
        column_pipepline = std::move(*ctx->prepared_pipeline);

        /// Prepare next column pipeline to initiate prefetching
        auto next_column_it = std::next(ctx->it_name_and_type);
        if (next_column_it != global_ctx->gathering_columns.end())
            ctx->prepared_pipeline = createPipelineForReadingOneColumn(next_column_it->name);
    }
    else
    {
        column_pipepline = createPipelineForReadingOneColumn(column_name);
    }

    ctx->build_statistics_transforms = std::move(column_pipepline.build_statistics_transforms);
    ctx->column_parts_pipeline = std::move(column_pipepline.pipeline);

    /// Dereference unique_ptr
    ctx->column_parts_pipeline.setProgressCallback(MergeProgressCallback(
        global_ctx->merge_list_element_ptr,
        global_ctx->watch_prev_elapsed,
        *global_ctx->column_progress,
        [&my_ctx = *global_ctx]() { my_ctx.checkOperationIsNotCanceled(); }));

    /// Is calculated inside MergeProgressCallback.
    ctx->column_parts_pipeline.disableProfileEventUpdate();
    ctx->executor = std::make_unique<PullingPipelineExecutor>(ctx->column_parts_pipeline);

    NamesAndTypesList columns_list = {*ctx->it_name_and_type};

    ctx->column_to = std::make_unique<MergedColumnOnlyOutputStream>(
        global_ctx->new_data_part,
        global_ctx->data_settings,
        global_ctx->metadata_snapshot,
        columns_list,
        column_pipepline.indexes_to_recalc,
        global_ctx->compression_codec,
        global_ctx->to->getIndexGranularity(),
        global_ctx->merge_list_element_ptr->total_size_bytes_uncompressed,
        &global_ctx->written_offset_substreams);

    ctx->column_elems_written = 0;
}


bool MergeTask::VerticalMergeStage::executeVerticalMergeForOneColumn() const
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    UInt64 step_time_ms
        = (*global_ctx->data_settings)[MergeTreeSetting::background_task_preferred_step_execution_time_ms].totalMilliseconds();

    do
    {
        Block block;

        if (global_ctx->isCancelled()
            || !ctx->executor->pull(block))
            return false;

        ctx->column_elems_written += block.rows();
        ctx->column_to->write(block);
    } while (watch.elapsedMilliseconds() < step_time_ms);

    /// Need execute again
    return true;
}


void MergeTask::VerticalMergeStage::finalizeVerticalMergeForOneColumn() const
{
    const String & column_name = ctx->it_name_and_type->name;
    global_ctx->checkOperationIsNotCanceled();

    ctx->executor.reset();
    mergeBuiltStatistics(std::move(ctx->build_statistics_transforms), global_ctx);

    ctx->column_to->finalizeIndexGranularity();
    auto changed_checksums = ctx->column_to->fillChecksums(global_ctx->new_data_part, global_ctx->new_data_part->checksums);
    global_ctx->gathered_data.checksums.add(std::move(changed_checksums));

    const auto & columns_substreams = ctx->column_to->getColumnsSubstreams();
    global_ctx->gathered_data.columns_substreams = ColumnsSubstreams::merge(global_ctx->gathered_data.columns_substreams, columns_substreams, global_ctx->new_data_part->getColumns().getNames());

    auto cached_marks = ctx->column_to->releaseCachedMarks();
    for (auto & [name, marks] : cached_marks)
        global_ctx->cached_marks.emplace(name, std::move(marks));

    auto cached_index_marks = ctx->column_to->releaseCachedIndexMarks();
    for (auto & [name, marks] : cached_index_marks)
        global_ctx->cached_index_marks.emplace(name, std::move(marks));

    ctx->delayed_streams.emplace_back(std::move(ctx->column_to));

    while (ctx->delayed_streams.size() > ctx->max_delayed_streams)
    {
        ctx->delayed_streams.front()->finish(ctx->need_sync);
        ctx->delayed_streams.pop_front();
    }

    if (!global_ctx->isCancelled() && global_ctx->rows_written != ctx->column_elems_written)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Written {} elements of column {}, but {} rows of PK columns",
                        toString(ctx->column_elems_written), column_name, toString(global_ctx->rows_written));
    }

    UInt64 rows = 0;
    UInt64 bytes = 0;
    ctx->column_parts_pipeline.tryGetResultRowsAndBytes(rows, bytes);

    /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).

    global_ctx->merge_list_element_ptr->columns_written += 1;
    global_ctx->merge_list_element_ptr->bytes_written_uncompressed += bytes;
    global_ctx->merge_list_element_ptr->progress.store(ctx->progress_before + ctx->column_sizes->columnWeight(column_name), std::memory_order_relaxed);

    /// This is the external loop increment.
    ++ctx->it_name_and_type;
}


bool MergeTask::VerticalMergeStage::finalizeVerticalMergeForAllColumns() const
{
    for (auto & stream : ctx->delayed_streams)
        stream->finish(ctx->need_sync);

    return false;
}


bool MergeTask::MergeProjectionsStage::prepareProjections() const
{
    /// Print overall profiling info. NOTE: it may duplicates previous messages
    {
        ProfileEvents::increment(ProfileEvents::MergedColumns, global_ctx->merging_columns.size());
        ProfileEvents::increment(ProfileEvents::GatheredColumns, global_ctx->gathering_columns.size());

        double elapsed_seconds = global_ctx->merge_list_element_ptr->watch.elapsedSeconds();
        LOG_DEBUG(ctx->log,
            "Merge sorted {} rows, containing {} columns ({} merged, {} gathered) in {} sec., {} rows/sec., {}/sec.",
            global_ctx->merge_list_element_ptr->rows_read.load(),
            global_ctx->storage_columns.size(),
            global_ctx->merging_columns.size(),
            global_ctx->gathering_columns.size(),
            elapsed_seconds,
            static_cast<double>(global_ctx->merge_list_element_ptr->rows_read) / elapsed_seconds,
            ReadableSize(static_cast<double>(global_ctx->merge_list_element_ptr->bytes_read_uncompressed) / elapsed_seconds));
    }

    if (global_ctx->merged_part_offsets && !global_ctx->merged_part_offsets->isFinalized())
        global_ctx->merged_part_offsets->flush();

    for (const auto & projection : global_ctx->projections_to_merge)
    {
        MergeTreeData::DataPartsVector projection_parts = global_ctx->projections_to_merge_parts[projection->name];
        LOG_DEBUG(
            ctx->log,
            "Selected {} projection_parts from {} to {}",
            projection_parts.size(),
            projection_parts.front()->name,
            projection_parts.back()->name);

        auto projection_future_part = std::make_shared<FutureMergedMutatedPart>();
        projection_future_part->assign(std::move(projection_parts), /*patch_parts_=*/ {}, projection);
        projection_future_part->name = projection->name;
        // TODO (ab): path in future_part is only for merge process introspection, which is not available for merges of projection parts.
        // Let's comment this out to avoid code inconsistency and add it back after we implement projection merge introspection.
        // projection_future_part->path = global_ctx->future_part->path + "/" + projection.name + ".proj/";
        projection_future_part->part_info = MergeListElement::FAKE_RESULT_PART_FOR_PROJECTION;

        MergeTreeData::MergingParams projection_merging_params;
        projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
        if (projection->type == ProjectionDescription::Type::Aggregate)
            projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

        ctx->tasks_for_projections.emplace_back(std::make_shared<MergeTask>(
            projection_future_part,
            projection->metadata,
            global_ctx->merge_entry,
            std::make_unique<MergeListElement>((*global_ctx->merge_entry)->table_id, projection_future_part, global_ctx->context),
            global_ctx->time_of_merge,
            global_ctx->context,
            *global_ctx->holder,
            global_ctx->space_reservation,
            global_ctx->deduplicate,
            global_ctx->deduplicate_by_columns,
            global_ctx->cleanup,
            projection_merging_params,
            global_ctx->need_prefix,
            projection,
            global_ctx->new_data_part.get(),
            projection->with_parent_part_offset ? global_ctx->merged_part_offsets : nullptr,
            ".proj",
            NO_TRANSACTION_PTR,
            global_ctx->data,
            global_ctx->mutator,
            global_ctx->merges_blocker,
            global_ctx->ttl_merges_blocker));
    }

    /// merge projections with _part_offset first so that we can release offset mapping earlier.
    std::sort(
        ctx->tasks_for_projections.begin(),
        ctx->tasks_for_projections.end(),
        [](const auto & l, const auto & r) { return l->global_ctx->merged_part_offsets && !r->global_ctx->merged_part_offsets; });

    /// We will iterate through projections and execute them
    ctx->projections_iterator = ctx->tasks_for_projections.begin();

    return false;
}


bool MergeTask::MergeProjectionsStage::executeProjections() const
{
    if (ctx->projections_iterator == ctx->tasks_for_projections.end())
        return false;

    /// Release offset mapping when all projections with _part_offset has been merged.
    if (global_ctx->merged_part_offsets && !(*ctx->projections_iterator)->global_ctx->merged_part_offsets)
        global_ctx->merged_part_offsets->clear();

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
        global_ctx->to->finalizePart(global_ctx->new_data_part, global_ctx->gathered_data, ctx->need_sync, nullptr);
    else
        global_ctx->to->finalizePart(global_ctx->new_data_part, global_ctx->gathered_data, ctx->need_sync, &global_ctx->storage_columns);

    auto cached_marks = global_ctx->to->releaseCachedMarks();
    for (auto & [name, marks] : cached_marks)
        global_ctx->cached_marks.emplace(name, std::move(marks));

    auto cached_index_marks = global_ctx->to->releaseCachedIndexMarks();
    for (auto & [name, marks] : cached_index_marks)
        global_ctx->cached_index_marks.emplace(name, std::move(marks));

    global_ctx->new_data_part->getDataPartStorage().precommitTransaction();
    global_ctx->promise.set_value(std::exchange(global_ctx->new_data_part, nullptr));

    return false;
}

MergeTask::StageRuntimeContextPtr MergeTask::MergeProjectionsStage::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    /// The projection stage has its own empty projection stage which may add a drift of several milliseconds.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeProjectionStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    return nullptr;
}

bool MergeTask::VerticalMergeStage::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;

    return subtasks_iterator != subtasks.end();
}

void MergeTask::VerticalMergeStage::cancel() noexcept
{
    if (ctx->column_to)
        ctx->column_to->cancel();

    if (ctx->prepared_pipeline.has_value())
        ctx->prepared_pipeline->pipeline.cancel();

    for (auto & stream : ctx->delayed_streams)
        stream->cancel();

    if (ctx->executor)
        ctx->executor->cancel();

}

MergeTask::StageRuntimeContextPtr MergeTask::MergeTextIndexStage::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeTextIndexStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    auto new_ctx = std::make_shared<MergeProjectionsRuntimeContext>();
    new_ctx->need_sync = ctx->need_sync;
    ctx.reset();
    return new_ctx;
}

bool MergeTask::MergeTextIndexStage::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}

void MergeTask::MergeTextIndexStage::cancel() noexcept
{
    for (auto & task : ctx->merge_tasks)
        task->cancel();
}

std::vector<TextIndexSegment> MergeTask::MergeTextIndexStage::getTextIndexSegments(const String & part_name, const String & index_name, size_t part_idx) const
{
    auto it = global_ctx->build_text_index_transforms.find(part_name);
    if (it == global_ctx->build_text_index_transforms.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index transform for index {} and part {} not found", index_name, part_name);

    for (const auto & transform : it->second)
    {
        if (transform->hasIndex(index_name))
            return transform->getSegments(index_name, part_idx);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index transform for index {} and part {} not found", index_name, part_name);
}

bool MergeTask::MergeTextIndexStage::prepare() const
{
    if (global_ctx->parent_part)
        return false;

    if (global_ctx->text_indexes_to_merge.empty())
        return false;

    if (!global_ctx->merge_may_reduce_rows)
    {
        /// If merge text indexes without rebuilt, part offsets must be set.
        if (!global_ctx->merged_part_offsets)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Merged part offsets are not set");

        global_ctx->merged_part_offsets->flush();
    }

    if (global_ctx->temporary_text_index_storage)
        global_ctx->temporary_text_index_storage->commitTransaction();

    auto reader_settings = MergeTreeReaderSettings::createForMergeMutation(global_ctx->context->getReadSettings());

    for (const auto & index : global_ctx->text_indexes_to_merge)
    {
        auto index_ptr = MergeTreeIndexFactory::instance().get(index);
        std::vector<TextIndexSegment> segments;

        if (global_ctx->merge_may_reduce_rows)
        {
            /// Text index was built for the resulting part.
            segments = getTextIndexSegments(global_ctx->new_data_part->name, index.name, 0);
        }
        else
        {
            for (size_t part_idx = 0; part_idx < global_ctx->future_part->parts.size(); ++part_idx)
            {
                const auto & part = global_ctx->future_part->parts[part_idx];

                if (index_ptr->getDeserializedFormat(part->checksums, index_ptr->getFileName()))
                {
                    /// If text index exists in the source part, take it as is.
                    segments.emplace_back(part->getDataPartStoragePtr(), index_ptr->getFileName(), part_idx);
                }
                else
                {
                    /// Otherwise, it should be materialized on merge.
                    /// Take the segments from the build text index transform.
                    auto part_segments = getTextIndexSegments(part->name, index.name, part_idx);
                    std::move(part_segments.begin(), part_segments.end(), std::back_inserter(segments));
                }
            }
        }

        auto task = std::make_unique<MergeTextIndexesTask>(
            std::move(segments),
            global_ctx->new_data_part,
            index_ptr,
            global_ctx->merged_part_offsets,
            reader_settings,
            global_ctx->to->getWriterSettings());

        ctx->merge_tasks.emplace_back(std::move(task));
    }

    return false;
}

bool MergeTask::MergeTextIndexStage::execute() const
{
    if (global_ctx->parent_part)
        return false;

    if (ctx->merge_tasks.empty())
        return false;

    auto & task = ctx->merge_tasks.back();
    if (task->executeStep())
        return true;

    task->addToChecksums(global_ctx->gathered_data.checksums);
    ctx->merge_tasks.pop_back();
    return !ctx->merge_tasks.empty();
}

bool MergeTask::MergeTextIndexStage::finalize() const
{
    if (global_ctx->parent_part)
        return false;

    if (global_ctx->merged_part_offsets && global_ctx->projections_to_merge.empty())
        global_ctx->merged_part_offsets->clear();

    if (global_ctx->temporary_text_index_storage)
        global_ctx->temporary_text_index_storage->removeRecursive();

    return false;
}

bool MergeTask::MergeProjectionsStage::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}

void MergeTask::MergeProjectionsStage::cancel() noexcept
{
    for (auto & prj_task: ctx->tasks_for_projections)
        prj_task->cancel();
}


bool MergeTask::VerticalMergeStage::executeVerticalMergeForAllColumns() const
{
    /// No need to execute this part if it is horizontal merge.
    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    /// This is the external cycle condition
    if (ctx->it_name_and_type == global_ctx->gathering_columns.end())
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
try
{
    chassert(stages_iterator != stages.end());
    const auto & current_stage = *stages_iterator;

    if (current_stage->execute())
        return true;

    /// Stage is finished, need to initialize context for the next stage and update profile events.

    UInt64 current_elapsed_ms = global_ctx->merge_list_element_ptr->watch.elapsedMilliseconds();
    UInt64 stage_elapsed_ms = current_elapsed_ms - global_ctx->prev_elapsed_ms;
    global_ctx->prev_elapsed_ms = current_elapsed_ms;

    auto next_stage_context = current_stage->getContextForNextStage();

    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(current_stage->getTotalTimeProfileEvent(), stage_elapsed_ms);
        ProfileEvents::increment(ProfileEvents::MergeTotalMilliseconds, stage_elapsed_ms);
    }

    /// Move to the next stage in an array of stages
    ++stages_iterator;
    if (stages_iterator == stages.end())
        return false;

    (*stages_iterator)->setRuntimeContext(std::move(next_stage_context), global_ctx);
    return true;
}
catch (...)
{
    DimensionalMetrics::add(
        DimensionalMetrics::MergeFailures,
        {String(ErrorCodes::getName(getCurrentExceptionCode()))});
    throw;
}

void MergeTask::cancel() noexcept
{
    if (stages_iterator != stages.end())
        (*stages_iterator)->cancel();

    if (global_ctx->merging_executor)
        global_ctx->merging_executor->cancel();

    global_ctx->merged_pipeline.cancel();

    if (global_ctx->to)
        global_ctx->to->cancel();

    if (global_ctx->new_data_part)
        global_ctx->new_data_part->removeIfNeeded();
}


/// Apply merge strategy (Ordinary, Collapsing, Aggregating, etc) to the stream
class MergePartsStep : public ITransformingStep
{
public:
    MergePartsStep(
        const SharedHeader & input_header_,
        const SortDescription & sort_description_,
        const Names partition_and_sorting_required_columns_,
        const MergeTreeData::MergingParams & merging_params_,
        const String & rows_sources_temporary_file_name_,
        const std::optional<String> & filter_column_name_,
        UInt64 merge_block_size_rows_,
        UInt64 merge_block_size_bytes_,
        std::optional<size_t> max_dynamic_subcolumns_,
        bool blocks_are_granules_size_,
        bool cleanup_,
        time_t time_of_merge_)
        : ITransformingStep(input_header_, input_header_, getTraits())
        , sort_description(sort_description_)
        , partition_and_sorting_required_columns(partition_and_sorting_required_columns_)
        , merging_params(merging_params_)
        , rows_sources_temporary_file_name(rows_sources_temporary_file_name_)
        , filter_column_name(filter_column_name_)
        , merge_block_size_rows(merge_block_size_rows_)
        , merge_block_size_bytes(merge_block_size_bytes_)
        , max_dynamic_subcolumns(max_dynamic_subcolumns_)
        , blocks_are_granules_size(blocks_are_granules_size_)
        , cleanup(cleanup_)
        , time_of_merge(time_of_merge_)
    {}

    String getName() const override { return "MergeParts"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & pipeline_settings) override
    {
        /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
        /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
        ///  that is going in insertion order.
        ProcessorPtr merged_transform;

        auto header = pipeline.getSharedHeader();
        const auto input_streams_count = pipeline.getNumStreams();

        WriteBuffer * rows_sources_write_buf = nullptr;
        if (!rows_sources_temporary_file_name.empty())
        {
            if (!pipeline_settings.temporary_file_lookup)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file lookup is not set in pipeline settings for vertical merge");
            rows_sources_write_buf = &pipeline_settings.temporary_file_lookup->getTemporaryFileForWriting(rows_sources_temporary_file_name);
        }

        switch (merging_params.mode)
        {
            case MergeTreeData::MergingParams::Ordinary:
                merged_transform = std::make_shared<MergingSortedTransform>(
                    header,
                    input_streams_count,
                    sort_description,
                    merge_block_size_rows,
                    merge_block_size_bytes,
                    max_dynamic_subcolumns,
                    SortingQueueStrategy::Default,
                    /* limit_= */0,
                    /* always_read_till_end_= */false,
                    rows_sources_write_buf,
                    filter_column_name,
                    blocks_are_granules_size);
                break;

            case MergeTreeData::MergingParams::Collapsing:
                merged_transform = std::make_shared<CollapsingSortedTransform>(
                    header, input_streams_count, sort_description, merging_params.sign_column, false,
                    merge_block_size_rows, merge_block_size_bytes, max_dynamic_subcolumns, rows_sources_write_buf, blocks_are_granules_size);
                break;

            case MergeTreeData::MergingParams::Summing:
                merged_transform = std::make_shared<SummingSortedTransform>(
                    header, input_streams_count, sort_description, merging_params.columns_to_sum, partition_and_sorting_required_columns, merge_block_size_rows, merge_block_size_bytes, max_dynamic_subcolumns);
                break;

            case MergeTreeData::MergingParams::Aggregating:
                merged_transform = std::make_shared<AggregatingSortedTransform>(header, input_streams_count, sort_description, merge_block_size_rows, merge_block_size_bytes, max_dynamic_subcolumns);
                break;

            case MergeTreeData::MergingParams::Replacing:
                merged_transform = std::make_shared<ReplacingSortedTransform>(
                    header, input_streams_count, sort_description, merging_params.is_deleted_column, merging_params.version_column,
                    merge_block_size_rows, merge_block_size_bytes, max_dynamic_subcolumns, rows_sources_write_buf, blocks_are_granules_size,
                    cleanup);
                break;

            case MergeTreeData::MergingParams::Coalescing:
                merged_transform = std::make_shared<CoalescingSortedTransform>(
                    header, input_streams_count, sort_description, merging_params.columns_to_sum, partition_and_sorting_required_columns, merge_block_size_rows, merge_block_size_bytes, max_dynamic_subcolumns);
                break;

            case MergeTreeData::MergingParams::Graphite:
                merged_transform = std::make_shared<GraphiteRollupSortedTransform>(
                    header, input_streams_count, sort_description, merge_block_size_rows, merge_block_size_bytes, max_dynamic_subcolumns,
                    merging_params.graphite_params, time_of_merge);
                break;

            case MergeTreeData::MergingParams::VersionedCollapsing:
                merged_transform = std::make_shared<VersionedCollapsingTransform>(
                    header, input_streams_count, sort_description, merging_params.sign_column,
                    merge_block_size_rows, merge_block_size_bytes, max_dynamic_subcolumns, rows_sources_write_buf, blocks_are_granules_size);
                break;
        }

        pipeline.addTransform(std::move(merged_transform));

#ifndef NDEBUG
        if (!sort_description.empty())
        {
            pipeline.addSimpleTransform([&](const SharedHeader & header_)
            {
                auto transform = std::make_shared<CheckSortedTransform>(header_, sort_description);
                return transform;
            });
        }
#endif
    }

    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

private:
    static Traits getTraits()
    {
        return ITransformingStep::Traits
        {
            {
                .returns_single_stream = true,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = false,
            }
        };
    }

    const SortDescription sort_description;
    const Names partition_and_sorting_required_columns;
    const MergeTreeData::MergingParams merging_params{};
    const String rows_sources_temporary_file_name;
    const std::optional<String> filter_column_name;
    const UInt64 merge_block_size_rows;
    const UInt64 merge_block_size_bytes;
    const std::optional<size_t> max_dynamic_subcolumns;
    const bool blocks_are_granules_size;
    const bool cleanup{false};
    const time_t time_of_merge{0};
};

class TTLStep : public ITransformingStep
{
public:
    TTLStep(
        const SharedHeader & input_header_,
        const ContextPtr & context_,
        const MergeTreeData & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeData::MutableDataPartPtr & data_part_,
        const NamesAndTypesList & expired_columns_,
        time_t current_time,
        bool force_)
        : ITransformingStep(input_header_, TTLTransform::addExpiredColumnsToBlock(input_header_, expired_columns_), getTraits())
    {
        transform = std::make_shared<TTLTransform>(
            context_, input_header_, storage_, metadata_snapshot_, data_part_, expired_columns_, current_time, force_);
        subqueries_for_sets = transform->getSubqueries();
    }

    String getName() const override { return "TTL"; }

    PreparedSets::Subqueries getSubqueries() { return std::move(subqueries_for_sets); }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addTransform(transform);
    }

    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

private:
    static Traits getTraits()
    {
        return ITransformingStep::Traits
        {
            {
                .returns_single_stream = true,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = false,
            }
        };
    }

    std::shared_ptr<TTLTransform> transform;
    PreparedSets::Subqueries subqueries_for_sets;
};

class BuildTextIndexStep : public ITransformingStep, private WithContext
{
public:
    BuildTextIndexStep(SharedHeader input_header_, SharedHeader output_header_, std::shared_ptr<BuildTextIndexTransform> transform_, ContextPtr context_)
        : ITransformingStep(input_header_, output_header_, getTraits())
        , WithContext(context_)
        , transform(std::move(transform_))
        , original_output_header(output_header_)
    {
    }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addTransform(transform);

        /// Remove possible temporary columns added by index expression.
        if (!isCompatibleHeader(*pipeline.getSharedHeader(), *getOutputHeader()))
        {
            auto converting_dag = ActionsDAG::makeConvertingActions(
                pipeline.getSharedHeader()->getColumnsWithTypeAndName(),
                getOutputHeader()->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                getContext());

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));

            pipeline.addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<ExpressionTransform>(header, std::move(converting_actions));
            });
        }
    }

    void updateOutputHeader() override
    {
        /// The output header must be the original header (without extra index
        /// expression columns), not the input header which may contain extra
        /// columns added by index expression steps. This is important to keep
        /// all per-part plan headers compatible in the UnionStep during merge.
        output_header = original_output_header;
    }

    String getName() const override { return "BuildTextIndex"; }

private:
    static Traits getTraits()
    {
        return ITransformingStep::Traits
        {
            {
                .returns_single_stream = true,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = true,
            }
        };
    }

    std::shared_ptr<BuildTextIndexTransform> transform;
    const SharedHeader original_output_header;
};

void MergeTask::addSkipIndexesExpressionSteps(QueryPlan & plan, const IndicesDescription & indices_description, const GlobalRuntimeContextPtr & global_ctx)
{
    auto indices_expression = indices_description.getSingleExpressionForIndices(global_ctx->metadata_snapshot->getColumns(), global_ctx->data->getContext());
    auto indices_expression_dag = indices_expression->getActionsDAG().clone();
    auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(*plan.getCurrentHeader(), indices_expression_dag.getRequiredColumnsNames(), global_ctx->data->getContext());
    indices_expression_dag.addMaterializingOutputActions(/*materialize_sparse=*/ true); /// Const columns cannot be written without materialization.

    auto calculate_indices_expression_step = std::make_unique<ExpressionStep>(
        plan.getCurrentHeader(),
        ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(indices_expression_dag)));

    plan.addStep(std::move(calculate_indices_expression_step));
}

void MergeTask::addBuildTextIndexesStep(QueryPlan & plan, const IMergeTreeDataPart & data_part, const GlobalRuntimeContextPtr & global_ctx)
{
    if (global_ctx->text_indexes_to_merge.empty())
        return;

    auto original_header = plan.getCurrentHeader();
    auto read_column_names = original_header->getNameSet();

    IndicesDescription description_to_build;
    std::vector<MergeTreeIndexPtr> indexes_to_build;
    auto storage_columns = global_ctx->storage_columns.getNameSet();

    for (const auto & index : global_ctx->text_indexes_to_merge)
    {
        auto required_columns = index.expression->getRequiredColumns();

        bool read_any_required_column = std::ranges::any_of(required_columns, [&](const auto & column_name)
        {
            return read_column_names.contains(getColumnNameInStorage(column_name, storage_columns));
        });

        if (!read_any_required_column)
            continue;

        auto index_ptr = MergeTreeIndexFactory::instance().get(index);

        /// Rebuild index if merge may reduce rows because we cannot adjust parts offsets in that case.
        /// Build index if it is not materialized in the data part.
        if (global_ctx->merge_may_reduce_rows || !index_ptr->getDeserializedFormat(data_part.checksums, index_ptr->getFileName()))
        {
            description_to_build.push_back(index);
            indexes_to_build.push_back(std::move(index_ptr));
        }
    }

    if (indexes_to_build.empty())
        return;

    if (!global_ctx->temporary_text_index_storage)
    {
        auto new_part_path = global_ctx->new_data_part->getDataPartStorage().getRelativePath();
        global_ctx->temporary_text_index_storage = createTemporaryTextIndexStorage(global_ctx->disk, new_part_path);
    }

    addSkipIndexesExpressionSteps(plan, description_to_build, global_ctx);

    MergeTreeWriterSettings writer_settings(
        global_ctx->data->getContext()->getSettingsRef(),
        global_ctx->context->getWriteSettings(),
        global_ctx->data->getSettings(),
        global_ctx->new_data_part,
        global_ctx->new_data_part->index_granularity_info.mark_type.adaptive,
        /*rewrite_primary_key=*/ false,
        /*save_marks_in_cache=*/ false,
        /*save_primary_index_in_memory=*/ false,
        /*blocks_are_granules_size=*/ false);

    auto transform = std::make_shared<BuildTextIndexTransform>(
        plan.getCurrentHeader(),
        "tmp_" + data_part.name,
        std::move(indexes_to_build),
        global_ctx->temporary_text_index_storage,
        std::move(writer_settings),
        global_ctx->compression_codec,
        global_ctx->new_data_part->index_granularity_info.mark_type.getFileExtension());

    /// Pass original header as output header to remove temporary columns added by the transform.
    /// This is important to make this part's plan compatible with other parts' plans that don't materialize indexes.
    auto build_text_index_step = std::make_unique<BuildTextIndexStep>(plan.getCurrentHeader(), original_header, transform, global_ctx->context);

    /// Save transform to the context to be able to take segments for merging from it later.
    global_ctx->build_text_index_transforms[data_part.name].push_back(std::move(transform));
    plan.addStep(std::move(build_text_index_step));
}

BuildStatisticsTransformPtr MergeTask::addBuildStatisticsStep(QueryPlan & plan, const IMergeTreeDataPart & data_part, const GlobalRuntimeContextPtr & global_ctx)
{
    auto it = global_ctx->statistics_to_build_by_part.find(data_part.name);
    if (it == global_ctx->statistics_to_build_by_part.end() || it->second.empty())
        return nullptr;

    auto read_column_names = plan.getCurrentHeader()->getNameSet();
    ColumnsStatistics statistics_to_build;

    for (const auto & [column_name, column_stats] : it->second)
    {
        if (read_column_names.contains(column_name))
            statistics_to_build.emplace(column_name, column_stats->cloneEmpty());
    }

    if (statistics_to_build.empty())
        return nullptr;

    auto transform = std::make_shared<BuildStatisticsTransform>(
        plan.getCurrentHeader(),
        std::move(statistics_to_build));

    auto build_statistics_step = std::make_unique<BuildStatisticsStep>(plan.getCurrentHeader(), transform);
    plan.addStep(std::move(build_statistics_step));
    return transform;
}

void MergeTask::mergeBuiltStatistics(BuildStatisticsTransformMap && build_statistics_transforms, const GlobalRuntimeContextPtr & global_ctx)
{
    for (const auto & [name, transform] : build_statistics_transforms)
    {
        const auto & built_statistics = transform->getStatistics();
        global_ctx->gathered_data.statistics.merge(built_statistics);
    }
}

void MergeTask::ExecuteAndFinalizeHorizontalPart::createMergedStream() const
{
    /** Read from all parts, merge and write into a new one.
      * In passing, we calculate expression for sorting.
      */

    global_ctx->watch_prev_elapsed = 0;

    /// We count total amount of bytes in parts
    /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
    ctx->read_with_direct_io = false;
    const auto & merge_tree_settings = global_ctx->data_settings;
    if ((*merge_tree_settings)[MergeTreeSetting::min_merge_bytes_to_use_direct_io] != 0)
    {
        size_t total_size = 0;
        for (const auto & part : global_ctx->future_part->parts)
        {
            total_size += part->getBytesOnDisk();
            if (total_size >= (*merge_tree_settings)[MergeTreeSetting::min_merge_bytes_to_use_direct_io])
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

    Names merging_column_names = global_ctx->merging_columns.getNames();
    for (const auto * projection : global_ctx->projections_to_merge)
    {
        /// If projection needs part offset mapping, add _part_index column to build this mapping
        if (projection->with_parent_part_offset)
        {
            if (global_ctx->metadata_snapshot->hasSortingKey())
            {
                chassert(global_ctx->merged_part_offsets == nullptr);
                chassert(std::find(merging_column_names.begin(), merging_column_names.end(), "_part_index") == merging_column_names.end());
                global_ctx->merged_part_offsets
                    = std::make_shared<MergedPartOffsets>(global_ctx->future_part->parts.size(), MergedPartOffsets::MappingMode::Enabled);
                merging_column_names.push_back("_part_index");
            }
            else
            {
                global_ctx->merged_part_offsets
                    = std::make_shared<MergedPartOffsets>(global_ctx->future_part->parts.size(), MergedPartOffsets::MappingMode::Disabled);
            }
            break;
        }
    }

    if (!global_ctx->merge_may_reduce_rows
        && !global_ctx->text_indexes_to_merge.empty()
        && (!global_ctx->merged_part_offsets || !global_ctx->merged_part_offsets->isMappingEnabled()))
    {
        global_ctx->merged_part_offsets = std::make_shared<MergedPartOffsets>(global_ctx->future_part->parts.size(), MergedPartOffsets::MappingMode::Enabled);
        merging_column_names.push_back("_part_index");
    }

    global_ctx->vertical_lightweight_delete = isVerticalLightweightDelete(*global_ctx);
    /// Do not apply mask for lightweight delete in vertical merge, because it is applied in merging algorithm
    bool apply_deleted_mask = !global_ctx->vertical_lightweight_delete;

    if (global_ctx->vertical_lightweight_delete)
    {
        merging_column_names.push_back(RowExistsColumn::name);
    }

    /// Read from all parts
    std::vector<QueryPlanPtr> plans;
    size_t part_starting_offset = 0;
    for (size_t i = 0; i < global_ctx->future_part->parts.size(); ++i)
    {
        auto plan_for_part = std::make_unique<QueryPlan>();
        const auto & part = global_ctx->future_part->parts[i];

        if (part->getMarksCount() == 0)
            LOG_TRACE(ctx->log, "Part {} is empty", part->name);

        createReadFromPartStep(
            MergeTreeSequentialSourceType::Merge,
            *plan_for_part,
            *global_ctx->data,
            global_ctx->storage_snapshot,
            RangesInDataPart(part, nullptr, i, part_starting_offset),
            global_ctx->alter_conversions[i],
            global_ctx->merged_part_offsets,
            merging_column_names,
            global_ctx->input_rows_filtered,
            apply_deleted_mask,
            /*filter=*/ std::nullopt,
            ctx->read_with_direct_io,
            /*prefetch=*/ false,
            global_ctx->context,
            ctx->log);

        /// Add step for building missed text indexes and statistics for single parts.
        /// If merge may reduce rows, we will rebuild index and statistics
        /// for the resulting part in the end of the pipeline.
        if (!global_ctx->merge_may_reduce_rows)
        {
            addBuildTextIndexesStep(*plan_for_part, *part, global_ctx);

            if (auto transform = addBuildStatisticsStep(*plan_for_part, *part, global_ctx))
                ctx->build_statistics_transforms.emplace(part->name, std::move(transform));
        }

        plans.emplace_back(std::move(plan_for_part));
        part_starting_offset += global_ctx->future_part->parts[i]->rows_count;
    }

    QueryPlan merge_parts_query_plan;

    /// Union of all parts streams
    {
        SharedHeaders input_headers;
        input_headers.reserve(plans.size());
        for (auto & plan : plans)
            input_headers.emplace_back(plan->getCurrentHeader());

        auto union_step = std::make_unique<UnionStep>(std::move(input_headers));
        merge_parts_query_plan.unitePlans(std::move(union_step), std::move(plans));
    }

    if (global_ctx->metadata_snapshot->hasSortingKey())
    {
        /// Calculate sorting key expressions so that they are available for merge sorting.
        const auto & sorting_key_expression = global_ctx->metadata_snapshot->getSortingKey().expression;
        auto sorting_key_expression_dag = sorting_key_expression->getActionsDAG().clone();
        auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(*merge_parts_query_plan.getCurrentHeader(), sorting_key_expression_dag.getRequiredColumnsNames(), global_ctx->data->getContext());
        auto calculate_sorting_key_expression_step = std::make_unique<ExpressionStep>(
            merge_parts_query_plan.getCurrentHeader(),
            ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(sorting_key_expression_dag)));
        merge_parts_query_plan.addStep(std::move(calculate_sorting_key_expression_step));
    }

    SortDescription sort_description;
    /// Merge
    {
        Names sort_columns = global_ctx->metadata_snapshot->getSortingKeyColumns();
        std::vector<bool> reverse_flags = global_ctx->metadata_snapshot->getSortingKeyReverseFlags();
        sort_description.compile_sort_description = global_ctx->data->getContext()->getSettingsRef()[Setting::compile_sort_description];
        sort_description.min_count_to_compile_sort_description = global_ctx->data->getContext()->getSettingsRef()[Setting::min_count_to_compile_sort_description];

        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);

        auto partition_and_sorting_required_columns = global_ctx->metadata_snapshot->getPartitionKey().expression->getRequiredColumns();
        partition_and_sorting_required_columns.append_range(global_ctx->metadata_snapshot->getSortingKey().expression->getRequiredColumns());

        for (size_t i = 0; i < sort_columns_size; ++i)
        {
            if (!reverse_flags.empty() && reverse_flags[i])
                sort_description.emplace_back(sort_columns[i], -1, 1);
            else
                sort_description.emplace_back(sort_columns[i], 1, 1);
        }

        const bool is_vertical_merge = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Vertical);

        if (global_ctx->cleanup && !(*merge_tree_settings)[MergeTreeSetting::allow_experimental_replacing_merge_with_cleanup])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Experimental merges with CLEANUP are not allowed");

        bool cleanup = global_ctx->cleanup && global_ctx->future_part->final;
        std::optional<String> filter_column_name = global_ctx->vertical_lightweight_delete ? std::make_optional(RowExistsColumn::name) : std::nullopt;

        std::optional<size_t> max_dynamic_subcolumns = std::nullopt;
        if (global_ctx->future_part->part_format.part_type == MergeTreeDataPartType::Wide)
            max_dynamic_subcolumns = (*merge_tree_settings)[MergeTreeSetting::merge_max_dynamic_subcolumns_in_wide_part].valueOrNullopt();
        else if (global_ctx->future_part->part_format.part_type == MergeTreeDataPartType::Compact)
            max_dynamic_subcolumns = (*merge_tree_settings)[MergeTreeSetting::merge_max_dynamic_subcolumns_in_compact_part].valueOrNullopt();

        auto merge_step = std::make_unique<MergePartsStep>(
            merge_parts_query_plan.getCurrentHeader(),
            sort_description,
            partition_and_sorting_required_columns,
            global_ctx->merging_params,
            (is_vertical_merge ? RowsSourcesTemporaryFile::FILE_ID : ""), /// rows_sources' temporary file is used only for vertical merge
            filter_column_name,
            (*merge_tree_settings)[MergeTreeSetting::merge_max_block_size],
            (*merge_tree_settings)[MergeTreeSetting::merge_max_block_size_bytes],
            max_dynamic_subcolumns,
            is_vertical_merge,
            cleanup,
            global_ctx->time_of_merge);

        merge_step->setStepDescription("Merge sorted parts");
        merge_parts_query_plan.addStep(std::move(merge_step));
    }

    if (global_ctx->deduplicate)
    {
        const auto & virtuals = *global_ctx->data->getVirtualsPtr();

        /// We don't want to deduplicate by virtual persistent column.
        /// If deduplicate_by_columns is empty, add all columns except virtuals.
        if (global_ctx->deduplicate_by_columns.empty())
        {
            for (const auto & column : global_ctx->merging_columns)
            {
                if (virtuals.tryGet(column.name, VirtualsKind::Persistent))
                    continue;

                global_ctx->deduplicate_by_columns.emplace_back(column.name);
            }
        }

        auto deduplication_step = std::make_unique<DistinctStep>(
            merge_parts_query_plan.getCurrentHeader(),
            SizeLimits(), 0 /*limit_hint*/,
            global_ctx->deduplicate_by_columns,
            false /*pre_distinct*/);

        deduplication_step->setStepDescription("Deduplication step");
        deduplication_step->applyOrder(sort_description); // Distinct-in-order.
        merge_parts_query_plan.addStep(std::move(deduplication_step));
    }

    PreparedSets::Subqueries subqueries;

    /// TTL step
    if (ctx->need_remove_expired_values || !global_ctx->merging_columns_expired_by_ttl.empty())
    {
        auto ttl_step = std::make_unique<TTLStep>(
            merge_parts_query_plan.getCurrentHeader(),
            global_ctx->context,
            *global_ctx->data,
            global_ctx->metadata_snapshot,
            global_ctx->new_data_part,
            global_ctx->merging_columns_expired_by_ttl,
            global_ctx->time_of_merge,
            ctx->force_ttl);

        subqueries = ttl_step->getSubqueries();
        ttl_step->setStepDescription("TTL step");
        merge_parts_query_plan.addStep(std::move(ttl_step));
    }

    /// Secondary indices expressions
    if (!global_ctx->merging_skip_indexes.empty())
        addSkipIndexesExpressionSteps(merge_parts_query_plan, global_ctx->merging_skip_indexes, global_ctx);

    if (!subqueries.empty())
        addCreatingSetsStep(merge_parts_query_plan, std::move(subqueries), global_ctx->context);

    /// If merge may reduce rows, rebuild text index and statistics for the resulting part.
    if (global_ctx->merge_may_reduce_rows)
    {
        addBuildTextIndexesStep(merge_parts_query_plan, *global_ctx->new_data_part, global_ctx);

        if (auto transform = addBuildStatisticsStep(merge_parts_query_plan, *global_ctx->new_data_part, global_ctx))
            ctx->build_statistics_transforms.emplace(global_ctx->new_data_part->name, std::move(transform));
    }

    {
        QueryPlanOptimizationSettings optimization_settings(global_ctx->context);
        auto pipeline_settings = BuildQueryPipelineSettings(global_ctx->context);
        pipeline_settings.temporary_file_lookup = ctx->rows_sources_temporary_file;
        auto builder = merge_parts_query_plan.buildQueryPipeline(optimization_settings, pipeline_settings);

        // Merges are not using concurrency control now. Queries and merges running together could lead to CPU overcommit.
        // TODO(serxa): Enable concurrency control for merges. This should be done after CPU scheduler introduction.
        builder->setConcurrencyControl(false);

        global_ctx->merged_pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    }

    /// Dereference unique_ptr and pass horizontal_stage_progress by reference
    global_ctx->merged_pipeline.setProgressCallback(MergeProgressCallback(
        global_ctx->merge_list_element_ptr,
        global_ctx->watch_prev_elapsed,
        *global_ctx->horizontal_stage_progress,
        [&my_ctx = *global_ctx]() { my_ctx.checkOperationIsNotCanceled(); }));

    /// Is calculated inside MergeProgressCallback.
    global_ctx->merged_pipeline.disableProfileEventUpdate();
    global_ctx->merging_executor = std::make_unique<PullingPipelineExecutor>(global_ctx->merged_pipeline);
}


MergeAlgorithm MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm() const
{
    const size_t total_rows_count = global_ctx->merge_list_element_ptr->total_rows_count;
    const size_t total_size_bytes_uncompressed = global_ctx->merge_list_element_ptr->total_size_bytes_uncompressed;
    const auto & merge_tree_settings = global_ctx->data_settings;

    if (global_ctx->deduplicate)
        return MergeAlgorithm::Horizontal;
    if ((*merge_tree_settings)[MergeTreeSetting::enable_vertical_merge_algorithm] == 0)
        return MergeAlgorithm::Horizontal;
    if (ctx->need_remove_expired_values)
        return MergeAlgorithm::Horizontal;
    if (global_ctx->future_part->part_format.part_type != MergeTreeDataPartType::Wide)
        return MergeAlgorithm::Horizontal;
    if (global_ctx->future_part->part_format.storage_type != MergeTreeDataPartStorageType::Full)
        return MergeAlgorithm::Horizontal;
    if (global_ctx->cleanup)
        return MergeAlgorithm::Horizontal;

    if (!(*merge_tree_settings)[MergeTreeSetting::allow_vertical_merges_from_compact_to_wide_parts])
    {
        for (const auto & part : global_ctx->future_part->parts)
        {
            if (!isWidePart(part))
                return MergeAlgorithm::Horizontal;
        }
    }

    bool is_supported_storage =
        global_ctx->merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        global_ctx->merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
        global_ctx->merging_params.mode == MergeTreeData::MergingParams::Replacing ||
        global_ctx->merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

    bool enough_ordinary_cols = global_ctx->gathering_columns.size() >= (*merge_tree_settings)[MergeTreeSetting::vertical_merge_algorithm_min_columns_to_activate];

    bool enough_total_rows = total_rows_count >= (*merge_tree_settings)[MergeTreeSetting::vertical_merge_algorithm_min_rows_to_activate];

    bool enough_total_bytes = total_size_bytes_uncompressed >= (*merge_tree_settings)[MergeTreeSetting::vertical_merge_algorithm_min_bytes_to_activate];

    bool no_parts_overflow = global_ctx->future_part->parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_total_bytes && enough_ordinary_cols && no_parts_overflow) ?
                        MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    return merge_alg;
}

}
