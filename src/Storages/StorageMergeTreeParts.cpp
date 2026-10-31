#include <Storages/StorageMergeTreeParts.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeReadPoolStatelessParts.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool use_uncompressed_cache;
    extern const SettingsBool merge_tree_use_const_size_tasks_for_remote_reading;
    extern const SettingsUInt64 merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem;
    extern const SettingsUInt64 merge_tree_min_rows_for_concurrent_read_for_remote_filesystem;
    extern const SettingsNonZeroUInt64 merge_tree_min_read_task_size;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
}

MergeTreeSettingsPtr StorageMergeTreeParts::ReadFromPartsInfo::buildStorageSettings() const
{
    auto settings = std::make_shared<MergeTreeSettings>();
    settings->set("index_granularity", Field(index_granularity));
    settings->set("index_granularity_bytes", Field(index_granularity_bytes));
    settings->set("share_nested_offsets", Field(share_nested_offsets));
    return settings;
}

VirtualColumnsDescription StorageMergeTreeParts::createVirtuals()
{
    VirtualColumnsDescription desc;
    /// Register the same persistent virtual columns as MergeTreeData so that
    /// StorageSnapshot::check accepts them when the query explicitly selects e.g.
    /// _block_number or _block_offset.  These columns are physically stored in
    /// parts (enable_block_number_column / enable_block_offset_column) and live in
    /// VirtualColumnsDescription rather than in ColumnsDescription.
    desc.addPersistent(
        RowExistsColumn::name, RowExistsColumn::type, nullptr,
        "Persisted mask created by lightweight delete that show whether row exists or is deleted");
    desc.addPersistent(
        BlockNumberColumn::name, BlockNumberColumn::type, BlockNumberColumn::codec,
        "Persisted original number of block that was assigned at insert");
    desc.addPersistent(
        BlockOffsetColumn::name, BlockOffsetColumn::type, BlockOffsetColumn::codec,
        "Persisted original number of row in block that was assigned at insert");
    return desc;
}

StorageMergeTreeParts::StorageMergeTreeParts(
    const ReadFromPartsInfo & read_from_parts_info_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , read_from_parts_info(read_from_parts_info_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageMergeTreeParts::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t num_streams)
{
    /// A column-less read is okay, e.g. `SELECT count()`.
    if (!column_names.empty())
        storage_snapshot->check(column_names);

    if (read_from_parts_info.parts.empty())
        return {};

    const auto & settings = context_->getSettingsRef();
    auto reader_settings = MergeTreeReaderSettings::createFromContext(context_);
    auto actions_settings = ExpressionActionsSettings(context_);

    size_t sum_marks = 0;
    for (const auto & part : read_from_parts_info.parts)
        sum_marks += part.ranges.getNumberOfMarks();

    if (!sum_marks)
        return {};

    /// Concurrency is sized in marks, and a mark is as big as the granularity of the table that wrote
    /// the parts, which only `table_settings(...)` knows.
    const auto storage_settings = read_from_parts_info.buildStorageSettings();

    /// The remote-filesystem thresholds, because the point of reading parts this way is to read them
    /// from a disk that does not belong to this server.
    const size_t min_marks_for_concurrent_read = MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
        settings[Setting::merge_tree_min_rows_for_concurrent_read_for_remote_filesystem],
        settings[Setting::merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem],
        (*storage_settings)[MergeTreeSetting::index_granularity],
        (*storage_settings)[MergeTreeSetting::index_granularity_bytes],
        settings[Setting::merge_tree_min_read_task_size],
        sum_marks);

    /// Cap the streams by the amount of work, not by the number of parts: the pool splits each part's
    /// mark ranges across threads, so a single big part must still fan out.
    num_streams = std::min(num_streams, (sum_marks + min_marks_for_concurrent_read - 1) / min_marks_for_concurrent_read);
    num_streams = std::max<size_t>(num_streams, 1);

    /// The block size predictor takes a concrete part, which a borrowed part never has, so byte-based
    /// block sizing stays off and reads come in row-count batches of `max_block_size`.
    MergeTreeReadPoolBase::PoolSettings pool_settings{
        .threads = num_streams,
        .sum_marks = sum_marks,
        .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        .preferred_block_size_bytes = 0,
        .use_uncompressed_cache = settings[Setting::use_uncompressed_cache],
        .do_not_steal_tasks = false,
        .use_const_size_tasks_for_remote_reading = settings[Setting::merge_tree_use_const_size_tasks_for_remote_reading],
        .total_query_nodes = 1,
    };

    MergeTreeReadTask::BlockSizeParams block_size_params{
        .max_block_size_rows = max_block_size,
        .preferred_block_size_bytes = 0,
    };

    auto pool = std::make_shared<MergeTreeReadPoolStatelessParts>(
        read_from_parts_info,
        storage_snapshot,
        query_info.row_level_filter,
        query_info.prewhere_info,
        actions_settings,
        reader_settings,
        column_names,
        pool_settings,
        block_size_params,
        context_);

    Pipes pipes;
    pipes.reserve(num_streams);
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(i);
        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool,
            std::move(algorithm),
            query_info.row_level_filter,
            query_info.prewhere_info,
            IndexReadTasks{},
            actions_settings,
            reader_settings,
            /*merge_tree_index_build_context=*/ nullptr,
            /*lazy_materializing_rows=*/ nullptr,
            &storage_snapshot->metadata->getColumns());
        auto source = std::make_shared<MergeTreeSource>(std::move(processor), getName());
        pipes.emplace_back(std::move(source));
    }

    return Pipe::unitePipes(std::move(pipes));
}

}
