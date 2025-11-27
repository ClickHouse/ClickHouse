#include <Processors/QueryPlan/LazyReadFromMergeTree.h>
#include <Processors/Sources/LazyReadFromMergeTreeSource.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsBool merge_tree_use_const_size_tasks_for_remote_reading;
    extern const SettingsBool use_uncompressed_cache;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LazyReadFromMergeTree::LazyReadFromMergeTree(
    SharedHeader header,
    size_t max_block_size_,
    MergeTreeReaderSettings reader_settings_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    const std::string & log_name_)
    : ISourceStep(std::move(header))
    , max_block_size(max_block_size_)
    , reader_settings(reader_settings_)
    , mutations_snapshot(std::move(mutations_snapshot_))
    , storage_snapshot(std::move(storage_snapshot_))
    , context(std::move(context_))
    , log_name(log_name_)
{
}

void LazyReadFromMergeTree::setLazyMaterializingRows(LazyMaterializingRowsPtr lazy_materializing_rows_)
{
    lazy_materializing_rows = std::move(lazy_materializing_rows_);
}

void LazyReadFromMergeTree::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazyReadFromMergeTree: lazy_materializing_rows is not set");

    auto source = std::make_shared<LazyReadFromMergeTreeSource>(
        getOutputHeader(),
        max_block_size,
        settings.max_threads,
        settings.actions_settings,
        reader_settings,
        mutations_snapshot,
        storage_snapshot,
        context,
        log_name,
        lazy_materializing_rows
    );

    Pipe pipe(std::move(source));
    pipeline.init(std::move(pipe));
}

}
