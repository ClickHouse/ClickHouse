#include <Storages/MergeTree/MergeTreeReadPoolProjectionIndex.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReadPoolProjectionIndex::MergeTreeReadPoolProjectionIndex(
    MutationsSnapshotPtr mutations_snapshot_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const PoolSettings & settings_,
    const MergeTreeReadTask::BlockSizeParams & params_,
    const ContextPtr & context_)
    : MergeTreeReadPoolBase(
          std::move(mutations_snapshot_),
          storage_snapshot_,
          prewhere_info_,
          actions_settings_,
          reader_settings_,
          column_names_,
          settings_,
          params_,
          context_)
{
}

MergeTreeReadTaskPtr MergeTreeReadPoolProjectionIndex::getTask(size_t task_idx, MergeTreeReadTask * /* previous_task */)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Requested task with idx {}, which is not supported in read pool MergeTreeReadPoolProjectionIndex",
        task_idx);
}

MergeTreeReadTaskPtr MergeTreeReadPoolProjectionIndex::getTask(const RangesInDataPart & part)
{
    MergeTreeReadTaskInfo read_task_info = buildReadTaskInfo(part, getContext()->getSettingsRef());
    return createTask(
        std::make_shared<MergeTreeReadTaskInfo>(std::move(read_task_info)), part.ranges, nullptr /* previous_task */);
}

}
