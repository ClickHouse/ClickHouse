#include <Storages/MergeTree/ExportPartFromPartitionExportTask.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event ExportPartitionZooKeeperRequests;
    extern const Event ExportPartitionZooKeeperGetChildren;
    extern const Event ExportPartitionZooKeeperCreate;
}
namespace DB
{

ExportPartFromPartitionExportTask::ExportPartFromPartitionExportTask(
    StorageReplicatedMergeTree & storage_,
    const std::string & key_,
    const MergeTreePartExportManifest & manifest_)
    : storage(storage_),
    key(key_),
    manifest(manifest_)
{
    export_part_task = std::make_shared<ExportPartTask>(storage, manifest);
}

bool ExportPartFromPartitionExportTask::executeStep()
{
    const auto zk = storage.getZooKeeper();
    const auto part_name = manifest.data_part->name;

    LOG_INFO(storage.log, "ExportPartFromPartitionExportTask: Attempting to lock part: {}", part_name);

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperCreate);
    if (Coordination::Error::ZOK == zk->tryCreate(fs::path(storage.zookeeper_path) / "exports" / key / "locks" / part_name, storage.replica_name, zkutil::CreateMode::Ephemeral))
    {
        LOG_INFO(storage.log, "ExportPartFromPartitionExportTask: Locked part: {}", part_name);
        export_part_task->executeStep();
        return false;
    }

    std::lock_guard inner_lock(storage.export_manifests_mutex);
    storage.export_manifests.erase(manifest);

    LOG_INFO(storage.log, "ExportPartFromPartitionExportTask: Failed to lock part {}, skipping", part_name);
    return false;
}

void ExportPartFromPartitionExportTask::cancel() noexcept
{
    export_part_task->cancel();
}

void ExportPartFromPartitionExportTask::onCompleted()
{
    export_part_task->onCompleted();
}

StorageID ExportPartFromPartitionExportTask::getStorageID() const
{
    return export_part_task->getStorageID();
}

Priority ExportPartFromPartitionExportTask::getPriority() const
{
    return export_part_task->getPriority();
}

String ExportPartFromPartitionExportTask::getQueryId() const
{
    return export_part_task->getQueryId();
}
}
