#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <Common/UniqueLock.h>

namespace DB
{

FsMetadata::FsMetadata(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name)
    : latest_snapshot(std::make_shared<FsSnapshot>())
    , remote_layout_directories_count(metric_directories_name, 0)
    , remote_layout_files_count(metric_files_name, 0)
{
}

void FsMetadata::applySnapshot(std::shared_ptr<FsSnapshot> snapshot)
{
    const auto [directories_delta, files_delta] = snapshot->getRemoteLayoutDeltas();

    UniqueLock lock(mutex);
    latest_snapshot = std::move(snapshot);
    remote_layout_directories_count.add(directories_delta);
    remote_layout_files_count.add(files_delta);
}

void FsMetadata::applyLayout(std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout)
{
    auto new_tree = std::make_shared<FsSnapshot>();
    for (auto & [path, info] : remote_layout)
        new_tree->recordDirectoryPath(path, std::move(info));

    const auto [directories_count, files_count] = new_tree->getRemoteLayoutDeltas();

    UniqueLock lock(mutex);
    latest_snapshot = std::move(new_tree);
    remote_layout_directories_count.changeTo(directories_count);
    remote_layout_files_count.changeTo(files_count);
}

std::shared_ptr<FsSnapshot> FsMetadata::takeReadWriteSnapshot() const
{
    UniqueLock lock(mutex);
    return std::make_shared<FsSnapshot>(latest_snapshot->getRoot());
}

std::shared_ptr<const FsSnapshot> FsMetadata::takeReadOnlySnapshot() const
{
    UniqueLock lock(mutex);
    return latest_snapshot;
}

}
