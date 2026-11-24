#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/BlobLinkCounts.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <Common/UniqueLock.h>

namespace DB
{

FsMetadata::FsMetadata(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name)
    : blob_link_counts(std::make_shared<BlobLinkCounts>())
    , latest_snapshot(std::make_shared<FsSnapshot>(blob_link_counts))
    , remote_layout_directories_count(metric_directories_name, 0)
    , remote_layout_files_count(metric_files_name, 0)
{
}

std::vector<std::string> FsMetadata::applyJournal(const FsJournal & journal)
{
    UniqueLock lock(mutex);

    /// The nodes of the latest snapshot are never mutated in place, so readers holding it are not affected.
    auto new_snapshot = std::make_shared<FsSnapshot>(latest_snapshot->getRoot(), blob_link_counts);
    new_snapshot->setBackupsOfPendingReplaceTargets(latest_snapshot->getBackupsOfPendingReplaceTargets());
    new_snapshot->replay(journal);

    const auto [directories_delta, files_delta] = new_snapshot->getRemoteLayoutDeltas();
    auto unlinked_blob_keys = blob_link_counts->apply(new_snapshot->getBlobLinkDeltas());
    /// The deltas are now part of the committed counts; drop them so reads through the committed snapshot do not re-apply them.
    new_snapshot->resetDeltas();
    latest_snapshot = std::move(new_snapshot);
    remote_layout_directories_count.add(directories_delta);
    remote_layout_files_count.add(files_delta);
    return unlinked_blob_keys;
}

void FsMetadata::applyLayout(
    std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout,
    std::shared_ptr<const BlobObjectKeyRemap> backups_of_pending_replace_targets)
{
    std::unordered_map<std::string, uint32_t> new_blob_link_counts;
    for (const auto & [path, info] : remote_layout)
        for (const auto & [file_name, file_info] : info.files)
            ++new_blob_link_counts[getBlobKey(info, file_name, file_info)];

    auto new_tree = std::make_shared<FsSnapshot>(blob_link_counts);
    for (auto & [path, info] : remote_layout)
        new_tree->recordDirectoryPath(path, std::move(info));
    new_tree->setBackupsOfPendingReplaceTargets(std::move(backups_of_pending_replace_targets));

    const auto [directories_count, files_count] = new_tree->getRemoteLayoutDeltas();

    UniqueLock lock(mutex);
    blob_link_counts->replace(new_blob_link_counts);
    latest_snapshot = std::move(new_tree);
    remote_layout_directories_count.changeTo(directories_count);
    remote_layout_files_count.changeTo(files_count);
}

std::shared_ptr<FsSnapshot> FsMetadata::takeReadWriteSnapshot() const
{
    UniqueLock lock(mutex);
    return std::make_shared<FsSnapshot>(latest_snapshot->getRoot(), blob_link_counts);
}

std::shared_ptr<const FsSnapshot> FsMetadata::takeReadOnlySnapshot() const
{
    UniqueLock lock(mutex);
    return latest_snapshot;
}

}
