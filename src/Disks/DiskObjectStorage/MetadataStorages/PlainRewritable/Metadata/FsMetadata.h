#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <Common/CurrentMetrics.h>
#include <Common/MultiVersion.h>

#include <base/defines.h>

namespace DB
{

class BlobLinkCounts;

/// The committed state of the plain-rewritable metadata: the tree of directories and files, and the numbers of links to the blobs.
class FsMetadata
{
public:
    FsMetadata(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name);

    /// Atomically publishes a new version of the tree: the latest one with the journal replayed on top of it.
    /// Nothing is published if the replay throws. Returns the keys of the blobs that have lost their last link: this happens
    /// when concurrent transactions remove different links to the same blob, and each of them sees another link left.
    std::vector<std::string> applyJournal(const FsJournal & journal);
    /// Publishes the loaded tree together with the remap of the blobs of the pending replacements (see `FsSnapshot`).
    void applyLayout(
        std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout,
        std::shared_ptr<const BlobObjectKeyRemap> backups_of_pending_replace_targets = nullptr);

    std::shared_ptr<FsSnapshot> takeReadWriteSnapshot() const;
    std::shared_ptr<const FsSnapshot> takeReadOnlySnapshot() const;

private:
    mutable std::mutex mutex;
    const std::shared_ptr<BlobLinkCounts> blob_link_counts;
    std::shared_ptr<FsSnapshot> latest_snapshot TSA_GUARDED_BY(mutex);
    mutable CurrentMetrics::Increment remote_layout_directories_count TSA_GUARDED_BY(mutex);
    mutable CurrentMetrics::Increment remote_layout_files_count TSA_GUARDED_BY(mutex);
};

}
