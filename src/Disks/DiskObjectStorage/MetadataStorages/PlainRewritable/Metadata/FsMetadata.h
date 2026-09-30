#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <Common/CurrentMetrics.h>
#include <Common/MultiVersion.h>

#include <base/defines.h>

namespace DB
{

class FsMetadata
{
public:
    FsMetadata(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name);

    void applySnapshot(std::shared_ptr<FsSnapshot> snapshot);
    void applyLayout(std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout);

    std::shared_ptr<FsSnapshot> takeReadWriteSnapshot() const;
    std::shared_ptr<const FsSnapshot> takeReadOnlySnapshot() const;

private:
    mutable std::mutex mutex;
    std::shared_ptr<FsSnapshot> latest_snapshot TSA_GUARDED_BY(mutex);
    mutable CurrentMetrics::Increment remote_layout_directories_count TSA_GUARDED_BY(mutex);
    mutable CurrentMetrics::Increment remote_layout_files_count TSA_GUARDED_BY(mutex);
};

}
