#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>

#include <Common/CurrentMetrics.h>

#include <base/defines.h>

#include <unordered_map>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <mutex>

namespace DB
{

struct FileRemoteInfo
{
    size_t bytes_size;
    time_t last_modified;
};

struct DirectoryRemoteInfo
{
    std::string remote_path;
    std::string etag;
    time_t last_modified = 0;
    std::unordered_map<std::string, FileRemoteInfo> files;
};

struct FsNode : public std::enable_shared_from_this<FsNode>
{
    std::optional<DirectoryRemoteInfo> info = {};
    std::unordered_map<std::string, std::shared_ptr<FsNode>> subdirectories = {};
};

/// Mutable snapshot of the virtual file system tree.
class FsSnapshot
{
public:
    explicit FsSnapshot();
    explicit FsSnapshot(std::shared_ptr<FsNode> root_);

    /// Directory Write Methods

    void recordDirectoryPath(const std::string & path, DirectoryRemoteInfo info);
    void moveDirectory(const std::string & from, const std::string & to);
    void removeDirectory(const std::string & path);

    /// File Write Methods

    void recordFile(const std::string & path, FileRemoteInfo info);
    void removeFile(const std::string & path);

    /// Directory Read Methods

    std::vector<std::string> listDirectory(const std::string & path) const;
    bool existsDirectory(const std::string & path) const;
    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> getSubtreeRemoteInfo(const std::string & path) const;
    std::optional<DirectoryRemoteInfo> getDirectoryRemoteInfo(const std::string & path) const;

    /// File Read Methods

    std::optional<FileRemoteInfo> getFileRemoteInfo(const std::string & path) const;
    bool existsFile(const std::string & path) const;

    /// Snapshot Methods

    std::shared_ptr<FsNode> getRoot() const;
    void setRoot(std::shared_ptr<FsNode> new_root);
    std::pair<int64_t, int64_t> getRemoteLayoutDeltas() const;

private:
    mutable std::mutex mutex;
    std::shared_ptr<FsNode> root TSA_GUARDED_BY(mutex);
    mutable int64_t remote_layout_directories_delta TSA_GUARDED_BY(mutex) = 0;
    mutable int64_t remote_layout_files_delta TSA_GUARDED_BY(mutex) = 0;
};

}
