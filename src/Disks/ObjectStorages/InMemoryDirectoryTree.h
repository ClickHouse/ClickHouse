#pragma once

#include <Common/CurrentMetrics.h>

#include <base/defines.h>

#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <filesystem>

namespace DB
{

struct DirectoryRemoteInfo
{
    std::string remote_path;
    std::string etag;
    time_t last_modified = 0;
    std::unordered_set<std::string> file_names;
};

/// Maintains virtual file system tree of directories. Files are not included into
/// the tree and contain in tree nodes as values. Nodes can be virtual and physical.
/// Physical node will have remote info - that means it was written into object storage.
/// For virtual nodes some restrictions are applied:
///     - No underlying files
///     - Can not be moved/removed
class InMemoryDirectoryTree
{
    struct INode;

    /// Resolved inode starting from the root reachable by path.
    std::shared_ptr<INode> walk(const std::filesystem::path & path, bool create_missing = false) const TSA_REQUIRES(mutex);

    /// For each inode in path subtree (including the inode related to path) will call observe.
    /// Order of traverse is not guaranteed.
    using ObserveFunction = std::function<void(const std::string & /*path*/, const std::shared_ptr<INode> & /*inode*/)>;
    void traverseSubtree(const std::filesystem::path & path, const ObserveFunction & observe) const TSA_REQUIRES(mutex);

    /// Constructs path which can be resolved (walked) to node.
    std::filesystem::path determineNodePath(std::shared_ptr<INode> node) const TSA_REQUIRES(mutex);

    /// Removes all parents of node while the parent should not exist. Node should exist if some physical node exists in it's subtree.
    std::shared_ptr<INode> trimDanglingVirtualPath(std::shared_ptr<INode> node) TSA_REQUIRES(mutex);

public:
    InMemoryDirectoryTree(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name);
    void apply(std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout);

    /// Returns (logical path, remote info) if directory did not change since last tree update.
    std::optional<std::pair<std::string, DirectoryRemoteInfo>> lookupDirectoryIfNotChanged(const std::string & remote_path, const std::string & etag) const;

    /// For each node in path subtree (including the inode related to path) will return related remote info.
    /// NOTE: If node is virtual it still will be in map but remote info will be nullopt.
    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> getSubtreeRemoteInfo(const std::string & path) const;

    /// Creates virtual path in tree according to the path. Only leaf of this path will be physical (will have remote info).
    void recordDirectoryPath(const std::string & path, DirectoryRemoteInfo info);

    void unlinkTree(const std::string & path);

    /// Normal File System Methods
    void moveDirectory(const std::string & from, const std::string & to);
    std::vector<std::string> listDirectory(const std::string & path) const;
    std::pair<bool, std::optional<DirectoryRemoteInfo>> existsDirectory(const std::string & path) const;

    bool existsFile(const std::string & path) const;
    void addFile(const std::string & path);
    void removeFile(const std::string & path);

private:
    mutable CurrentMetrics::Increment remote_layout_directories_count;
    mutable CurrentMetrics::Increment remote_layout_files_count;

    mutable std::mutex mutex;
    std::shared_ptr<INode> root TSA_GUARDED_BY(mutex);
    std::unordered_map<std::string, std::shared_ptr<INode>> remote_path_to_inode TSA_GUARDED_BY(mutex);
};

}
