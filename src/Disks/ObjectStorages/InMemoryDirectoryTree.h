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

class InMemoryDirectoryTree
{
    struct INode;

    /// TODO:
    std::shared_ptr<INode> walk(const std::filesystem::path & path, bool create_missing = false) const TSA_REQUIRES(mutex);

    /// TODO:
    void traverseSubtree(const std::filesystem::path & path, std::function<void(const std::string &, const std::shared_ptr<INode> &)> observe) const TSA_REQUIRES(mutex);

    /// TODO:
    std::filesystem::path determineNodePath(std::shared_ptr<INode> node) const TSA_REQUIRES(mutex);

public:
    InMemoryDirectoryTree(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name);
    void apply(std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout);

    /// TODO:
    std::optional<std::pair<std::string, DirectoryRemoteInfo>> lookupDirectoryIfNotChanged(const std::string & remote_path, const std::string & etag) const;

    /// TODO:
    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> getSubtreeRemoteInfo(const std::string & path) const;

    /// TODO:
    void recordDirectoryPath(const std::string & path, DirectoryRemoteInfo info);

    /// TODO:
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
