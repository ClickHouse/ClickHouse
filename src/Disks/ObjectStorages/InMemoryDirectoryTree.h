#pragma once

#include <Common/CurrentMetrics.h>

#include <base/defines.h>

#include <unordered_map>
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
    std::vector<std::string> file_names;
};

class InMemoryDirectoryTree
{
    struct INode;

    /// TODO:
    std::shared_ptr<INode> walk(const std::filesystem::path & path, bool create_missing) const TSA_REQUIRES(mutex);

    /// TODO:
    void traverseSubtree(const std::filesystem::path & path, std::function<void(const std::string &, const std::shared_ptr<INode> &)> observe) const TSA_REQUIRES(mutex);

public:
    InMemoryDirectoryTree(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name);

    /// TODO:
    bool existsRemotePathUnchanged(const std::string & remote_path, const std::string & etag) const;

    /// TODO:
    std::optional<DirectoryRemoteInfo> getDirectoryRemoteInfo(const std::string & path) const;

    /// TODO:
    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> getSubtreeRemoteInfo(const std::string & path) const;

    /// TODO:
    void recordDirectoryPath(const std::string & path, DirectoryRemoteInfo info);

    /// TODO:
    void unlinkTree(const std::string & path);

    /// Normal File System Methods
    bool existsDirectory(const std::string & path) const;
    void moveDirectory(const std::string & from, const std::string & to);
    std::vector<std::string> listDirectory(const std::string & path) const;

    bool existsFile(const std::string & path) const;
    void addFile(const std::string & path);
    void removeFile(const std::string & path);

private:
    CurrentMetrics::Metric metric_directories;
    CurrentMetrics::Metric metric_files;

    mutable std::mutex mutex;
    std::shared_ptr<INode> root TSA_GUARDED_BY(mutex);
    std::unordered_map<std::string, std::shared_ptr<INode>> remote_path_to_inode TSA_GUARDED_BY(mutex);
};

}
