#pragma once

#include <set>
#include <map>
#include <mutex>
#include <optional>
#include <functional>
#include <filesystem>
#include <base/defines.h>

#include <Common/CurrentMetrics.h>


namespace DB
{

class InMemoryDirectoryPathMap
{
public:
    InMemoryDirectoryPathMap(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name)
        : metric_directories(metric_directories_name, 0), metric_files(metric_files_name, 0)
    {
    }

    /// Breadth-first order.
    struct PathComparator
    {
        bool operator()(const std::filesystem::path & path1, const std::filesystem::path & path2) const
        {
            auto d1 = std::distance(path1.begin(), path1.end());
            auto d2 = std::distance(path2.begin(), path2.end());
            if (d1 != d2)
                return d1 < d2;
            return path1 < path2;
        }
    };

    using FileNames = std::set<std::string>;
    struct RemotePathInfo
    {
        std::string path;
        time_t last_modified = 0;
        FileNames files;
    };

    size_t directoriesCount() const
    {
        std::lock_guard lock(mutex);
        return map.size();
    }

    bool existsRemotePath(const std::string & remote_path) const
    {
        std::lock_guard lock(mutex);
        return remote_directories.contains(remote_path);
    }

    bool existsLocalPath(const std::string & local_path) const
    {
        std::lock_guard lock(mutex);
        return map.contains(local_path);
    }

    auto addPathIfNotExists(std::string path, RemotePathInfo info)
    {
        std::string remote_path = info.path;
        std::lock_guard lock(mutex);

        size_t num_files = info.files.size();
        auto res = map.emplace(std::move(path), std::move(info));

        if (res.second)
        {
            remote_directories.emplace(remote_path);
            metric_directories.add(1);
            metric_files.add(num_files);
        }

        return res;
    }

    bool existsFile(const std::string & local_path) const
    {
        auto path = std::filesystem::path(local_path);
        auto dir = path.parent_path();
        auto filename = path.filename();

        std::lock_guard lock(mutex);
        auto it = map.find(dir);
        if (it == map.end())
            return false;

        return it->second.files.contains(filename);
    }

    bool addFile(const std::string & local_path)
    {
        auto path = std::filesystem::path(local_path);
        auto dir = path.parent_path();
        auto file = path.filename();

        std::lock_guard lock(mutex);
        auto it = map.find(dir);
        if (it == map.end())
            return false;
        if (it->second.files.emplace(file).second)
            metric_files.add(1);
        return true;
    }

    bool removeFile(const std::string & local_path)
    {
        auto path = std::filesystem::path(local_path);
        auto dir = path.parent_path();
        auto file = path.filename();

        std::lock_guard lock(mutex);
        auto it = map.find(dir);
        if (it == map.end())
            return false;
        metric_files.sub(1);
        return it->second.files.erase(file);
    }

    std::optional<RemotePathInfo> getRemotePathInfoIfExists(const std::string & path) const
    {
        auto base_path = path;
        if (base_path.ends_with('/'))
            base_path.pop_back();

        std::lock_guard lock(mutex);
        auto it = map.find(base_path);
        if (it == map.end())
            return std::nullopt;
        return it->second;
    }

    bool removePathIfExists(const std::string & path)
    {
        std::lock_guard lock(mutex);
        auto it = map.find(path);
        if (map.end() == it)
            return false;

        metric_files.sub(it->second.files.size());
        metric_directories.sub(1);

        remote_directories.erase(it->second.path);
        map.erase(it);
        return true;
    }

    size_t removeOutdatedPaths(const std::set<std::string> & actual_set_of_remote_directories)
    {
        size_t num_removed = 0;
        std::lock_guard lock(mutex);
        for (auto it = map.begin(); it != map.end();)
        {
            if (actual_set_of_remote_directories.contains(it->second.path))
            {
                ++it;
            }
            else
            {
                metric_files.sub(it->second.files.size());
                metric_directories.sub(1);

                remote_directories.erase(it->second.path);
                it = map.erase(it);
                ++num_removed;
            }
        }
        return num_removed;
    }

    void iterateFiles(const std::string & path, std::function<void(const std::string &)> callback) const
    {
        auto base_path = path;
        if (base_path.ends_with('/'))
            base_path.pop_back();

        std::lock_guard lock(mutex);
        auto it = map.find(base_path);
        if (it != map.end())
            for (const auto & file : it->second.files)
                callback(file);
    }

    void iterateSubdirectories(const std::string & path, std::function<void(const std::string &)> callback) const
    {
        std::lock_guard lock(mutex);
        for (auto it = map.lower_bound(path); it != map.end(); ++it)
        {
            const auto & subdirectory = it->first.string();
            if (!subdirectory.starts_with(path))
                break;

            auto slash_num = count(subdirectory.begin() + path.size(), subdirectory.end(), '/');

            /// The directory map comparator ensures that the paths with the smallest number of
            /// hops from the local_path are iterated first. The paths do not end with '/', hence
            /// break the loop if the number of slashes to the right from the offset is greater than 0.
            if (slash_num != 0)
                break;

            callback(std::string(subdirectory.begin() + path.size(), subdirectory.end()) + "/");
        }
    }

    void moveDirectory(const std::string & from, const std::string & to)
    {
        std::lock_guard lock(mutex);
        [[maybe_unused]] auto result = map.emplace(to, map.extract(from).mapped());
        chassert(result.second);
        result.first->second.last_modified = time(nullptr);
    }

private:
    mutable std::mutex mutex;

    /// A mapping from logical filesystem path to the storage path.
    using Map = std::map<std::filesystem::path, RemotePathInfo, PathComparator>;
    Map TSA_GUARDED_BY(mutex) map;

    /// A set of known storage paths (randomly-assigned names).
    FileNames TSA_GUARDED_BY(mutex) remote_directories;

    CurrentMetrics::Increment metric_directories;
    CurrentMetrics::Increment metric_files;
};

}
