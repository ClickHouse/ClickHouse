#pragma once

#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <functional>
#include <set>
#include <base/defines.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>


namespace DB
{

class InMemoryDirectoryPathMap
{
public:
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
    using FileNamesIterator = FileNames::iterator;
    struct FileNameIteratorComparator
    {
        bool operator()(const FileNames::iterator & lhs, const FileNames::iterator & rhs) const { return *lhs < *rhs; }
    };
    using FileNamesIterators = std::set<FileNamesIterator, FileNameIteratorComparator>;

    struct RemotePathInfo
    {
        std::string path;
        time_t last_modified = 0;
        FileNamesIterators filename_iterators;
    };

    size_t directoriesCount() const
    {
        SharedLockGuard lock(mutex);
        return map.size();
    }

    size_t filesCount() const
    {
        SharedLockGuard lock(mutex);
        return unique_filenames.size();
    }

    bool existsRemotePath(const std::string & remote_path) const
    {
        SharedLockGuard lock(mutex);
        return remote_directories.contains(remote_path);
    }

    bool existsLocalPath(const std::string & local_path) const
    {
        SharedLockGuard lock(mutex);
        return map.contains(local_path);
    }

    auto addPathIfNotExists(std::string path, RemotePathInfo info, const FileNames & files)
    {
        std::string remote_path = info.path;
        std::lock_guard lock(mutex);

        auto res = map.emplace(std::move(path), std::move(info));

        if (res.second)
        {
            remote_directories.emplace(remote_path);

            for (const auto & file : files)
            {
                auto file_insert_res = unique_filenames.insert(file);
                if (file_insert_res.second)
                    res.first->second.filename_iterators.insert(file_insert_res.first);
            }
        }

        return res;
    }

    std::optional<RemotePathInfo> getRemotePathInfoIfExists(const std::string & path) const
    {
        auto base_path = path;
        if (base_path.ends_with('/'))
            base_path.pop_back();

        SharedLockGuard lock(mutex);
        auto it = map.find(base_path);
        if (it == map.end())
            return std::nullopt;
        return it->second;
    }

    bool removePathIfExists(const std::filesystem::path & path)
    {
        std::lock_guard lock(mutex);
        auto it = map.find(path);
        if (map.end() == it)
            return false;

        for (auto unique_files_it : it->second.filename_iterators)
            unique_filenames.erase(unique_files_it);
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
                for (auto unique_files_it : it->second.filename_iterators)
                    unique_filenames.erase(unique_files_it);
                remote_directories.erase(it->second.path);
                it = map.erase(it);
                ++num_removed;
            }
        }
        return num_removed;
    }

    void iterateFiles(const std::string & path, std::function<void(const std::string &)> callback) const
    {
        std::string parent_path = std::filesystem::path(path).parent_path();

        SharedLockGuard lock(mutex);
        auto it = map.find(parent_path);
        if (it != map.end())
            for (const auto & filename_it : it->second.filename_iterators)
                callback(*filename_it);
    }

    void iterateSubdirectories(const std::string & path, std::function<void(const std::string &)> callback) const
    {
        SharedLockGuard lock(mutex);
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
    mutable SharedMutex mutex;

    /// A set of logical paths of all files.
    FileNames TSA_GUARDED_BY(mutex) unique_filenames;

    /// A mapping from logical filesystem path to the storage path.
    using Map = std::map<std::filesystem::path, RemotePathInfo, PathComparator>;
    Map TSA_GUARDED_BY(mutex) map;

    /// A set of known storage paths (randomly-assigned names).
    FileNames TSA_GUARDED_BY(mutex) remote_directories;
};

}
