#pragma once

#include <filesystem>
#include <map>
#include <optional>
#include <shared_mutex>
#include <base/defines.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>

namespace DB
{


struct InMemoryPathMap
{
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
    struct RemotePathInfo
    {
        std::string path;
        time_t last_modified = 0;
    };

    using Map = std::map<std::filesystem::path, RemotePathInfo, PathComparator>;

    std::optional<RemotePathInfo> getRemotePathInfoIfExists(const std::string & path)
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

    mutable SharedMutex mutex;

#ifdef OS_LINUX
    Map TSA_GUARDED_BY(mutex) map;
/// std::shared_mutex may not be annotated with the 'capability' attribute in libcxx.
#else
    Map map;
#endif
};

}
