#pragma once

#include <filesystem>
#include <map>
#include <base/defines.h>
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
    /// Local -> Remote path.
    using Map = std::map<std::filesystem::path, std::string, PathComparator>;
    mutable SharedMutex mutex;
    Map map TSA_GUARDED_BY(mutex);
};

}
