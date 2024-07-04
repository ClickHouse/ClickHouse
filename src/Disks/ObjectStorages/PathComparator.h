#pragma once

#include <filesystem>

namespace DB
{
// TODO: rename
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

}
