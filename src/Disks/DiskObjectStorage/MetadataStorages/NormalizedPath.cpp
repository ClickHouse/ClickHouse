#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>

#include <base/defines.h>

#include <algorithm>

namespace DB
{

NormalizedPath NormalizedPath::parent_path() const
{
    return NormalizedPath{std::filesystem::path::parent_path()};
}

NormalizedPath normalizePath(std::string path)
{
    auto lexically_normal = std::filesystem::path(path).lexically_normal();
    auto filtered_path = lexically_normal.string();

    /// Check that paths do not use .. anytime
    chassert(std::ranges::contains(lexically_normal, "..") == false);

    /// Remove leftovers from the ends
    std::string_view normalized_path = filtered_path;
    while (normalized_path.ends_with('/') || normalized_path.ends_with('.'))
        normalized_path.remove_suffix(1);

    while (normalized_path.starts_with('/') || normalized_path.starts_with('.'))
        normalized_path.remove_prefix(1);

    return NormalizedPath{normalized_path};
}

}
