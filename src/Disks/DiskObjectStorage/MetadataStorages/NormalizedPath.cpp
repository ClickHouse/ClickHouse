#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>

namespace DB
{

NormalizedPath NormalizedPath::parent_path() const
{
    return NormalizedPath{std::filesystem::path::parent_path()};
}

NormalizedPath normalizePath(std::string path)
{
    auto filtered_path = std::filesystem::path(path).lexically_normal().string();

    /// Remove / leftovers from the ends
    std::string_view normalized_path = filtered_path;
    if (normalized_path.ends_with('/'))
        normalized_path.remove_suffix(1);

    if (normalized_path.starts_with('/'))
        normalized_path.remove_prefix(1);

    return NormalizedPath{normalized_path};
}

}
