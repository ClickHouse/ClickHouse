#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

[[maybe_unused]] static bool isPathRelative(const std::filesystem::path & path)
{
    for (const auto & step : path)
        if (step.string() == "..")
            return true;

    return false;
}

NormalizedPath NormalizedPath::parent_path() const
{
    return NormalizedPath{std::filesystem::path::parent_path()};
}

NormalizedPath normalizePath(std::string path)
{
    auto lexically_normal = std::filesystem::path(path).lexically_normal();

#ifndef NDEBUG
    if (isPathRelative(lexically_normal))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Path '{}' should not be used in disks", lexically_normal.string());
#endif

    /// Remove leftovers from the ends
    auto filtered_path = lexically_normal.string();
    std::string_view normalized_path = filtered_path;
    while (normalized_path.ends_with('/') || normalized_path.ends_with('.'))
        normalized_path.remove_suffix(1);

    while (normalized_path.starts_with('/') || normalized_path.starts_with('.'))
        normalized_path.remove_prefix(1);

    return NormalizedPath{normalized_path};
}

}
