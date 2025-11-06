#include <Disks/ObjectStorages/FlatDirectoryStructureKeyGenerator.h>
#include <Disks/ObjectStorages/InMemoryDirectoryPathMap.h>
#include <Common/ObjectStorageKey.h>
#include <Common/getRandomASCIIString.h>

#include <optional>
#include <string>


namespace DB
{

FlatDirectoryStructureKeyGenerator::FlatDirectoryStructureKeyGenerator(
    String storage_key_prefix_, std::weak_ptr<InMemoryDirectoryPathMap> path_map_)
    : storage_key_prefix(storage_key_prefix_), path_map(std::move(path_map_))
{
}

ObjectStorageKey FlatDirectoryStructureKeyGenerator::generate(const String & path, bool is_directory, const std::optional<String> & key_prefix) const
{
    if (is_directory)
        chassert(path.empty() || path.ends_with('/'));

    const auto fs_path = std::filesystem::path(path);
    std::filesystem::path directory = fs_path.parent_path();

    std::optional<std::filesystem::path> remote_path;
    {
        const auto ptr = path_map.lock();
        auto res = ptr->getRemotePathInfoIfExists(fs_path);
        if (res)
            return ObjectStorageKey::createAsRelative(key_prefix.has_value() ? *key_prefix : storage_key_prefix, res->path);

        res = ptr->getRemotePathInfoIfExists(directory);
        if (res)
            remote_path = res->path;
    }
    constexpr size_t part_size = 32;
    std::filesystem::path key = remote_path.has_value() ? *remote_path
        : is_directory                                  ? std::filesystem::path(getRandomASCIIString(part_size))
                                                        : directory;

    if (!is_directory)
        key /= fs_path.filename();

    return ObjectStorageKey::createAsRelative(key_prefix.has_value() ? *key_prefix : storage_key_prefix, key);
}

}
