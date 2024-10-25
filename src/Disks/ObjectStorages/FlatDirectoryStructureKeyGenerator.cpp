#include "FlatDirectoryStructureKeyGenerator.h"
#include <Disks/ObjectStorages/InMemoryDirectoryPathMap.h>
#include "Common/ObjectStorageKey.h"
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
#include <Common/getRandomASCIIString.h>

#include <optional>
#include <shared_mutex>
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
        chassert(path.ends_with('/'));

    const auto p = std::filesystem::path(path);
    auto directory = p.parent_path();

    std::optional<std::filesystem::path> remote_path;
    {
        const auto ptr = path_map.lock();
        SharedLockGuard lock(ptr->mutex);
        auto it = ptr->map.find(p);
        if (it != ptr->map.end())
            return ObjectStorageKey::createAsRelative(key_prefix.has_value() ? *key_prefix : storage_key_prefix, it->second.path);

        it = ptr->map.find(directory);
        if (it != ptr->map.end())
            remote_path = it->second.path;
    }
    constexpr size_t part_size = 32;
    std::filesystem::path key = remote_path.has_value() ? *remote_path
        : is_directory                                  ? std::filesystem::path(getRandomASCIIString(part_size))
                                                        : directory;

    if (!is_directory)
        key /= p.filename();

    return ObjectStorageKey::createAsRelative(key_prefix.has_value() ? *key_prefix : storage_key_prefix, key);
}

}
