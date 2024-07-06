#include "CommonPathPrefixKeyGenerator.h"

#include <Common/getRandomASCIIString.h>

#include <deque>
#include <filesystem>
#include <tuple>

namespace DB
{

CommonPathPrefixKeyGenerator::CommonPathPrefixKeyGenerator(
    String key_prefix_, SharedMutex & shared_mutex_, std::weak_ptr<PathMap> path_map_)
    : storage_key_prefix(key_prefix_), shared_mutex(shared_mutex_), path_map(std::move(path_map_))
{
}

ObjectStorageKey CommonPathPrefixKeyGenerator::generate(const String & path, bool is_directory) const
{
    const auto & [object_key_prefix, suffix_parts] = getLongestObjectKeyPrefix(path);

    auto key = std::filesystem::path(object_key_prefix.empty() ? storage_key_prefix : object_key_prefix);

    /// The longest prefix is the same as path, meaning that the  path is already mapped.
    if (suffix_parts.empty())
        return ObjectStorageKey::createAsRelative(std::move(key));

    /// File and top-level directory paths are mapped as is.
    if (!is_directory || object_key_prefix.empty())
        for (const auto & part : suffix_parts)
            key /= part;
    /// Replace the last part of the directory path with a pseudorandom suffix.
    else
    {
        for (size_t i = 0; i + 1 < suffix_parts.size(); ++i)
            key /= suffix_parts[i];

        constexpr size_t part_size = 16;
        key /= getRandomASCIIString(part_size);
    }

    return ObjectStorageKey::createAsRelative(key);
}

std::tuple<std::string, std::vector<std::string>> CommonPathPrefixKeyGenerator::getLongestObjectKeyPrefix(const std::string & path) const
{
    std::filesystem::path p(path);
    std::deque<std::string> dq;

    std::shared_lock lock(shared_mutex);

    auto ptr = path_map.lock();

    while (p != p.root_path())
    {
        auto it = ptr->find(p / "");
        if (it != ptr->end())
        {
            std::vector<std::string> vec(std::make_move_iterator(dq.begin()), std::make_move_iterator(dq.end()));
            return std::make_tuple(it->second, std::move(vec));
        }

        if (!p.filename().empty())
            dq.push_front(p.filename());

        p = p.parent_path();
    }

    return {std::string(), std::vector<std::string>(std::make_move_iterator(dq.begin()), std::make_move_iterator(dq.end()))};
}

}
