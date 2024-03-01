#include "CommonPathPrefixKeyGenerator.h"

#include <Common/Exception.h>
#include <Common/getRandomASCIIString.h>

#include <deque>
#include <filesystem>
#include <tuple>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

CommonPathPrefixKeyGenerator::CommonPathPrefixKeyGenerator(String key_prefix_, std::weak_ptr<PathMap> path_map_)
    : key_prefix(key_prefix_), path_map(std::move(path_map_))
{
}

ObjectStorageKey CommonPathPrefixKeyGenerator::generate(const String & path, bool is_directory) const
{
    auto result = getLongestPrefix(path);

    const auto & unrealized_parts = std::get<1>(result);

    if (unrealized_parts.size() >= 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find object key prefix for path {}", path);

    auto key = std::filesystem::path(std::get<0>(result));
    if (unrealized_parts.empty())
        return ObjectStorageKey::createAsRelative(std::move(key));

    constexpr size_t part_size = 8;
    if (is_directory)
        key /= getRandomASCIIString(part_size);
    else
        key /= unrealized_parts.front();

    return ObjectStorageKey::createAsRelative(key);
}

std::tuple<std::string, std::vector<std::string>> CommonPathPrefixKeyGenerator::getLongestPrefix(const std::string & path) const
{
    std::filesystem::path p(path);
    std::deque<std::string> dq;

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

    return {key_prefix, std::vector<std::string>(std::make_move_iterator(dq.begin()), std::make_move_iterator(dq.end()))};
}

}
