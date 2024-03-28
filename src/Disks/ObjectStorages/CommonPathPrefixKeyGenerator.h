#pragma once

#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/SharedMutex.h>

#include <unordered_map>

namespace DB
{

class CommonPathPrefixKeyGenerator : public IObjectStorageKeysGenerator
{
public:
    using PathMap = std::unordered_map<std::string, std::string>;

    explicit CommonPathPrefixKeyGenerator(String key_prefix_, SharedMutex & shared_mutex_, std::weak_ptr<PathMap> path_map_);

    ObjectStorageKey generate(const String & path, bool is_directory) const override;

private:
    std::tuple<std::string, std::vector<String>> getLongestObjectKeyPrefix(const String & path) const;

    const String storage_key_prefix;

    SharedMutex & shared_mutex;
    std::weak_ptr<PathMap> path_map;
};

}
