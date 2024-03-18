#pragma once

#include <Common/ObjectStorageKeyGenerator.h>

#include <unordered_map>

namespace DB
{

class CommonPathPrefixKeyGenerator : public IObjectStorageKeysGenerator
{
public:
    using PathMap = std::unordered_map<std::string, std::string>;

    explicit CommonPathPrefixKeyGenerator(String key_prefix_, std::weak_ptr<PathMap> path_map_);

    ObjectStorageKey generate(const String & path, bool is_directory) const override;

private:
    std::tuple<std::string, std::vector<String>> getLongestObjectKeyPrefix(const String & path) const;

    String storage_key_prefix;
    std::weak_ptr<PathMap> path_map;
};

}
