#pragma once

#include <Common/ObjectStorageKeyGenerator.h>

#include <memory>
namespace DB
{

struct InMemoryDirectoryPathMap;
class FlatDirectoryStructureKeyGenerator : public IObjectStorageKeysGenerator
{
public:
    explicit FlatDirectoryStructureKeyGenerator(String storage_key_prefix_, std::weak_ptr<InMemoryDirectoryPathMap> path_map_);

    ObjectStorageKey generate(const String & path, bool is_directory, const std::optional<String> & key_prefix) const override;

private:
    const String storage_key_prefix;

    std::weak_ptr<InMemoryDirectoryPathMap> path_map;
};

}
