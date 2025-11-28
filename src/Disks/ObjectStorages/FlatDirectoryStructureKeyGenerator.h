#pragma once

#include <Disks/ObjectStorages/InMemoryDirectoryTree.h>

#include <Common/ObjectStorageKeyGenerator.h>

#include <memory>

namespace DB
{

class FlatDirectoryStructureKeyGenerator : public IObjectStorageKeysGenerator
{
public:
    explicit FlatDirectoryStructureKeyGenerator(String storage_key_prefix_, std::weak_ptr<InMemoryDirectoryTree> tree_);

    ObjectStorageKey generate(const String & path, bool is_directory, const std::optional<String> & key_prefix) const override;

    bool isRandom() const override { return true; }

private:
    const String storage_key_prefix;

    std::weak_ptr<InMemoryDirectoryTree> tree;
};

}
