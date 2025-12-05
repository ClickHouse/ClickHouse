#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/InMemoryDirectoryTree.h>

#include <Common/ObjectStorageKeyGenerator.h>

#include <memory>

namespace DB
{

class FlatDirectoryStructureKeyGenerator : public IObjectStorageKeyGenerator
{
public:
    explicit FlatDirectoryStructureKeyGenerator(std::string storage_key_prefix_, std::weak_ptr<InMemoryDirectoryTree> tree_);

    ObjectStorageKey generate(const std::string & path) const override;
    std::string generateForDirectory(const std::string & path) const;

    bool isRandom() const override { return true; }

private:
    const std::string storage_key_prefix;

    std::weak_ptr<InMemoryDirectoryTree> tree;
};

}
