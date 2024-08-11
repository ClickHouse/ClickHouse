#pragma once

#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>

#include <memory>
#include <unordered_set>


namespace DB
{

class MetadataStorageFromPlainRewritableObjectStorage final : public MetadataStorageFromPlainObjectStorage
{
private:
    const std::string metadata_key_prefix;
    std::shared_ptr<InMemoryPathMap> path_map;

public:
    MetadataStorageFromPlainRewritableObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t file_sizes_cache_size);
    ~MetadataStorageFromPlainRewritableObjectStorage() override;

    MetadataStorageType getType() const override { return MetadataStorageType::PlainRewritable; }
    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;
    std::vector<std::string> listDirectory(const std::string & path) const override;

protected:
    std::string getMetadataKeyPrefix() const override { return metadata_key_prefix; }
    std::shared_ptr<InMemoryPathMap> getPathMap() const override { return path_map; }
    void getDirectChildrenOnDisk(
        const std::string & storage_key,
        const RelativePathsWithMetadata & remote_paths,
        const std::string & local_path,
        std::unordered_set<std::string> & result) const;

private:
    bool useSeparateLayoutForMetadata() const;
};

}
