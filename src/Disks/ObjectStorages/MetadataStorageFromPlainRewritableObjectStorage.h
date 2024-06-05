#pragma once

#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>

#include <memory>

namespace DB
{

class MetadataStorageFromPlainRewritableObjectStorage final : public MetadataStorageFromPlainObjectStorage
{
private:
    std::shared_ptr<PathMap> path_map;

public:
    MetadataStorageFromPlainRewritableObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_);
    ~MetadataStorageFromPlainRewritableObjectStorage() override;

    MetadataStorageType getType() const override { return MetadataStorageType::PlainRewritable; }

protected:
    std::shared_ptr<PathMap> getPathMap() const override { return path_map; }
    std::vector<std::string> getDirectChildrenOnDisk(
        const std::string & storage_key, const RelativePathsWithMetadata & remote_paths, const std::string & local_path) const override;
};

}
