#pragma once

#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>

#include <memory>
#include <unordered_set>
#include <IO/WriteHelpers.h>


namespace DB
{

class MetadataStorageFromPlainRewritableObjectStorageTransaction final : public MetadataStorageFromPlainObjectStorageTransaction
{
public:
    explicit MetadataStorageFromPlainRewritableObjectStorageTransaction(
        MetadataStorageFromPlainObjectStorage & metadata_storage_, ObjectStoragePtr object_storage_)
        : DB::MetadataStorageFromPlainObjectStorageTransaction(metadata_storage_, object_storage_)
    {
    }

    void createEmptyFile(const std::string & path) override
    {
        const auto key = object_storage->generateObjectKeyForPath(path, std::nullopt);
        StoredObject object(key.serialize(), "", /* file_size */0);
        auto buf = object_storage->writeObject(object, WriteMode::Rewrite);
        buf->finalize();
    }
};


class MetadataStorageFromPlainRewritableObjectStorage final : public MetadataStorageFromPlainObjectStorage
{
private:
    const std::string metadata_key_prefix;
    std::shared_ptr<InMemoryDirectoryPathMap> path_map;

public:
    MetadataStorageFromPlainRewritableObjectStorage(
        ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t object_metadata_cache_size);
    ~MetadataStorageFromPlainRewritableObjectStorage() override;

    MetadataStorageType getType() const override { return MetadataStorageType::PlainRewritable; }

    MetadataTransactionPtr createTransaction() override
    {
        return std::make_shared<MetadataStorageFromPlainRewritableObjectStorageTransaction>(*this, object_storage);
    }

    bool existsFile(const std::string & path) const override;

    bool existsDirectory(const std::string & path) const override;

    bool existsFileOrDirectory(const std::string & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    std::optional<Poco::Timestamp> getLastModifiedIfExists(const String & path) const override;

protected:
    std::string getMetadataKeyPrefix() const override { return metadata_key_prefix; }
    std::shared_ptr<InMemoryDirectoryPathMap> getPathMap() const override { return path_map; }
    std::unordered_set<std::string> getDirectChildrenOnDisk(const std::filesystem::path & local_path) const;

private:
    bool useSeparateLayoutForMetadata() const;
};

}
