#pragma once

#include <Common/config.h>

#include <Disks/ObjectStorages/IObjectStorage.h>


namespace DB
{

class LocalObjectStorage : public IObjectStorage
{
public:
    LocalObjectStorage() = default;

    bool exists(const std::string & path) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const std::string & path,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const std::string & common_path_prefix,
        const BlobsPathToSize & blobs_to_read,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const std::string & path,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listPrefix(const std::string & path, BlobsPathToSize & children) const override;

    void removeObject(const std::string & path) override;

    void removeObjects(const std::vector<std::string> & paths) override;

    void removeObjectIfExists(const std::string & path) override;

    void removeObjectsIfExist(const std::vector<std::string> & paths) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void copyObject( /// NOLINT
        const std::string & object_from,
        const std::string & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void copyObjectToAnotherObjectStorage( /// NOLINT
        const std::string & object_from,
        const std::string & object_to,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context) override;

    String getObjectsNamespace() const override { return ""; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    bool supportsAppend() const override { return true; }

    String getUniqueIdForBlob(const String & path) override;

    std::string generateBlobNameForPath(const std::string & path) override { return path; }

    bool isRemote() const override { return false; }

private:
};

}
