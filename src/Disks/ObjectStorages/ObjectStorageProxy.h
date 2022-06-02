#pragma once

#include <Common/config.h>

#include <Disks/ObjectStorages/IObjectStorage.h>


namespace DB
{

class ObjectStorageProxy : public IObjectStorage
{
public:
    explicit ObjectStorageProxy(ObjectStoragePtr object_storage_) : object_storage(std::move(object_storage_)) {}

    bool exists(const std::string & path) const override
    {
        return object_storage->exists(path);
    }

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const std::string & path,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override
    {
        return object_storage->readObject(path, read_settings, read_hint, file_size);
    }

    std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const std::string & common_path_prefix,
        const BlobsPathToSize & blobs_to_read,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override
    {
        return object_storage->readObjects(common_path_prefix, blobs_to_read, read_settings, read_hint, file_size);
    }

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const std::string & path,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override
    {
        return object_storage->writeObject(path, mode, attributes, std::move(finalize_callback), buf_size, write_settings);
    }

    void listPrefix(const std::string & path, BlobsPathToSize & children) const override
    {
        object_storage->listPrefix(path, children);
    }

    void removeObject(const std::string & path) override
    {
        object_storage->removeObject(path);
    }

    void removeObjects(const std::vector<std::string> & paths) override
    {
        object_storage->removeObjects(paths);
    }

    void removeObjectIfExists(const std::string & path) override
    {
        object_storage->removeObjectIfExists(path);
    }

    void removeObjectsIfExist(const std::vector<std::string> & paths) override
    {
        object_storage->removeObjectsIfExist(paths);
    }

    ObjectMetadata getObjectMetadata(const std::string & path) const override
    {
        return object_storage->getObjectMetadata(path);
    }

    void copyObject( /// NOLINT
        const std::string & object_from,
        const std::string & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override
    {
        object_storage->copyObject(object_from, object_to, object_to_attributes);
    }

    void copyObjectToAnotherObjectStorage( /// NOLINT
        const std::string & object_from,
        const std::string & object_to,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override
    {
        object_storage->copyObjectToAnotherObjectStorage(object_from, object_to, object_storage_to, object_to_attributes);
    }

    void shutdown() override
    {
        object_storage->shutdown();
    }

    void startup() override
    {
        object_storage->startup();
    }

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context) override
    {
        object_storage->applyNewSettings(config, config_prefix, context);
    }

    String getObjectsNamespace() const override
    {
        return object_storage->getObjectsNamespace();
    }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override
    {
        return object_storage->cloneObjectStorage(new_namespace, config, config_prefix, context);
    }

protected:
    ObjectStoragePtr object_storage;
};

}
