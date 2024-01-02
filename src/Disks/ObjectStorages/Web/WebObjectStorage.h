#pragma once

#include "config.h"

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <shared_mutex>

namespace Poco
{
class Logger;
}

namespace DB
{

class WebObjectStorage : public IObjectStorage, WithContext
{
    friend class MetadataStorageFromStaticFilesWebServer;
    friend class MetadataStorageFromStaticFilesWebServerTransaction;

public:
    WebObjectStorage(const String & url_, ContextPtr context_);

    DataSourceDescription getDataSourceDescription() const override
    {
        return DataSourceDescription{
            .type = DataSourceType::WebServer,
            .description = url,
            .is_encrypted = false,
            .is_cached = false,
        };
    }

    std::string getName() const override { return "WebObjectStorage"; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const StoredObjects & objects,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects &  objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    String getObjectsNamespace() const override { return ""; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    std::string generateBlobNameForPath(const std::string & path) override { return path; }

    bool isRemote() const override { return true; }

    bool isReadOnly() const override { return true; }

protected:
    [[noreturn]] static void throwNotAllowed();
    bool exists(const std::string & path) const;

    enum class FileType
    {
        File,
        Directory
    };

    struct FileData
    {
        FileType type{};
        size_t size = 0;
    };

    using Files = std::map<String, FileData>; /// file path -> file data
    mutable Files files;
    mutable std::shared_mutex metadata_mutex;

private:
    void initialize(const String & path, const std::unique_lock<std::shared_mutex> &) const;

    const String url;
    Poco::Logger * log;
    size_t min_bytes_for_seek;
};

}
