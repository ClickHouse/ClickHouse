#pragma once

#include <Common/config.h>

#include <Disks/ObjectStorages/IObjectStorage.h>

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
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listPrefix(const std::string & path, RelativePathsWithSize & children) const override;

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

    bool supportsAppend() const override { return false; }

    std::string generateBlobNameForPath(const std::string & path) override { return path; }

    bool isRemote() const override { return true; }

    bool isReadOnly() const override { return true; }

protected:
    void initialize(const String & uri_path) const;

    [[noreturn]] static void throwNotAllowed();

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

    using Files = std::unordered_map<String, FileData>; /// file path -> file data
    mutable Files files;

    String url;

private:
    Poco::Logger * log;

    size_t min_bytes_for_seek;
};

}
