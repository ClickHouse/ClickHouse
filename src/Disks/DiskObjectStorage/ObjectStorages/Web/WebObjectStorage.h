#pragma once

#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/HTTPHeaderEntries.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class WebObjectStorage : public IObjectStorage, public WithContext
{
    friend class MetadataStorageFromStaticFilesWebServer;
    friend class MetadataStorageFromStaticFilesWebServerTransaction;

public:
    WebObjectStorage(const String & url_, const String & query_fragment_, ContextPtr context_, HTTPHeaderEntries headers_ = {});

    std::string getName() const override { return "Web"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Web; }

    std::string getCommonKeyPrefix() const override { return url; }

    std::string getDescription() const override { return url; }

    const String & getBaseURL() const { return url; }
    const HTTPHeaderEntries & getHeaders() const { return headers; }

    bool exists(const StoredObject & object) const override;
    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override;
    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    String getObjectsNamespace() const override { return ""; }

    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override;

    bool isRemote() const override { return true; }

    bool isReadOnly() const override { return true; }

protected:
    [[noreturn]] static void throwNotAllowed();
    bool exists(const std::string & path) const;
    std::string buildURL(const std::string & path) const;

private:
    const String url;
    const String query_fragment;
    const HTTPHeaderEntries headers;
    LoggerPtr log;
};

}
