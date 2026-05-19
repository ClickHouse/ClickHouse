#pragma once

#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/HTTPHeaderEntries.h>

#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

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
    struct URL
    {
        String base_url;
        String query_fragment;
    };

    using URLOptions = std::vector<URL>;
    using URLShards = std::vector<URLOptions>;

    WebObjectStorage(
        const String & url_,
        const String & query_fragment_,
        ContextPtr context_,
        HTTPHeaderEntries headers_ = {},
        size_t max_directories_to_read_ = 0);

    WebObjectStorage(
        URLShards url_shards_,
        ContextPtr context_,
        HTTPHeaderEntries headers_ = {},
        size_t max_directories_to_read_ = 0);

    std::string getName() const override { return "Web"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Web; }

    std::string getCommonKeyPrefix() const override { return getBaseURL(); }

    std::string getDescription() const override { return getBaseURL(); }

    const String & getBaseURL() const { return url_shards.front().front().base_url; }
    const String & getQueryFragment() const { return url_shards.front().front().query_fragment; }
    const URLShards & getURLShards() const { return url_shards; }
    const HTTPHeaderEntries & getHeaders() const { return headers; }
    std::vector<String> buildURLs(const std::string & path) const;
    std::vector<String> buildURLs(const std::string & path, size_t shard_index) const;

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
    ObjectMetadata getObjectMetadata(const RelativePathWithMetadata & path, bool with_tags) const override;
    std::optional<ObjectMetadata> tryGetObjectMetadata(const RelativePathWithMetadata & path, bool with_tags) const override;

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
    static std::string buildURL(const URL & url_option, const std::string & path);

private:
    enum class HeadSupport
    {
        Unknown,
        Supported,
        Unsupported,
    };

    HeadSupport getHeadSupportForOrigin(const Poco::URI & uri) const;
    void setHeadSupportForOrigin(const Poco::URI & uri, HeadSupport support) const;
    size_t getMaxDirectoriesToRead() const;

    static constexpr size_t max_head_support_cache_size = 65536;

    const URLShards url_shards;
    const HTTPHeaderEntries headers;
    const size_t max_directories_to_read;
    mutable std::mutex head_support_mutex;
    using HeadSupportLRUList = std::list<std::pair<String, HeadSupport>>;
    using HeadSupportLRUIndex = std::unordered_map<String, HeadSupportLRUList::iterator>;
    mutable HeadSupportLRUList head_support_lru;
    mutable HeadSupportLRUIndex head_support_by_origin;
    LoggerPtr log;
};

}
