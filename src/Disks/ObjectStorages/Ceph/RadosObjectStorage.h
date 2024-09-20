#pragma once

#include <string>
#include "Disks/ObjectStorages/StoredObject.h"
#include "config.h"

#if USE_CEPH

#include <memory>
#include <IO/Ceph/RadosIOContext.h>
#include <Disks/ObjectStorages/Ceph/RadosUtils.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/MultiVersion.h>
#include <Common/ObjectStorageKeyGenerator.h>


namespace DB
{

/// The maximum size of object in rados is 4GB, therefore we need to split large objects into multiple rados objects
/// Ceph project provides `libradostriper`, nevertheless, it doesn't fit well to ClickHouse. Here we implement
/// a simple striper logic:
/// - A ClickHouse object may or may not be split to smaller physical object on rados (chunk), depending on if its size
///   fit a rados object
/// - Each chunk has same name with ClickHouse object, with the first (HEAD) chunk has exactly same name and subsequent
///   chunk has suffix `.clickhouse.chunk.N` at the end, with N is the chunk sequence.
/// - The HEAD chunk has special attributes to store the total number of objects in the stripe and the size and mtime of
///   the ClickHouse object.
namespace RadosStriper
{
    static constexpr const char * const XATTR_OBJECT_CHUNK_COUNT = "clickhouse.striper.object_chunk_count";
    static constexpr const char * const XATTR_OBJECT_SIZE = "clickhouse.striper.object_size";
    static constexpr const char * const XATTR_OBJECT_MTIME = "clickhouse.striper.object_mtime";
    static constexpr const char * const CHUNK_SUFFIX = "clickhouse.chunk";

    inline String getChunkName(const String & object_id, size_t chunk_seq)
    {
        return fmt::format("{}.{}.{}", object_id, CHUNK_SUFFIX, chunk_seq);
    }

    inline bool isOrdinaryObject(const String & object_name) { return !object_name.contains(CHUNK_SUFFIX); }

    inline bool isFirstChunkInObject(const std::shared_ptr<RadosIOContext> io, const String & object_name)
    {
        return io->getAttributeIfExists(object_name, XATTR_OBJECT_CHUNK_COUNT).value.has_value();
    }

    inline void patchStriperAtrributes(ObjectMetadata & metadata)
    {
        if (metadata.attributes.contains(RadosStriper::XATTR_OBJECT_SIZE))
            metadata.size_bytes = std::stoull(metadata.attributes.at(RadosStriper::XATTR_OBJECT_SIZE));
        if (metadata.attributes.contains(RadosStriper::XATTR_OBJECT_MTIME))
            metadata.last_modified = std::stoull(metadata.attributes.at(RadosStriper::XATTR_OBJECT_MTIME));
        metadata.attributes.erase(XATTR_OBJECT_CHUNK_COUNT);
        metadata.attributes.erase(XATTR_OBJECT_SIZE);
        metadata.attributes.erase(XATTR_OBJECT_MTIME);
    }
}

/// Some of important Ceph options, it can be in global_options map but we extract it here
/// for easier access
struct OSDSettings
{
    /// Maximum osd object size in bytes
    UInt64 osd_max_object_size = 128 * 1024 * 1024; /// 128M - default ceph setting value
    /// Maximum size of a single write to OSD
    UInt64 osd_max_write_size = 90 * 1024 * 1024; /// 90M - default ceph setting value
    /// Object is Ceph's term, here simply understanding as maximum number of operations that
    /// a single rados client can queue before throttling
    UInt64 objecter_inflight_ops = 1024; /// 1024 - default ceph setting value
};

struct RadosObjectStorageSettings
{
    RadosObjectStorageSettings() = default;

    RadosObjectStorageSettings(
        const RadosOptions & global_options_,
        bool read_only_)
        : global_options(global_options_)
        , read_only(read_only_)
    {}

    RadosOptions global_options;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    {
        global_options.user = config.getString(config_prefix + ".user", "");
        if (config.has(config_prefix + ".options"))
            global_options.loadFromConfig(config, config_prefix + ".options");
        global_options.validate();

        /// Extract OSDSettings
        if (global_options.contains("osd_max_object_size"))
            osd_settings.osd_max_object_size = std::stoull(global_options.at("osd_max_object_size"));
        if (global_options.contains("osd_max_write_size"))
            osd_settings.osd_max_write_size = std::stoull(global_options.at("osd_max_write_size")) * 1024 * 1024;
        if (global_options.contains("objecter_inflight_ops"))
            osd_settings.objecter_inflight_ops = std::stoull(global_options.at("objecter_inflight_ops"));

        read_only = config.getBool(config_prefix + ".read_only", read_only);
    }

    OSDSettings osd_settings;
    bool read_only = false;
};

/// Rados cluster include many pool (equivalent to S3 bucket). In each pool, we can have many namespace.
/// RadosObjectStorage associated with a pool and a namespace. listObject and iterate with prefix implementation is sub-par
class RadosObjectStorage : public IObjectStorage, public std::enable_shared_from_this<RadosObjectStorage>
{
private:
    RadosObjectStorage(
        const char * logger_name,
        std::shared_ptr<librados::Rados> rados_,
        std::unique_ptr<RadosObjectStorageSettings> ceph_settings_,
        RadosEndpoint endpoint_,
        const String & disk_name_,
        bool for_disk_ceph_ = true)
        : endpoint(endpoint_)
        , disk_name(disk_name_)
        , rados(std::move(rados_))
        , ceph_settings(std::move(ceph_settings_))
        , log(getLogger(logger_name))
        , for_disk_ceph(for_disk_ceph_)
    {
        io_ctx = std::make_unique<RadosIOContext>(rados, endpoint.pool, endpoint.nspace);
    }

public:
    template <class ...Args>
    explicit RadosObjectStorage(std::shared_ptr<librados::Rados> rados_, Args && ...args)
        : RadosObjectStorage("RadosObjectStorage", std::move(rados_), std::forward<Args>(args)...)
    {
    }

    ~RadosObjectStorage() override = default;

    String getCommonKeyPrefix() const override { return endpoint.getRelativePath(); }

    String getDescription() const override { return endpoint.getDescription(); }

    std::string getName() const override { return "RadosObjectStorage"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Rados; }

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

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    ObjectStorageIteratorPtr iterate(const std::string & path_prefix, size_t max_keys) const override;

    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects & objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context,
        const ApplyNewSettingsOptions & options) override;

    WriteSettings patchSettings(const WriteSettings & write_settings) const override;

    std::string getObjectsNamespace() const override { return endpoint.pool; }

    bool isRemote() const override { return true; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace, /// Not rados namespace, this is a new pool name (equivalent to bucket in S3)
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    bool supportParallelWrite() const override { return true; }

    ObjectStorageKey generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const override;

    bool isReadOnly() const override { return ceph_settings.get()->read_only; }

    std::shared_ptr<librados::Rados> tryGetRadosClient() const override { return rados; }

private:
    void setNewSettings(std::unique_ptr<RadosObjectStorageSettings> && ceph_settings_);

    void removeObjectImpl(const StoredObject & object, bool if_exists);
    void removeObjectsImpl(const StoredObjects & objects, bool if_exists);

    std::unique_ptr<ReadBufferFromFileBase> readObjectsImpl( /// NOLINT
        const StoredObjects & objects,
        const ReadSettings & read_settings,
        bool with_cache) const;

    StoredObjects getRadosObjects(const StoredObjects & objects, bool if_exists, bool with_size) const;

    const RadosEndpoint endpoint;

    std::string disk_name;

    std::shared_ptr<librados::Rados> rados;
    MultiVersion<RadosObjectStorageSettings> ceph_settings;
    std::shared_ptr<RadosIOContext> io_ctx;

    LoggerPtr log;

    const bool for_disk_ceph;
};

}

#endif
