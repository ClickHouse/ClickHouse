#pragma once

#include <string>
#include "Disks/ObjectStorages/StoredObject.h"
#include "config.h"

#if USE_CEPH

#include <memory>
#include <IO/Ceph/RadosIO.h>
#include <Disks/ObjectStorages/Ceph/CephUtils.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/MultiVersion.h>
#include <Common/ObjectStorageKeyGenerator.h>


namespace DB
{

/// The maximum size of object in rados is 4GB, therefore we need to split large objects into multiple objects
/// Ceph project provides `libradostriper`, nevertheless, it doesn't fit well to ClickHouse. Here we implement
/// a simple striper logic:
/// - Each object has a suffix `.#N` where N is a sequence number of the object in the stripe
/// - The first object in the stripe has a suffix `.#0` and all attributes are stored in this object
/// - The first object has special atributes to store the total number of objects in the stripe and the
///   size and mtime of the big object
namespace RadosStriper
{
    static constexpr const char * const XATTR_OBJECT_COUNT = "clickhouse.striper.object_count";
    static constexpr const char * const XATTR_OBJECT_SIZE = "clickhouse.striper.object_size";
    static constexpr const char * const XATTR_OBJECT_MTIME = "clickhouse.striper.object_mtime";
    static constexpr const char * const STRIP_SUFFIX = "clickhouse.strip";

    inline String getStripedObjectName(const String & object_id, size_t object_seq)
    {
        return fmt::format("{}.{}.{}", object_id, STRIP_SUFFIX, object_seq);
    }

    inline bool isRegularObject(const String & object_name) { return !object_name.contains(STRIP_SUFFIX); }

    inline bool isFirstObjectInStripe(const std::shared_ptr<Ceph::RadosIO> io, const String & object_name)
    {
        return io->getAttributeIfExists(object_name, XATTR_OBJECT_COUNT).value.has_value();
    }
}

struct CephObjectStorageSettings
{
    CephObjectStorageSettings() = default;

    CephObjectStorageSettings(
        const CephOptions & global_options_,
        size_t max_object_size_,
        bool read_only_)
        : global_options(global_options_)
        , max_object_size(max_object_size_)
        , read_only(read_only_)
    {}

    CephOptions global_options;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    {
        global_options.user = config.getString(config_prefix + ".user", "");
        if (config.has(config_prefix + ".options"))
            global_options.loadFromConfig(config, config_prefix + ".options");
        global_options.validate();
        max_object_size = config.getUInt64(config_prefix + ".max_object_size", max_object_size);
        read_only = config.getBool(config_prefix + ".read_only", read_only);
    }

    uint64_t max_object_size = 0;
    bool read_only = false;
};

/// Rados cluster include many pool (equivalent to S3 bucket). In each pool, we can have many namespace.
/// CephObjectStorage associated with a pool and a namespace. The object name will have namespace as prefix.
/// listObject and iterate with prefix implementation is sub-par, so we cannot use CephObjectStorage with plain
/// metadata type.
class CephObjectStorage : public IObjectStorage
{
private:
    CephObjectStorage(
        const char * logger_name,
        std::shared_ptr<librados::Rados> rados_,
        std::unique_ptr<CephObjectStorageSettings> ceph_settings_,
        CephEndpoint endpoint_,
        const String & disk_name_,
        bool for_disk_ceph_ = true)
        : endpoint(endpoint_)
        , disk_name(disk_name_)
        , rados(std::move(rados_))
        , log(getLogger(logger_name))
        , for_disk_ceph(for_disk_ceph_)
    {
        /// Not allow using empty namespace if this is for disk
        if (for_disk_ceph && (endpoint.nspace.empty() || endpoint.nspace == LIBRADOS_ALL_NSPACES))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "CephObjectStorage: namespace cannot be empty if it's created for disk");
        io_impl = std::make_unique<Ceph::RadosIO>(rados, endpoint.pool, endpoint.nspace);
        /// Get max object size from cluster
        String val;
        size_t rados_max_osd_object_size = io_impl->getMaxObjectSize();
        if (ceph_settings_->max_object_size == 0 || ceph_settings_->max_object_size > rados_max_osd_object_size)
            ceph_settings_->max_object_size = rados_max_osd_object_size;
        ceph_settings.set(std::move(ceph_settings_));
    }

public:
    template <class ...Args>
    explicit CephObjectStorage(std::shared_ptr<librados::Rados> rados_, Args && ...args)
        : CephObjectStorage("CephObjectStorage", std::move(rados_), std::forward<Args>(args)...)
    {
    }

    ~CephObjectStorage() override = default;

    String getCommonKeyPrefix() const override { return endpoint.nspace; }

    String getDescription() const override { return endpoint.mon_hosts + "/" + endpoint.pool; }

    std::string getName() const override { return "CephObjectStorage"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Ceph; }

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

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) const override;

    bool isReadOnly() const override { return ceph_settings.get()->read_only; }

private:
    void setNewSettings(std::unique_ptr<CephObjectStorageSettings> && ceph_settings_);

    void removeObjectImpl(const StoredObject & object, bool if_exists);
    void removeObjectsImpl(const StoredObjects & objects, bool if_exists);

    std::unique_ptr<ReadBufferFromFileBase> readObjectsImpl( /// NOLINT
        const StoredObjects & objects,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const;

    StoredObjects getRadosObjects(const StoredObjects & objects, bool if_exists, bool with_size) const;

    const CephEndpoint endpoint;

    std::string disk_name;

    std::shared_ptr<librados::Rados> rados;
    MultiVersion<CephObjectStorageSettings> ceph_settings;
    std::shared_ptr<Ceph::RadosIO> io_impl;

    LoggerPtr log;

    const bool for_disk_ceph;
};

}

#endif
