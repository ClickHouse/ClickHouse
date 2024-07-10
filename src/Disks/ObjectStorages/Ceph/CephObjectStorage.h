#pragma once

#include "config.h"

#if USE_CEPH

#include <memory>
#include <IO/Ceph/RadosIO.h>
#include <Disks/ObjectStorages/Ceph/CephUtils.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/StorageCephSettings.h>
#include <Common/MultiVersion.h>
#include <Common/ObjectStorageKeyGenerator.h>


namespace DB
{

struct CephObjectStorageSettings
{
    CephObjectStorageSettings() = default;

    CephObjectStorageSettings(
        const CephOptions & global_options_,
        uint64_t min_bytes_for_seek_,
        int32_t list_object_keys_size_,
        int32_t objects_chunk_size_to_delete_,
        bool read_only_)
        : global_options(global_options_)
        , min_bytes_for_seek(min_bytes_for_seek_)
        , list_object_keys_size(list_object_keys_size_)
        , objects_chunk_size_to_delete(objects_chunk_size_to_delete_)
        , read_only(read_only_)
    {}

    CephOptions global_options;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    {
        global_options.user = config.getString(config_prefix + ".user", "");
        if (config.has(config_prefix + ".options"))
            global_options.loadFromConfig(config, config_prefix + ".options");
        global_options.validate();
        min_bytes_for_seek = config.getUInt64(config_prefix + ".min_bytes_for_seek", min_bytes_for_seek);
        list_object_keys_size = config.getInt(config_prefix + ".list_object_keys_size", list_object_keys_size);
        objects_chunk_size_to_delete = config.getInt(config_prefix + ".objects_chunk_size_to_delete", objects_chunk_size_to_delete);
        read_only = config.getBool(config_prefix + ".read_only", read_only);
    }

    uint64_t min_bytes_for_seek;
    int32_t list_object_keys_size;
    int32_t objects_chunk_size_to_delete;
    bool read_only;
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
        , ceph_settings(std::move(ceph_settings_))
        , log(getLogger(logger_name))
        , for_disk_ceph(for_disk_ceph_)
    {
        /// Not allow using empty namespace if this is for disk
        if (for_disk_ceph && (endpoint.nspace.empty() || endpoint.nspace == LIBRADOS_ALL_NSPACES))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "CephObjectStorage: namespace cannot be empty if it's created for disk");
        io_impl = std::make_unique<Ceph::RadosIO>(rados, endpoint.pool, endpoint.nspace);
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

    std::string getObjectsNamespace() const override { return endpoint.pool; }

    bool isRemote() const override { return true; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace, ///!!! Not rados namespace, this is a new pool name (equivalent to bucket in S3)
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
