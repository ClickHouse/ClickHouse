#pragma once
#include "config.h"


#if USE_HDFS

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Core/UUID.h>
#include <memory>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

struct HDFSObjectStorageSettings
{

    HDFSObjectStorageSettings() = default;

    size_t min_bytes_for_seek;
    int objects_chunk_size_to_delete;
    int replication;

    HDFSObjectStorageSettings(
            int min_bytes_for_seek_,
            int objects_chunk_size_to_delete_,
            int replication_)
        : min_bytes_for_seek(min_bytes_for_seek_)
        , objects_chunk_size_to_delete(objects_chunk_size_to_delete_)
        , replication(replication_)
    {}
};


class HDFSObjectStorage : public IObjectStorage
{
public:

    using SettingsPtr = std::unique_ptr<HDFSObjectStorageSettings>;

    HDFSObjectStorage(
        const String & hdfs_root_path_,
        SettingsPtr settings_,
        const Poco::Util::AbstractConfiguration & config_)
        : config(config_)
        , hdfs_builder(createHDFSBuilder(hdfs_root_path_, config))
        , hdfs_fs(createHDFSFS(hdfs_builder.get()))
        , settings(std::move(settings_))
        , hdfs_root_path(hdfs_root_path_)
    {
    }

    std::string getName() const override { return "HDFSObjectStorage"; }

    std::string getCommonKeyPrefix() const override { return hdfs_root_path; }

    std::string getDescription() const override { return hdfs_root_path; }

    ObjectStorageType getType() const override { return ObjectStorageType::HDFS; }

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

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects & objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    String getObjectsNamespace() const override { return ""; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) const override;

    bool isRemote() const override { return true; }

private:
    const Poco::Util::AbstractConfiguration & config;

    HDFSBuilderWrapper hdfs_builder;
    HDFSFSPtr hdfs_fs;
    SettingsPtr settings;
    const std::string hdfs_root_path;
};

}

#endif
