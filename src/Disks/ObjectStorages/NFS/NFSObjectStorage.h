#pragma once

#include <Common/config.h>
#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskLocalCheckThread.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

class DiskLocalReservation;

struct NFSObjectStorageSettings
{
    size_t min_bytes_for_seek;
    size_t remote_file_buffer_size;
    int thread_pool_size;
    int objects_chunk_size_to_delete;
    size_t nfs_max_single_read_retries;

    NFSObjectStorageSettings(
            size_t min_bytes_for_seek_,
            size_t remote_file_buffer_size_,
            int thread_pool_size_,
            int objects_chunk_size_to_delete_,
            size_t nfs_max_single_read_retries_)
        : min_bytes_for_seek(min_bytes_for_seek_)
        , remote_file_buffer_size(remote_file_buffer_size_)
        , thread_pool_size(thread_pool_size_)
        , objects_chunk_size_to_delete(objects_chunk_size_to_delete_)
        , nfs_max_single_read_retries(nfs_max_single_read_retries_)
    {
    }
};

/**
 * Storage for persisting data in NFS and metadata on the local disk.
 * Files are represented by file in local filesystem (clickhouse_root/disks/disk_name/path/to/file)
 * that contains NFS object key with actual data.
 * NFS path example : /root_dir/volume/shard/shard_number/
 */
class NFSObjectStorage final : public IObjectStorage
{
public:
    using SettingsPtr = std::unique_ptr<NFSObjectStorageSettings>;
    using GetDiskSettings = std::function<SettingsPtr(const Poco::Util::AbstractConfiguration &, const String, ContextPtr)>;

    NFSObjectStorage(const String & name_, const String & path_);
    NFSObjectStorage(
        const String & name_,
        const String & root_path_,
        ContextPtr context_,
        SettingsPtr settings_,
        const Poco::Util::AbstractConfiguration & config_)
    : name(name_), context(context_), settings(std::move(settings_)), config(config_),
    log(&Poco::Logger::get("NFSObjectStorage"))
    {
        data_source_description.type = DataSourceType::NFS;
        data_source_description.description = root_path_;
        data_source_description.is_cached = false;
        data_source_description.is_encrypted = false;
    }

    bool isRemote() const override { return true; }

    bool supportsCache() const override { return true; }

    std::string getName() const override { return "NFSObjectStorage"; }

    String getObjectsNamespace() const override { return ""; }

    DataSourceDescription getDataSourceDescription() const override
    {
        return data_source_description;
    }

    void shutdown() override;

    void startup() override;

    bool exists(const StoredObject & object) const override;

    void listPrefix(const std::string & path, RelativePathsWithSize & children) const override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

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

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects & objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    std::string generateBlobNameForPath(const std::string & path) override;

private:
    String getRandomName() { return toString(UUIDHelpers::generateV4()); }
    String name;
    ContextPtr context;
    SettingsPtr settings;
    const Poco::Util::AbstractConfiguration & config;
    Poco::Logger * log;
    DataSourceDescription data_source_description;
};

}

