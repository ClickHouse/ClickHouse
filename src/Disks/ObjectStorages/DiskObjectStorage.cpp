#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadBufferFromEmptyFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/formatReadable.h>
#include <Common/CurrentThread.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Common/filesystemHelpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Disks/ObjectStorages/DiskObjectStorageRemoteMetadataRestoreHelper.h>
#include <IO/CachedInMemoryReadBufferFromFile.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/ObjectStorages/DiskObjectStorageTransaction.h>
#include <Disks/FakeDiskTransaction.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Parsers/ASTCreateResourceQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DISK_INDEX;
}


DiskTransactionPtr DiskObjectStorage::createTransaction()
{
    return std::make_shared<FakeDiskTransaction>(*this);
}

ObjectStoragePtr DiskObjectStorage::getObjectStorage()
{
    return object_storage;
}

DiskTransactionPtr DiskObjectStorage::createObjectStorageTransaction()
{
    return std::make_shared<DiskObjectStorageTransaction>(
        *object_storage,
        *metadata_storage,
        send_metadata ? metadata_helper.get() : nullptr);
}

DiskTransactionPtr DiskObjectStorage::createObjectStorageTransactionToAnotherDisk(DiskObjectStorage& to_disk)
{
    return std::make_shared<MultipleDisksObjectStorageTransaction>(
        *object_storage,
        *metadata_storage,
        *to_disk.getObjectStorage(),
        *to_disk.getMetadataStorage(),
        send_metadata ? metadata_helper.get() : nullptr);
}


DiskObjectStorage::DiskObjectStorage(
    const String & name_,
    const String & object_key_prefix_,
    MetadataStoragePtr metadata_storage_,
    ObjectStoragePtr object_storage_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
    : IDisk(name_, config, config_prefix)
    , object_key_prefix(object_key_prefix_)
    , log(getLogger("DiskObjectStorage(" + name + ")"))
    , metadata_storage(std::move(metadata_storage_))
    , object_storage(std::move(object_storage_))
    , send_metadata(config.getBool(config_prefix + ".send_metadata", false))
    , read_resource_name_from_config(config.getString(config_prefix + ".read_resource", ""))
    , write_resource_name_from_config(config.getString(config_prefix + ".write_resource", ""))
    , metadata_helper(std::make_unique<DiskObjectStorageRemoteMetadataRestoreHelper>(this, ReadSettings{}, WriteSettings{}))
{
    data_source_description = DataSourceDescription{
        .type = DataSourceType::ObjectStorage,
        .object_storage_type = object_storage->getType(),
        .metadata_type = metadata_storage->getType(),
        .description = object_storage->getDescription(),
        .is_encrypted = false,
        .is_cached = object_storage->supportsCache(),
    };
    resource_changes_subscription = Context::getGlobalContextInstance()->getWorkloadEntityStorage().getAllEntitiesAndSubscribe(
        [this] (const std::vector<IWorkloadEntityStorage::Event> & events)
        {
            std::unique_lock lock{resource_mutex};

            // Sets of matching resource names. Required to resolve possible conflicts in deterministic way
            std::set<String> new_read_resource_name_from_sql;
            std::set<String> new_write_resource_name_from_sql;
            std::set<String> new_read_resource_name_from_sql_any;
            std::set<String> new_write_resource_name_from_sql_any;

            // Current state
            if (!read_resource_name_from_sql.empty())
                new_read_resource_name_from_sql.insert(read_resource_name_from_sql);
            if (!write_resource_name_from_sql.empty())
                new_write_resource_name_from_sql.insert(write_resource_name_from_sql);
            if (!read_resource_name_from_sql_any.empty())
                new_read_resource_name_from_sql_any.insert(read_resource_name_from_sql_any);
            if (!write_resource_name_from_sql_any.empty())
                new_write_resource_name_from_sql_any.insert(write_resource_name_from_sql_any);

            // Process all updates in specified order
            for (const auto & [entity_type, resource_name, resource] : events)
            {
                if (entity_type == WorkloadEntityType::Resource)
                {
                    if (resource) // CREATE RESOURCE
                    {
                        auto * create = typeid_cast<ASTCreateResourceQuery *>(resource.get());
                        chassert(create);
                        for (const auto & [mode, disk] : create->operations)
                        {
                            if (!disk)
                            {
                                switch (mode)
                                {
                                    case ASTCreateResourceQuery::AccessMode::Read: new_read_resource_name_from_sql_any.insert(resource_name); break;
                                    case ASTCreateResourceQuery::AccessMode::Write: new_write_resource_name_from_sql_any.insert(resource_name); break;
                                }
                            }
                            else if (*disk == name)
                            {
                                switch (mode)
                                {
                                    case ASTCreateResourceQuery::AccessMode::Read: new_read_resource_name_from_sql.insert(resource_name); break;
                                    case ASTCreateResourceQuery::AccessMode::Write: new_write_resource_name_from_sql.insert(resource_name); break;
                                }
                            }
                        }
                    }
                    else // DROP RESOURCE
                    {
                        new_read_resource_name_from_sql.erase(resource_name);
                        new_write_resource_name_from_sql.erase(resource_name);
                        new_read_resource_name_from_sql_any.erase(resource_name);
                        new_write_resource_name_from_sql_any.erase(resource_name);
                    }
                }
            }

            String old_read_resource = getReadResourceNameNoLock();
            String old_write_resource = getWriteResourceNameNoLock();

            // Apply changes
            if (!new_read_resource_name_from_sql_any.empty())
                read_resource_name_from_sql_any = *new_read_resource_name_from_sql_any.begin();
            else
                read_resource_name_from_sql_any.clear();

            if (!new_write_resource_name_from_sql_any.empty())
                write_resource_name_from_sql_any = *new_write_resource_name_from_sql_any.begin();
            else
                write_resource_name_from_sql_any.clear();

            if (!new_read_resource_name_from_sql.empty())
                read_resource_name_from_sql = *new_read_resource_name_from_sql.begin();
            else
                read_resource_name_from_sql.clear();

            if (!new_write_resource_name_from_sql.empty())
                write_resource_name_from_sql = *new_write_resource_name_from_sql.begin();
            else
                write_resource_name_from_sql.clear();

            String new_read_resource = getReadResourceNameNoLock();
            String new_write_resource = getWriteResourceNameNoLock();

            if (old_read_resource != new_read_resource)
                LOG_INFO(log, "Using resource '{}' instead of '{}' for READ", new_read_resource, old_read_resource);
            if (old_write_resource != new_write_resource)
                LOG_INFO(log, "Using resource '{}' instead of '{}' for WRITE", new_write_resource, old_write_resource);
        });
}

StoredObjects DiskObjectStorage::getStorageObjects(const String & local_path) const
{
    return metadata_storage->getStorageObjects(local_path);
}


bool DiskObjectStorage::existsFile(const String & path) const
{
    return metadata_storage->existsFile(path);
}

bool DiskObjectStorage::existsDirectory(const String & path) const
{
    return metadata_storage->existsDirectory(path);
}

bool DiskObjectStorage::existsFileOrDirectory(const String & path) const
{
    return metadata_storage->existsFileOrDirectory(path);
}


void DiskObjectStorage::createFile(const String & path)
{
    auto transaction = createObjectStorageTransaction();
    transaction->createFile(path);
    transaction->commit();
}

size_t DiskObjectStorage::getFileSize(const String & path) const
{
    return metadata_storage->getFileSize(path);
}

void DiskObjectStorage::moveDirectory(const String & from_path, const String & to_path)
{
    if (send_metadata)
        sendMoveMetadata(from_path, to_path);

    auto transaction = createObjectStorageTransaction();
    transaction->moveDirectory(from_path, to_path);
    transaction->commit();
}

void DiskObjectStorage::moveFile(const String & from_path, const String & to_path, bool should_send_metadata)
{

    if (should_send_metadata)
        sendMoveMetadata(from_path, to_path);

    auto transaction = createObjectStorageTransaction();
    transaction->moveFile(from_path, to_path);
    transaction->commit();
}

void DiskObjectStorage::truncateFile(const String & path, size_t size)
{
    LOG_TEST(log, "Truncate file operation {} to size : {}", path, size);
    auto transaction = createObjectStorageTransaction();
    transaction->truncateFile(path, size);
    transaction->commit();
}

void DiskObjectStorage::copyFile( /// NOLINT
    const String & from_file_path,
    IDisk & to_disk,
    const String & to_file_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const std::function<void()> & cancellation_hook
    )
{
    if (getDataSourceDescription() == to_disk.getDataSourceDescription())
    {
            /// It may use s3-server-side copy
            auto & to_disk_object_storage = dynamic_cast<DiskObjectStorage &>(to_disk);
            auto transaction = createObjectStorageTransactionToAnotherDisk(to_disk_object_storage);
            transaction->copyFile(from_file_path, to_file_path, /*read_settings*/ {}, /*write_settings*/ {});
            transaction->commit();
    }
    else
    {
        /// Copy through buffers
        IDisk::copyFile(from_file_path, to_disk, to_file_path, read_settings, write_settings, cancellation_hook);
    }
}

void DiskObjectStorage::moveFile(const String & from_path, const String & to_path)
{
    moveFile(from_path, to_path, send_metadata);
}

void DiskObjectStorage::replaceFile(const String & from_path, const String & to_path)
{
    if (existsFile(to_path))
    {
        auto transaction = createObjectStorageTransaction();
        transaction->replaceFile(from_path, to_path);
        transaction->commit();
    }
    else
        moveFile(from_path, to_path);
}

void DiskObjectStorage::removeSharedFile(const String & path, bool delete_metadata_only)
{
    auto transaction = createObjectStorageTransaction();
    transaction->removeSharedFile(path, delete_metadata_only);
    transaction->commit();
}

void DiskObjectStorage::removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only)
{
    auto transaction = createObjectStorageTransaction();
    transaction->removeSharedFiles(files, keep_all_batch_data, file_names_remove_metadata_only);
    transaction->commit();
}

UInt32 DiskObjectStorage::getRefCount(const String & path) const
{
    return metadata_storage->getHardlinkCount(path);
}

std::unordered_map<String, String> DiskObjectStorage::getSerializedMetadata(const std::vector<String> & file_paths) const
{
    return metadata_storage->getSerializedMetadata(file_paths);
}

String DiskObjectStorage::getUniqueId(const String & path) const
{
    String id;
    auto blobs_paths = metadata_storage->getStorageObjects(path);
    if (!blobs_paths.empty())
        id = blobs_paths[0].remote_path;
    return id;
}

bool DiskObjectStorage::checkUniqueId(const String & id) const
{
    auto object = StoredObject(id);
    return object_storage->exists(object);
}

void DiskObjectStorage::createHardLink(const String & src_path, const String & dst_path, bool should_send_metadata)
{
    if (should_send_metadata && !dst_path.starts_with("shadow/"))
    {
        auto revision = metadata_helper->revision_counter + 1;
        metadata_helper->revision_counter += 1;
        const ObjectAttributes object_metadata {
            {"src_path", src_path},
            {"dst_path", dst_path}
        };
        metadata_helper->createFileOperationObject("hardlink", revision, object_metadata);
    }

    auto transaction = createObjectStorageTransaction();
    transaction->createHardLink(src_path, dst_path);
    transaction->commit();
}

void DiskObjectStorage::createHardLink(const String & src_path, const String & dst_path)
{
    createHardLink(src_path, dst_path, send_metadata);
}


void DiskObjectStorage::setReadOnly(const String & path)
{
    /// We should store read only flag inside metadata file (instead of using FS flag),
    /// because we modify metadata file when create hard-links from it.
    auto transaction = createObjectStorageTransaction();
    transaction->setReadOnly(path);
    transaction->commit();
}


void DiskObjectStorage::createDirectory(const String & path)
{
    auto transaction = createObjectStorageTransaction();
    transaction->createDirectory(path);
    transaction->commit();
}


void DiskObjectStorage::createDirectories(const String & path)
{
    auto transaction = createObjectStorageTransaction();
    transaction->createDirectories(path);
    transaction->commit();
}


void DiskObjectStorage::clearDirectory(const String & path)
{
    auto transaction = createObjectStorageTransaction();
    transaction->clearDirectory(path);
    transaction->commit();
}


void DiskObjectStorage::removeDirectory(const String & path)
{
    auto transaction = createObjectStorageTransaction();
    transaction->removeDirectory(path);
    transaction->commit();
}


DirectoryIteratorPtr DiskObjectStorage::iterateDirectory(const String & path) const
{
    return metadata_storage->iterateDirectory(path);
}


void DiskObjectStorage::listFiles(const String & path, std::vector<String> & file_names) const
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        file_names.push_back(it->name());
}


void DiskObjectStorage::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    auto transaction = createObjectStorageTransaction();
    transaction->setLastModified(path, timestamp);
    transaction->commit();
}


Poco::Timestamp DiskObjectStorage::getLastModified(const String & path) const
{
    return metadata_storage->getLastModified(path);
}

time_t DiskObjectStorage::getLastChanged(const String & path) const
{
    return metadata_storage->getLastChanged(path);
}

struct stat DiskObjectStorage::stat(const String & path) const
{
    return metadata_storage->stat(path);
}

void DiskObjectStorage::chmod(const String & path, mode_t mode)
{
    auto transaction = createObjectStorageTransaction();
    transaction->chmod(path, mode);
    transaction->commit();
}

void DiskObjectStorage::shutdown()
{
    LOG_INFO(log, "Shutting down disk {}", name);
    object_storage->shutdown();
    metadata_storage->shutdown();
    LOG_INFO(log, "Disk {} shut down", name);
}

void DiskObjectStorage::startupImpl(ContextPtr context)
{
    LOG_INFO(log, "Starting up disk {}", name);
    object_storage->startup();

    restoreMetadataIfNeeded(context->getConfigRef(), "storage_configuration.disks." + name, context);

    LOG_INFO(log, "Disk {} started up", name);
}

ReservationPtr DiskObjectStorage::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};

    return std::make_unique<DiskObjectStorageReservation>(
        std::static_pointer_cast<DiskObjectStorage>(shared_from_this()), bytes);
}

void DiskObjectStorage::removeSharedFileIfExists(const String & path, bool delete_metadata_only)
{
    auto transaction = createObjectStorageTransaction();
    transaction->removeSharedFileIfExists(path, delete_metadata_only);
    transaction->commit();
}

void DiskObjectStorage::removeSharedRecursive(
    const String & path, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only)
{
    auto transaction = createObjectStorageTransaction();
    transaction->removeSharedRecursive(path, keep_all_batch_data, file_names_remove_metadata_only);
    transaction->commit();
}

bool DiskObjectStorage::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(reservation_mutex);

    auto available_space = getAvailableSpace();
    if (!available_space)
    {
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }

    UInt64 unreserved_space = *available_space - std::min(*available_space, reserved_bytes);

    if (bytes == 0)
    {
        LOG_TRACE(log, "Reserved 0 bytes on remote disk {}", backQuote(name));
        ++reservation_count;
        return true;
    }

    if (unreserved_space >= bytes)
    {
        LOG_TRACE(
            log,
            "Reserved {} on remote disk {}, having unreserved {}.",
            ReadableSize(bytes),
            backQuote(name),
            ReadableSize(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }

    LOG_TRACE(log, "Could not reserve {} on remote disk {}. Not enough unreserved space", ReadableSize(bytes), backQuote(name));


    return false;
}
void DiskObjectStorage::sendMoveMetadata(const String & from_path, const String & to_path)
{
    chassert(send_metadata);
    auto revision = metadata_helper->revision_counter + 1;
    metadata_helper->revision_counter += 1;

    const ObjectAttributes object_metadata{{"from_path", from_path}, {"to_path", to_path}};
    metadata_helper->createFileOperationObject("rename", revision, object_metadata);
}

bool DiskObjectStorage::supportsCache() const
{
    return object_storage->supportsCache();
}

bool DiskObjectStorage::isReadOnly() const
{
    return object_storage->isReadOnly();
}

bool DiskObjectStorage::isWriteOnce() const
{
    return object_storage->isWriteOnce();
}

bool DiskObjectStorage::supportsHardLinks() const
{
    return !isWriteOnce() && !object_storage->isPlain();
}

DiskObjectStoragePtr DiskObjectStorage::createDiskObjectStorage()
{
    const auto config_prefix = "storage_configuration.disks." + name;
    return std::make_shared<DiskObjectStorage>(
        getName(),
        object_key_prefix,
        metadata_storage,
        object_storage,
        Context::getGlobalContextInstance()->getConfigRef(),
        config_prefix);
}

template <class Settings>
static inline Settings updateIOSchedulingSettings(const Settings & settings, const String & read_resource_name, const String & write_resource_name)
{
    if (read_resource_name.empty() && write_resource_name.empty())
        return settings;
    if (auto query_context = CurrentThread::getQueryContext())
    {
        Settings result(settings);
        if (!read_resource_name.empty())
            result.io_scheduling.read_resource_link = query_context->getWorkloadClassifier()->get(read_resource_name);
        if (!write_resource_name.empty())
            result.io_scheduling.write_resource_link = query_context->getWorkloadClassifier()->get(write_resource_name);
        return result;
    }
    return settings;
}

String DiskObjectStorage::getReadResourceName() const
{
    std::unique_lock lock(resource_mutex);
    return getReadResourceNameNoLock();
}

String DiskObjectStorage::getWriteResourceName() const
{
    std::unique_lock lock(resource_mutex);
    return getWriteResourceNameNoLock();
}

String DiskObjectStorage::getReadResourceNameNoLock() const
{
    if (read_resource_name_from_config.empty())
        return read_resource_name_from_sql.empty() ? read_resource_name_from_sql_any : read_resource_name_from_sql;
    else
        return read_resource_name_from_config;
}

String DiskObjectStorage::getWriteResourceNameNoLock() const
{
    if (write_resource_name_from_config.empty())
        return write_resource_name_from_sql.empty() ? write_resource_name_from_sql_any : write_resource_name_from_sql;
    else
        return write_resource_name_from_config;
}

std::unique_ptr<ReadBufferFromFileBase> DiskObjectStorage::readFile(
    const String & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    const auto storage_objects = metadata_storage->getStorageObjects(path);
    auto global_context = Context::getGlobalContextInstance();

    const bool file_can_be_empty = !file_size.has_value() || *file_size == 0;
    if (storage_objects.empty() && file_can_be_empty)
        return std::make_unique<ReadBufferFromEmptyFile>();

    auto read_settings = updateIOSchedulingSettings(settings, getReadResourceName(), getWriteResourceName());
    /// We wrap read buffer from object storage (read_buf = object_storage->readObject())
    /// inside ReadBufferFromRemoteFSGather, so add nested buffer setting.
    read_settings = read_settings.withNestedBuffer();

    auto read_buffer_creator =
        [this, read_settings, read_hint, file_size]
        (bool restricted_seek, const StoredObject & object_) mutable -> std::unique_ptr<ReadBufferFromFileBase>
    {
        read_settings.remote_read_buffer_restrict_seek = restricted_seek;
        auto impl = object_storage->readObject(object_, read_settings, read_hint, file_size);

        if ((!object_storage->supportsCache() || !read_settings.enable_filesystem_cache)
            && read_settings.page_cache && read_settings.use_page_cache_for_disks_without_file_cache)
        {
            /// Can't wrap CachedOnDiskReadBufferFromFile in CachedInMemoryReadBufferFromFile because the
            /// former doesn't support seeks.
            auto cache_path_prefix = fmt::format("{}:", magic_enum::enum_name(object_storage->getType()));
            const auto object_namespace = object_storage->getObjectsNamespace();
            if (!object_namespace.empty())
                cache_path_prefix += object_namespace + "/";

            const auto cache_key = FileChunkAddress { .path = cache_path_prefix + object_.remote_path };

            impl = std::make_unique<CachedInMemoryReadBufferFromFile>(
                cache_key, read_settings.page_cache, std::move(impl), read_settings);
        }
        return impl;
    };

    /// Avoid cache fragmentation by choosing bigger buffer size.
    bool prefer_bigger_buffer_size = object_storage->supportsCache() && read_settings.enable_filesystem_cache;
    size_t buffer_size = prefer_bigger_buffer_size
        ? std::max<size_t>(settings.remote_fs_buffer_size, DBMS_DEFAULT_BUFFER_SIZE)
        : settings.remote_fs_buffer_size;

    size_t total_objects_size = file_size ? *file_size : getTotalSize(storage_objects);
    if (total_objects_size)
        buffer_size = std::min(buffer_size, total_objects_size);

    const bool use_async_buffer = read_settings.remote_fs_method == RemoteFSReadMethod::threadpool;
    auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
        std::move(read_buffer_creator),
        storage_objects,
        read_settings,
        global_context->getFilesystemCacheLog(),
        /* use_external_buffer */use_async_buffer,
        /* buffer_size */use_async_buffer ? 0 : buffer_size);

    if (use_async_buffer)
    {
        auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
        return std::make_unique<AsynchronousBoundedReadBuffer>(
            std::move(impl),
            reader,
            read_settings,
            buffer_size,
            global_context->getAsyncReadCounters(),
            global_context->getFilesystemReadPrefetchesLog());

    }
    return impl;
}

std::unique_ptr<ReadBufferFromFileBase> DiskObjectStorage::readFileIfExists(
    const String & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (auto storage_objects = metadata_storage->getStorageObjectsIfExist(path))
        return readFile(path, settings, read_hint, file_size);
    else
        return {};
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorage::writeFile(
    const String & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    LOG_TEST(log, "Write file: {}", path);

    WriteSettings write_settings = updateIOSchedulingSettings(settings, getReadResourceName(), getWriteResourceName());
    auto transaction = createObjectStorageTransaction();
    return transaction->writeFile(path, buf_size, mode, write_settings);
}

Strings DiskObjectStorage::getBlobPath(const String & path) const
{
    auto objects = getStorageObjects(path);
    Strings res;
    res.reserve(objects.size() + 1);
    for (const auto & object : objects)
        res.emplace_back(object.remote_path);
    String objects_namespace = object_storage->getObjectsNamespace();
    if (!objects_namespace.empty())
        res.emplace_back(objects_namespace);
    return res;
}

void DiskObjectStorage::writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function)
{
    LOG_TEST(log, "Write file: {}", path);
    auto transaction = createObjectStorageTransaction();
    transaction->writeFileUsingBlobWritingFunction(path, mode, std::move(write_blob_function));
    transaction->commit();
}

void DiskObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config, ContextPtr context_, const String & /*config_prefix*/, const DisksMap & disk_map)
{
    /// FIXME we cannot use config_prefix that was passed through arguments because the disk may be wrapped with cache and we need another name
    const auto config_prefix = "storage_configuration.disks." + name;
    object_storage->applyNewSettings(config, config_prefix, context_, IObjectStorage::ApplyNewSettingsOptions{ .allow_client_change = true });

    {
        std::unique_lock lock(resource_mutex);
        if (String new_read_resource_name = config.getString(config_prefix + ".read_resource", ""); new_read_resource_name != read_resource_name_from_config)
            read_resource_name_from_config = new_read_resource_name;
        if (String new_write_resource_name = config.getString(config_prefix + ".write_resource", ""); new_write_resource_name != write_resource_name_from_config)
            write_resource_name_from_config = new_write_resource_name;
    }

    IDisk::applyNewSettings(config, context_, config_prefix, disk_map);
}

void DiskObjectStorage::restoreMetadataIfNeeded(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    if (send_metadata)
    {
        metadata_helper->restore(config, config_prefix, context);

        auto current_schema_version = metadata_helper->readSchemaVersion(object_storage.get(), object_key_prefix);
        if (current_schema_version < DiskObjectStorageRemoteMetadataRestoreHelper::RESTORABLE_SCHEMA_VERSION)
            metadata_helper->migrateToRestorableSchema();

        metadata_helper->findLastRevision();
    }
}

void DiskObjectStorage::syncRevision(UInt64 revision)
{
    metadata_helper->syncRevision(revision);
}

UInt64 DiskObjectStorage::getRevision() const
{
    return metadata_helper->getRevision();
}

#if USE_AWS_S3
std::shared_ptr<const S3::Client> DiskObjectStorage::getS3StorageClient() const
{
    return object_storage->getS3StorageClient();
}

std::shared_ptr<const S3::Client> DiskObjectStorage::tryGetS3StorageClient() const
{
    return object_storage->tryGetS3StorageClient();
}
#endif

DiskPtr DiskObjectStorageReservation::getDisk(size_t i) const
{
    if (i != 0)
        throw Exception(ErrorCodes::INCORRECT_DISK_INDEX, "Can't use i != 0 with single disk reservation");
    return disk;
}

void DiskObjectStorageReservation::update(UInt64 new_size)
{
    std::lock_guard lock(disk->reservation_mutex);
    disk->reserved_bytes -= size;
    size = new_size;
    disk->reserved_bytes += size;
}

DiskObjectStorageReservation::~DiskObjectStorageReservation()
{
    try
    {
        std::lock_guard lock(disk->reservation_mutex);
        if (disk->reserved_bytes < size)
        {
            disk->reserved_bytes = 0;
            LOG_ERROR(disk->log, "Unbalanced reservations size for disk '{}'.", disk->getName());
        }
        else
        {
            disk->reserved_bytes -= size;
        }

        if (disk->reservation_count == 0)
            LOG_ERROR(disk->log, "Unbalanced reservation count for disk '{}'.", disk->getName());
        else
            --disk->reservation_count;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
