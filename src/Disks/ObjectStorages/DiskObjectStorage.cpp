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
#include <Disks/ObjectStorages/DiskObjectStorageRemoteMetadataRestoreHelper.h>
#include <Disks/ObjectStorages/DiskObjectStorageTransaction.h>
#include <Disks/FakeDiskTransaction.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DISK_INDEX;
    extern const int FILE_DOESNT_EXIST;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int DIRECTORY_DOESNT_EXIST;
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
    , read_resource_name(config.getString(config_prefix + ".read_resource", ""))
    , write_resource_name(config.getString(config_prefix + ".write_resource", ""))
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
}

StoredObjects DiskObjectStorage::getStorageObjects(const String & local_path) const
{
    return metadata_storage->getStorageObjects(local_path);
}

void DiskObjectStorage::getRemotePathsRecursive(const String & local_path, std::vector<LocalPathWithObjectStoragePaths> & paths_map)
{
    if (!metadata_storage->exists(local_path))
        return;

    /// Protect against concurrent delition of files (for example because of a merge).
    if (metadata_storage->isFile(local_path))
    {
        try
        {
            paths_map.emplace_back(local_path, getStorageObjects(local_path));
        }
        catch (const Exception & e)
        {
            /// Unfortunately in rare cases it can happen when files disappear
            /// or can be empty in case of operation interruption (like cancelled metadata fetch)
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST ||
                e.code() == ErrorCodes::DIRECTORY_DOESNT_EXIST ||
                e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF ||
                e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
                return;

            throw;
        }
    }
    else
    {
        DirectoryIteratorPtr it;
        try
        {
            it = iterateDirectory(local_path);
        }
        catch (const Exception & e)
        {
            /// Unfortunately in rare cases it can happen when files disappear
            /// or can be empty in case of operation interruption (like cancelled metadata fetch)
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST ||
                e.code() == ErrorCodes::DIRECTORY_DOESNT_EXIST ||
                e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF ||
                e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
                return;

            throw;
        }
        catch (const fs::filesystem_error & e)
        {
            if (e.code() == std::errc::no_such_file_or_directory)
                return;
            throw;
        }

        for (; it->isValid(); it->next())
            DiskObjectStorage::getRemotePathsRecursive(fs::path(local_path) / it->name(), paths_map);
    }
}

bool DiskObjectStorage::exists(const String & path) const
{
    return metadata_storage->exists(path);
}


bool DiskObjectStorage::isFile(const String & path) const
{
    return metadata_storage->isFile(path);
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

void DiskObjectStorage::moveFile(const String & from_path, const String & to_path, bool should_send_metadata)
{

    if (should_send_metadata)
    {
        auto revision = metadata_helper->revision_counter + 1;
        metadata_helper->revision_counter += 1;

        const ObjectAttributes object_metadata {
            {"from_path", from_path},
            {"to_path", to_path}
        };
        metadata_helper->createFileOperationObject("rename", revision, object_metadata);
    }

    auto transaction = createObjectStorageTransaction();
    transaction->moveFile(from_path, to_path);
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
            transaction->copyFile(from_file_path, to_file_path);
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
    if (exists(to_path))
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


bool DiskObjectStorage::isDirectory(const String & path) const
{
    return metadata_storage->isDirectory(path);
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
    else
    {
        LOG_TRACE(log, "Could not reserve {} on remote disk {}. Not enough unreserved space", ReadableSize(bytes), backQuote(name));
    }

    return false;
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
static inline Settings updateResourceLink(const Settings & settings, const String & resource_name)
{
    if (resource_name.empty())
        return settings;
    if (auto query_context = CurrentThread::getQueryContext())
    {
        Settings result(settings);
        result.resource_link = query_context->getWorkloadClassifier()->get(resource_name);
        return result;
    }
    return settings;
}

String DiskObjectStorage::getReadResourceName() const
{
    std::unique_lock lock(resource_mutex);
    return read_resource_name;
}

String DiskObjectStorage::getWriteResourceName() const
{
    std::unique_lock lock(resource_mutex);
    return write_resource_name;
}

std::unique_ptr<ReadBufferFromFileBase> DiskObjectStorage::readFile(
    const String & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    auto storage_objects = metadata_storage->getStorageObjects(path);

    const bool file_can_be_empty = !file_size.has_value() || *file_size == 0;

    if (storage_objects.empty() && file_can_be_empty)
        return std::make_unique<ReadBufferFromEmptyFile>(path);

    return object_storage->readObjects(
        storage_objects,
        updateResourceLink(settings, getReadResourceName()),
        read_hint,
        file_size);
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorage::writeFile(
    const String & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    LOG_TEST(log, "Write file: {}", path);

    WriteSettings write_settings = updateResourceLink(settings, getWriteResourceName());
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
    object_storage->applyNewSettings(config, config_prefix, context_);

    {
        std::unique_lock lock(resource_mutex);
        if (String new_read_resource_name = config.getString(config_prefix + ".read_resource", ""); new_read_resource_name != read_resource_name)
            read_resource_name = new_read_resource_name;
        if (String new_write_resource_name = config.getString(config_prefix + ".write_resource", ""); new_write_resource_name != write_resource_name)
            write_resource_name = new_write_resource_name;
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
