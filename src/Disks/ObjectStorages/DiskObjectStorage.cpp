#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Common/checkStackSize.h>
#include <Common/getRandomASCIIString.h>
#include <boost/algorithm/string.hpp>
#include <Common/filesystemHelpers.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Common/FileCache.h>
#include <Disks/ObjectStorages/DiskObjectStorageRemoteMetadataRestoreHelper.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DISK_INDEX;
    extern const int UNKNOWN_FORMAT;
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_FILE_TYPE;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_OPEN_FILE;
}

static String revisionToString(UInt64 revision)
{
    return std::bitset<64>(revision).to_string();
}

namespace
{

/// Runs tasks asynchronously using thread pool.
class AsyncThreadPoolExecutor : public Executor
{
public:
    AsyncThreadPoolExecutor(const String & name_, int thread_pool_size)
        : name(name_)
        , pool(ThreadPool(thread_pool_size)) {}

    std::future<void> execute(std::function<void()> task) override
    {
        auto promise = std::make_shared<std::promise<void>>();
        pool.scheduleOrThrowOnError(
            [promise, task]()
            {
                try
                {
                    task();
                    promise->set_value();
                }
                catch (...)
                {
                    tryLogCurrentException("Failed to run async task");

                    try
                    {
                        promise->set_exception(std::current_exception());
                    }
                    catch (...) {}
                }
            });

        return promise->get_future();
    }

    void setMaxThreads(size_t threads)
    {
        pool.setMaxThreads(threads);
    }

private:
    String name;
    ThreadPool pool;
};

}

DiskObjectStorage::DiskObjectStorage(
    const String & name_,
    const String & remote_fs_root_path_,
    const String & log_name,
    MetadataStoragePtr && metadata_storage_,
    ObjectStoragePtr && object_storage_,
    DiskType disk_type_,
    bool send_metadata_,
    uint64_t thread_pool_size)
    : IDisk(std::make_unique<AsyncThreadPoolExecutor>(log_name, thread_pool_size))
    , name(name_)
    , remote_fs_root_path(remote_fs_root_path_)
    , log (&Poco::Logger::get(log_name))
    , disk_type(disk_type_)
    , metadata_storage(std::move(metadata_storage_))
    , object_storage(std::move(object_storage_))
    , send_metadata(send_metadata_)
    , metadata_helper(std::make_unique<DiskObjectStorageRemoteMetadataRestoreHelper>(this, ReadSettings{}))
{}

std::vector<String> DiskObjectStorage::getRemotePaths(const String & local_path) const
{
    return metadata_storage->getRemotePaths(local_path);
}

void DiskObjectStorage::getRemotePathsRecursive(const String & local_path, std::vector<LocalPathWithRemotePaths> & paths_map)
{
    /// Protect against concurrent delition of files (for example because of a merge).
    if (metadata_storage->isFile(local_path))
    {
        try
        {
            paths_map.emplace_back(local_path, getRemotePaths(local_path));
        }
        catch (const Exception & e)
        {
            /// Unfortunately in rare cases it can happen when files disappear
            /// or can be empty in case of operation interruption (like cancelled metadata fetch)
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST ||
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
                e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF ||
                e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
                return;
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
    auto tx = metadata_storage->createTransaction();
    tx->createEmptyMetadataFile(path);
    tx->commit();
}

size_t DiskObjectStorage::getFileSize(const String & path) const
{
    return metadata_storage->getFileSize(path);
}

void DiskObjectStorage::moveFile(const String & from_path, const String & to_path, bool should_send_metadata)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);

    if (!exists(from_path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} doesn't exist, cannot move", to_path);

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

    auto tx = metadata_storage->createTransaction();
    tx->moveFile(from_path, to_path);
    tx->commit();
}

void DiskObjectStorage::moveFile(const String & from_path, const String & to_path)
{
    moveFile(from_path, to_path, send_metadata);
}

void DiskObjectStorage::replaceFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
    {
        auto blobs = metadata_storage->getRemotePaths(to_path);

        auto tx = metadata_storage->createTransaction();
        tx->replaceFile(from_path, to_path);
        tx->commit();

        removeFromRemoteFS(blobs);
    }
    else
        moveFile(from_path, to_path);
}

void DiskObjectStorage::removeSharedFile(const String & path, bool delete_metadata_only)
{
    std::vector<String> paths_to_remove;
    removeMetadata(path, paths_to_remove);

    if (!delete_metadata_only)
        removeFromRemoteFS(paths_to_remove);
}

void DiskObjectStorage::removeFromRemoteFS(const std::vector<String> & paths)
{
    object_storage->removeObjects(paths);
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
    LOG_TRACE(log, "Remote path: {}, Path: {}", remote_fs_root_path, path);
    String id;
    auto blobs_paths = metadata_storage->getRemotePaths(path);
    if (!blobs_paths.empty())
        id = blobs_paths[0];
    return id;
}

bool DiskObjectStorage::checkObjectExists(const String & path) const
{
    if (!path.starts_with(remote_fs_root_path))
        return false;

    return object_storage->exists(path);
}

bool DiskObjectStorage::checkUniqueId(const String & id) const
{
    return checkObjectExists(id);
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

    /// Create FS hardlink to metadata file.
    auto tx = metadata_storage->createTransaction();
    tx->createHardLink(src_path, dst_path);
    tx->commit();
}

void DiskObjectStorage::createHardLink(const String & src_path, const String & dst_path)
{
    createHardLink(src_path, dst_path, send_metadata);
}


void DiskObjectStorage::setReadOnly(const String & path)
{
    /// We should store read only flag inside metadata file (instead of using FS flag),
    /// because we modify metadata file when create hard-links from it.
    auto tx = metadata_storage->createTransaction();
    tx->setReadOnly(path);
    tx->commit();
}


bool DiskObjectStorage::isDirectory(const String & path) const
{
    return metadata_storage->isDirectory(path);
}


void DiskObjectStorage::createDirectory(const String & path)
{
    auto tx = metadata_storage->createTransaction();
    tx->createDirectory(path);
    tx->commit();
}


void DiskObjectStorage::createDirectories(const String & path)
{
    auto tx = metadata_storage->createTransaction();
    tx->createDicrectoryRecursive(path);
    tx->commit();
}


void DiskObjectStorage::clearDirectory(const String & path)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        if (isFile(it->path()))
            removeFile(it->path());
}


void DiskObjectStorage::removeDirectory(const String & path)
{
    auto tx = metadata_storage->createTransaction();
    tx->removeDirectory(path);
    tx->commit();
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
    auto tx = metadata_storage->createTransaction();
    tx->setLastModified(path, timestamp);
    tx->commit();
}


Poco::Timestamp DiskObjectStorage::getLastModified(const String & path) const
{
    return metadata_storage->getLastModified(path);
}

time_t DiskObjectStorage::getLastChanged(const String & path) const
{
    return metadata_storage->getLastChanged(path);
}

void DiskObjectStorage::removeMetadata(const String & path, std::vector<String> & paths_to_remove)
{
    LOG_TRACE(log, "Remove file by path: {}", backQuote(metadata_storage->getPath() + path));

    if (!metadata_storage->exists(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata path '{}' doesn't exist", path);

    if (!metadata_storage->isFile(path))
        throw Exception(ErrorCodes::BAD_FILE_TYPE, "Path '{}' is not a regular file", path);


    try
    {
        uint32_t hardlink_count = metadata_storage->getHardlinkCount(path);
        auto remote_objects = metadata_storage->getRemotePaths(path);

        auto tx = metadata_storage->createTransaction();
        tx->unlinkMetadata(path);
        tx->commit();

        if (hardlink_count == 0)
        {
            paths_to_remove = remote_objects;
            for (const auto & path_to_remove : paths_to_remove)
                object_storage->removeFromCache(path_to_remove);
        }
    }
    catch (const Exception & e)
    {
        /// If it's impossible to read meta - just remove it from FS.
        if (e.code() == ErrorCodes::UNKNOWN_FORMAT
            || e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF
            || e.code() == ErrorCodes::CANNOT_READ_ALL_DATA
            || e.code() == ErrorCodes::CANNOT_OPEN_FILE)
        {
            LOG_INFO(log, "Failed to read metadata file {} before removal because it's incomplete or empty. "
                     "It's Ok and can happen after operation interruption (like metadata fetch), so removing as is", path);

            auto tx = metadata_storage->createTransaction();
            tx->unlinkFile(path);
            tx->commit();
        }
        else
            throw;
    }
}


void DiskObjectStorage::removeMetadataRecursive(const String & path, std::unordered_map<String, std::vector<String>> & paths_to_remove)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    if (metadata_storage->isFile(path))
    {
        removeMetadata(path, paths_to_remove[path]);
    }
    else
    {
        for (auto it = iterateDirectory(path); it->isValid(); it->next())
            removeMetadataRecursive(it->path(), paths_to_remove);

        auto tx = metadata_storage->createTransaction();
        tx->removeDirectory(path);
        tx->commit();
    }
}


void DiskObjectStorage::shutdown()
{
    LOG_INFO(log, "Shutting down disk {}", name);
    object_storage->shutdown();
    LOG_INFO(log, "Disk {} shut down", name);
}

void DiskObjectStorage::startup(ContextPtr context)
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

    return std::make_unique<DiskObjectStorageReservation>(std::static_pointer_cast<DiskObjectStorage>(shared_from_this()), bytes);
}

void DiskObjectStorage::removeSharedFileIfExists(const String & path, bool delete_metadata_only)
{
    std::vector<String> paths_to_remove;
    if (metadata_storage->exists(path))
    {
        removeMetadata(path, paths_to_remove);
        if (!delete_metadata_only)
            removeFromRemoteFS(paths_to_remove);
    }
}

void DiskObjectStorage::removeSharedRecursive(const String & path, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only)
{
    std::unordered_map<String, std::vector<String>> paths_to_remove;
    removeMetadataRecursive(path, paths_to_remove);

    if (!keep_all_batch_data)
    {
        std::vector<String> remove_from_remote;
        for (auto && [local_path, remote_paths] : paths_to_remove)
        {
            if (!file_names_remove_metadata_only.contains(fs::path(local_path).filename()))
            {
                remove_from_remote.insert(remove_from_remote.end(), remote_paths.begin(), remote_paths.end());
            }
        }
        removeFromRemoteFS(remove_from_remote);
    }
}

std::optional<UInt64> DiskObjectStorage::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(reservation_mutex);

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);

    if (bytes == 0)
    {
        LOG_TRACE(log, "Reserving 0 bytes on remote_fs disk {}", backQuote(name));
        ++reservation_count;
        return {unreserved_space};
    }

    if (unreserved_space >= bytes)
    {
        LOG_TRACE(log, "Reserving {} on disk {}, having unreserved {}.",
            ReadableSize(bytes), backQuote(name), ReadableSize(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return {unreserved_space - bytes};
    }

    return {};
}

std::unique_ptr<ReadBufferFromFileBase> DiskObjectStorage::readFile(
    const String & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    return object_storage->readObjects(remote_fs_root_path, metadata_storage->getBlobs(path), settings, read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorage::writeFile(
    const String & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    auto blob_name = getRandomASCIIString();

    std::optional<ObjectAttributes> object_attributes;
    if (send_metadata)
    {
        auto revision = metadata_helper->revision_counter + 1;
        metadata_helper->revision_counter++;
        object_attributes = {
            {"path", path}
        };
        blob_name = "r" + revisionToString(revision) + "-file-" + blob_name;
    }

    auto create_metadata_callback = [this, mode, path, blob_name] (size_t count)
    {
        auto tx = metadata_storage->createTransaction();
        if (mode == WriteMode::Rewrite)
            tx->createMetadataFile(path, blob_name, count);
        else
            tx->addBlobToMetadata(path, blob_name, count);

        tx->commit();
    };

    /// We always use mode Rewrite because we simulate append using metadata and different files
    return object_storage->writeObject(
        fs::path(remote_fs_root_path) / blob_name, WriteMode::Rewrite, object_attributes,
        std::move(create_metadata_callback),
        buf_size, settings);
}


void DiskObjectStorage::applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context_, const String &, const DisksMap &)
{
    const auto config_prefix = "storage_configuration.disks." + name;
    object_storage->applyNewSettings(config, config_prefix, context_);

    if (AsyncThreadPoolExecutor * exec = dynamic_cast<AsyncThreadPoolExecutor *>(&getExecutor()))
        exec->setMaxThreads(config.getInt(config_prefix + ".thread_pool_size", 16));
}

void DiskObjectStorage::restoreMetadataIfNeeded(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    if (send_metadata)
    {
        metadata_helper->restore(config, config_prefix, context);

        if (metadata_helper->readSchemaVersion(object_storage.get(), remote_fs_root_path) < DiskObjectStorageRemoteMetadataRestoreHelper::RESTORABLE_SCHEMA_VERSION)
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
        throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
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
