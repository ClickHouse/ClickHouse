#include <Disks/DiskObjectStorage.h>

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

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DISK_INDEX;
    extern const int UNKNOWN_FORMAT;
    extern const int FILE_ALREADY_EXISTS;
    extern const int PATH_ACCESS_DENIED;;
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_FILE_TYPE;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
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
    DiskPtr metadata_disk_,
    ObjectStoragePtr && object_storage_,
    DiskType disk_type_,
    bool send_metadata_,
    uint64_t thread_pool_size)
    : IDisk(std::make_unique<AsyncThreadPoolExecutor>(log_name, thread_pool_size))
    , name(name_)
    , remote_fs_root_path(remote_fs_root_path_)
    , log (&Poco::Logger::get(log_name))
    , metadata_disk(metadata_disk_)
    , disk_type(disk_type_)
    , object_storage(std::move(object_storage_))
    , send_metadata(send_metadata_)
    , metadata_helper(std::make_unique<DiskObjectStorageMetadataHelper>(this, ReadSettings{}))
{}

DiskObjectStorage::Metadata DiskObjectStorage::Metadata::readMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_)
{
    Metadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.load();
    return result;
}


DiskObjectStorage::Metadata DiskObjectStorage::Metadata::createAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync)
{
    Metadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.save(sync);
    return result;
}

DiskObjectStorage::Metadata DiskObjectStorage::Metadata::readUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, DiskObjectStorage::MetadataUpdater updater)
{
    Metadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.load();
    if (updater(result))
        result.save(sync);
    return result;
}

DiskObjectStorage::Metadata DiskObjectStorage::Metadata::createUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, DiskObjectStorage::MetadataUpdater updater)
{
    Metadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    updater(result);
    result.save(sync);
    return result;
}

DiskObjectStorage::Metadata DiskObjectStorage::Metadata::readUpdateStoreMetadataAndRemove(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, DiskObjectStorage::MetadataUpdater updater)
{
    Metadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.load();
    if (updater(result))
        result.save(sync);
    metadata_disk_->removeFile(metadata_file_path_);

    return result;

}

DiskObjectStorage::Metadata DiskObjectStorage::Metadata::createAndStoreMetadataIfNotExists(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, bool overwrite)
{
    if (overwrite || !metadata_disk_->exists(metadata_file_path_))
    {
        return createAndStoreMetadata(remote_fs_root_path_, metadata_disk_, metadata_file_path_, sync);
    }
    else
    {
        auto result = readMetadata(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
        if (result.read_only)
            throw Exception("File is read-only: " + metadata_file_path_, ErrorCodes::PATH_ACCESS_DENIED);
        return result;
    }
}

void DiskObjectStorage::Metadata::load()
{
    try
    {
        const ReadSettings read_settings;
        auto buf = metadata_disk->readFile(metadata_file_path, read_settings, 1024);  /* reasonable buffer size for small file */

        UInt32 version;
        readIntText(version, *buf);

        if (version < VERSION_ABSOLUTE_PATHS || version > VERSION_READ_ONLY_FLAG)
            throw Exception(
                ErrorCodes::UNKNOWN_FORMAT,
                "Unknown metadata file version. Path: {}. Version: {}. Maximum expected version: {}",
                metadata_disk->getPath() + metadata_file_path, toString(version), toString(VERSION_READ_ONLY_FLAG));

        assertChar('\n', *buf);

        UInt32 remote_fs_objects_count;
        readIntText(remote_fs_objects_count, *buf);
        assertChar('\t', *buf);
        readIntText(total_size, *buf);
        assertChar('\n', *buf);
        remote_fs_objects.resize(remote_fs_objects_count);

        for (size_t i = 0; i < remote_fs_objects_count; ++i)
        {
            String remote_fs_object_path;
            size_t remote_fs_object_size;
            readIntText(remote_fs_object_size, *buf);
            assertChar('\t', *buf);
            readEscapedString(remote_fs_object_path, *buf);
            if (version == VERSION_ABSOLUTE_PATHS)
            {
                if (!remote_fs_object_path.starts_with(remote_fs_root_path))
                    throw Exception(ErrorCodes::UNKNOWN_FORMAT,
                        "Path in metadata does not correspond to root path. Path: {}, root path: {}, disk path: {}",
                        remote_fs_object_path, remote_fs_root_path, metadata_disk->getPath());

                remote_fs_object_path = remote_fs_object_path.substr(remote_fs_root_path.size());
            }
            assertChar('\n', *buf);
            remote_fs_objects[i].relative_path = remote_fs_object_path;
            remote_fs_objects[i].bytes_size = remote_fs_object_size;
        }

        readIntText(ref_count, *buf);
        assertChar('\n', *buf);

        if (version >= VERSION_READ_ONLY_FLAG)
        {
            readBoolText(read_only, *buf);
            assertChar('\n', *buf);
        }
    }
    catch (Exception & e)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        if (e.code() == ErrorCodes::UNKNOWN_FORMAT)
            throw;

        if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            throw;

        throw Exception("Failed to read metadata file: " + metadata_file_path, ErrorCodes::UNKNOWN_FORMAT);
    }
}

/// Load metadata by path or create empty if `create` flag is set.
DiskObjectStorage::Metadata::Metadata(
        const String & remote_fs_root_path_,
        DiskPtr metadata_disk_,
        const String & metadata_file_path_)
    : remote_fs_root_path(remote_fs_root_path_)
    , metadata_file_path(metadata_file_path_)
    , metadata_disk(metadata_disk_)
    , total_size(0), ref_count(0)
{
}

void DiskObjectStorage::Metadata::addObject(const String & path, size_t size)
{
    total_size += size;
    remote_fs_objects.emplace_back(path, size);
}


void DiskObjectStorage::Metadata::saveToBuffer(WriteBuffer & buf, bool sync)
{
    writeIntText(VERSION_RELATIVE_PATHS, buf);
    writeChar('\n', buf);

    writeIntText(remote_fs_objects.size(), buf);
    writeChar('\t', buf);
    writeIntText(total_size, buf);
    writeChar('\n', buf);

    for (const auto & [remote_fs_object_path, remote_fs_object_size] : remote_fs_objects)
    {
        writeIntText(remote_fs_object_size, buf);
        writeChar('\t', buf);
        writeEscapedString(remote_fs_object_path, buf);
        writeChar('\n', buf);
    }

    writeIntText(ref_count, buf);
    writeChar('\n', buf);

    writeBoolText(read_only, buf);
    writeChar('\n', buf);

    buf.finalize();
    if (sync)
        buf.sync();

}

/// Fsync metadata file if 'sync' flag is set.
void DiskObjectStorage::Metadata::save(bool sync)
{
    auto buf = metadata_disk->writeFile(metadata_file_path, 1024);
    saveToBuffer(*buf, sync);
}

std::string DiskObjectStorage::Metadata::serializeToString()
{
    WriteBufferFromOwnString write_buf;
    saveToBuffer(write_buf, false);
    return write_buf.str();
}

DiskObjectStorage::Metadata DiskObjectStorage::readMetadataUnlocked(const String & path, std::shared_lock<std::shared_mutex> &) const
{
    return Metadata::readMetadata(remote_fs_root_path, metadata_disk, path);
}


DiskObjectStorage::Metadata DiskObjectStorage::readMetadata(const String & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path, lock);
}

DiskObjectStorage::Metadata DiskObjectStorage::readUpdateAndStoreMetadata(const String & path, bool sync, DiskObjectStorage::MetadataUpdater updater)
{
    std::unique_lock lock(metadata_mutex);
    return Metadata::readUpdateAndStoreMetadata(remote_fs_root_path, metadata_disk, path, sync, updater);
}


DiskObjectStorage::Metadata DiskObjectStorage::readUpdateStoreMetadataAndRemove(const String & path, bool sync, DiskObjectStorage::MetadataUpdater updater)
{
    std::unique_lock lock(metadata_mutex);
    return Metadata::readUpdateStoreMetadataAndRemove(remote_fs_root_path, metadata_disk, path, sync, updater);
}

DiskObjectStorage::Metadata DiskObjectStorage::readOrCreateUpdateAndStoreMetadata(const String & path, WriteMode mode, bool sync, DiskObjectStorage::MetadataUpdater updater)
{
    if (mode == WriteMode::Rewrite || !metadata_disk->exists(path))
    {
        std::unique_lock lock(metadata_mutex);
        return Metadata::createUpdateAndStoreMetadata(remote_fs_root_path, metadata_disk, path, sync, updater);
    }
    else
    {
        return Metadata::readUpdateAndStoreMetadata(remote_fs_root_path, metadata_disk, path, sync, updater);
    }
}

DiskObjectStorage::Metadata DiskObjectStorage::createAndStoreMetadata(const String & path, bool sync)
{
    return Metadata::createAndStoreMetadata(remote_fs_root_path, metadata_disk, path, sync);
}

DiskObjectStorage::Metadata DiskObjectStorage::createUpdateAndStoreMetadata(const String & path, bool sync, DiskObjectStorage::MetadataUpdater updater)
{
    return Metadata::createUpdateAndStoreMetadata(remote_fs_root_path, metadata_disk, path, sync, updater);
}

std::vector<String> DiskObjectStorage::getRemotePaths(const String & local_path) const
{
    auto metadata = readMetadata(local_path);

    std::vector<String> remote_paths;
    for (const auto & [remote_path, _] : metadata.remote_fs_objects)
        remote_paths.push_back(fs::path(metadata.remote_fs_root_path) / remote_path);

    return remote_paths;

}

void DiskObjectStorage::getRemotePathsRecursive(const String & local_path, std::vector<LocalPathWithRemotePaths> & paths_map)
{
    /// Protect against concurrent delition of files (for example because of a merge).
    if (metadata_disk->isFile(local_path))
    {
        try
        {
            paths_map.emplace_back(local_path, getRemotePaths(local_path));
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
                return;
            throw;
        }
    }
    else
    {
        DiskDirectoryIteratorPtr it;
        try
        {
            it = iterateDirectory(local_path);
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
    return metadata_disk->exists(path);
}


bool DiskObjectStorage::isFile(const String & path) const
{
    return metadata_disk->isFile(path);
}


void DiskObjectStorage::createFile(const String & path)
{
    createAndStoreMetadata(path, false);
}

size_t DiskObjectStorage::getFileSize(const String & path) const
{
    return readMetadata(path).total_size;
}

void DiskObjectStorage::moveFile(const String & from_path, const String & to_path, bool should_send_metadata)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);

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

    metadata_disk->moveFile(from_path, to_path);
}

void DiskObjectStorage::moveFile(const String & from_path, const String & to_path)
{
    moveFile(from_path, to_path, send_metadata);
}

void DiskObjectStorage::replaceFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
    {
        const String tmp_path = to_path + ".old";
        moveFile(to_path, tmp_path);
        moveFile(from_path, to_path);
        removeFile(tmp_path);
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
    return readMetadata(path).ref_count;
}

std::unordered_map<String, String> DiskObjectStorage::getSerializedMetadata(const std::vector<String> & file_paths) const
{
    std::unordered_map<String, String> metadatas;

    std::shared_lock lock(metadata_mutex);

    for (const auto & path : file_paths)
    {
        DiskObjectStorage::Metadata metadata = readMetadataUnlocked(path, lock);
        metadata.ref_count = 0;
        metadatas[path] = metadata.serializeToString();
    }

    return metadatas;
}

String DiskObjectStorage::getUniqueId(const String & path) const
{
    LOG_TRACE(log, "Remote path: {}, Path: {}", remote_fs_root_path, path);
    auto metadata = readMetadata(path);
    String id;
    if (!metadata.remote_fs_objects.empty())
        id = metadata.remote_fs_root_path + metadata.remote_fs_objects[0].relative_path;
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
    readUpdateAndStoreMetadata(src_path, false, [](Metadata & metadata) { metadata.ref_count++; return true; });

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
    metadata_disk->createHardLink(src_path, dst_path);
}

void DiskObjectStorage::createHardLink(const String & src_path, const String & dst_path)
{
    createHardLink(src_path, dst_path, send_metadata);
}


void DiskObjectStorage::setReadOnly(const String & path)
{
    /// We should store read only flag inside metadata file (instead of using FS flag),
    /// because we modify metadata file when create hard-links from it.
    readUpdateAndStoreMetadata(path, false, [](Metadata & metadata) { metadata.read_only = true; return true; });
}


bool DiskObjectStorage::isDirectory(const String & path) const
{
    return metadata_disk->isDirectory(path);
}


void DiskObjectStorage::createDirectory(const String & path)
{
    metadata_disk->createDirectory(path);
}


void DiskObjectStorage::createDirectories(const String & path)
{
    metadata_disk->createDirectories(path);
}


void DiskObjectStorage::clearDirectory(const String & path)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        if (isFile(it->path()))
            removeFile(it->path());
}


void DiskObjectStorage::removeDirectory(const String & path)
{
    metadata_disk->removeDirectory(path);
}


DiskDirectoryIteratorPtr DiskObjectStorage::iterateDirectory(const String & path)
{
    return metadata_disk->iterateDirectory(path);
}


void DiskObjectStorage::listFiles(const String & path, std::vector<String> & file_names)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        file_names.push_back(it->name());
}


void DiskObjectStorage::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    metadata_disk->setLastModified(path, timestamp);
}


Poco::Timestamp DiskObjectStorage::getLastModified(const String & path)
{
    return metadata_disk->getLastModified(path);
}

void DiskObjectStorage::removeMetadata(const String & path, std::vector<String> & paths_to_remove)
{
    LOG_TRACE(log, "Remove file by path: {}", backQuote(metadata_disk->getPath() + path));

    if (!metadata_disk->exists(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata path '{}' doesn't exist", path);

    if (!metadata_disk->isFile(path))
        throw Exception(ErrorCodes::BAD_FILE_TYPE, "Path '{}' is not a regular file", path);

    try
    {
        auto metadata_updater = [&paths_to_remove, this] (Metadata & metadata)
        {
            if (metadata.ref_count == 0)
            {
                for (const auto & [remote_fs_object_path, _] : metadata.remote_fs_objects)
                {
                    paths_to_remove.push_back(fs::path(remote_fs_root_path) / remote_fs_object_path);
                    object_storage->removeFromCache(fs::path(remote_fs_root_path) / remote_fs_object_path);
                }

                return false;
            }
            else /// In other case decrement number of references, save metadata and delete hardlink.
            {
                --metadata.ref_count;
            }

            return true;
        };

        readUpdateStoreMetadataAndRemove(path, false, metadata_updater);
        /// If there is no references - delete content from remote FS.
    }
    catch (const Exception & e)
    {
        /// If it's impossible to read meta - just remove it from FS.
        if (e.code() == ErrorCodes::UNKNOWN_FORMAT)
        {
            LOG_WARNING(log,
                "Metadata file {} can't be read by reason: {}. Removing it forcibly.",
                backQuote(path), e.nested() ? e.nested()->message() : e.message());
            metadata_disk->removeFile(path);
        }
        else
            throw;
    }
}


void DiskObjectStorage::removeMetadataRecursive(const String & path, std::unordered_map<String, std::vector<String>> & paths_to_remove)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    if (metadata_disk->isFile(path))
    {
        removeMetadata(path, paths_to_remove[path]);
    }
    else
    {
        for (auto it = iterateDirectory(path); it->isValid(); it->next())
            removeMetadataRecursive(it->path(), paths_to_remove);

        metadata_disk->removeDirectory(path);
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
    if (metadata_disk->exists(path))
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

std::optional<UInt64>  DiskObjectStorage::tryReserve(UInt64 bytes)
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
    auto metadata = readMetadata(path);
    return object_storage->readObjects(remote_fs_root_path, metadata.remote_fs_objects, settings, read_hint, file_size);
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

    auto create_metadata_callback = [this, path, blob_name, mode] (size_t count)
    {
        readOrCreateUpdateAndStoreMetadata(path, mode, false,
            [blob_name, count] (DiskObjectStorage::Metadata & metadata) { metadata.addObject(blob_name, count); return true; });
    };

    return object_storage->writeObject(fs::path(remote_fs_root_path) / blob_name, mode, object_attributes, std::move(create_metadata_callback), buf_size, settings);
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

        if (metadata_helper->readSchemaVersion(object_storage.get(), remote_fs_root_path) < DiskObjectStorageMetadataHelper::RESTORABLE_SCHEMA_VERSION)
            metadata_helper->migrateToRestorableSchema();

        metadata_helper->findLastRevision();
    }
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


void DiskObjectStorageMetadataHelper::createFileOperationObject(const String & operation_name, UInt64 revision, const ObjectAttributes & metadata) const
{
    const String path = disk->remote_fs_root_path + "operations/r" + revisionToString(revision) + "-" + operation_name;
    auto buf = disk->object_storage->writeObject(path, WriteMode::Rewrite, metadata);
    buf->write('0');
    buf->finalize();
}

void DiskObjectStorageMetadataHelper::findLastRevision()
{
    /// Construct revision number from high to low bits.
    String revision;
    revision.reserve(64);
    for (int bit = 0; bit < 64; ++bit)
    {
        auto revision_prefix = revision + "1";

        LOG_TRACE(disk->log, "Check object exists with revision prefix {}", revision_prefix);

        /// Check file or operation with such revision prefix exists.
        if (disk->object_storage->exists(disk->remote_fs_root_path + "r" + revision_prefix)
            || disk->object_storage->exists(disk->remote_fs_root_path + "operations/r" + revision_prefix))
            revision += "1";
        else
            revision += "0";
    }
    revision_counter = static_cast<UInt64>(std::bitset<64>(revision).to_ullong());
    LOG_INFO(disk->log, "Found last revision number {} for disk {}", revision_counter, disk->name);
}

int DiskObjectStorageMetadataHelper::readSchemaVersion(IObjectStorage * object_storage, const String & source_path)
{
    const std::string path = source_path + SCHEMA_VERSION_OBJECT;
    int version = 0;
    if (!object_storage->exists(path))
        return version;

    auto buf = object_storage->readObject(path);
    readIntText(version, *buf);

    return version;
}

void DiskObjectStorageMetadataHelper::saveSchemaVersion(const int & version) const
{
    auto path = disk->remote_fs_root_path + SCHEMA_VERSION_OBJECT;

    auto buf = disk->object_storage->writeObject(path, WriteMode::Rewrite);
    writeIntText(version, *buf);
    buf->finalize();

}

void DiskObjectStorageMetadataHelper::updateObjectMetadata(const String & key, const ObjectAttributes & metadata) const
{
    disk->object_storage->copyObject(key, key, metadata);
}

void DiskObjectStorageMetadataHelper::migrateFileToRestorableSchema(const String & path) const
{
    LOG_TRACE(disk->log, "Migrate file {} to restorable schema", disk->metadata_disk->getPath() + path);

    auto meta = disk->readMetadata(path);

    for (const auto & [key, _] : meta.remote_fs_objects)
    {
        ObjectAttributes metadata {
            {"path", path}
        };
        updateObjectMetadata(disk->remote_fs_root_path + key, metadata);
    }
}
void DiskObjectStorageMetadataHelper::migrateToRestorableSchemaRecursive(const String & path, Futures & results)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    LOG_TRACE(disk->log, "Migrate directory {} to restorable schema", disk->metadata_disk->getPath() + path);

    bool dir_contains_only_files = true;
    for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
    {
        if (disk->isDirectory(it->path()))
        {
            dir_contains_only_files = false;
            break;
        }
    }

    /// The whole directory can be migrated asynchronously.
    if (dir_contains_only_files)
    {
        auto result = disk->getExecutor().execute([this, path]
        {
            for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
                migrateFileToRestorableSchema(it->path());
        });

        results.push_back(std::move(result));
    }
    else
    {
        for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
            if (!disk->isDirectory(it->path()))
            {
                auto source_path = it->path();
                auto result = disk->getExecutor().execute([this, source_path]
                    {
                        migrateFileToRestorableSchema(source_path);
                    });

                results.push_back(std::move(result));
            }
            else
                migrateToRestorableSchemaRecursive(it->path(), results);
    }

}

void DiskObjectStorageMetadataHelper::migrateToRestorableSchema()
{
    try
    {
        LOG_INFO(disk->log, "Start migration to restorable schema for disk {}", disk->name);

        Futures results;

        for (const auto & root : data_roots)
            if (disk->exists(root))
                migrateToRestorableSchemaRecursive(root + '/', results);

        for (auto & result : results)
            result.wait();
        for (auto & result : results)
            result.get();

        saveSchemaVersion(RESTORABLE_SCHEMA_VERSION);
    }
    catch (const Exception &)
    {
        tryLogCurrentException(disk->log, fmt::format("Failed to migrate to restorable schema for disk {}", disk->name));

        throw;
    }
}

void DiskObjectStorageMetadataHelper::restore(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    LOG_INFO(disk->log, "Restore operation for disk {} called", disk->name);

    if (!disk->exists(RESTORE_FILE_NAME))
    {
        LOG_INFO(disk->log, "No restore file '{}' exists, finishing restore", RESTORE_FILE_NAME);
        return;
    }

    try
    {
        RestoreInformation information;
        information.source_path = disk->remote_fs_root_path;
        information.source_namespace = disk->object_storage->getObjectsNamespace();

        readRestoreInformation(information);
        if (information.revision == 0)
            information.revision = LATEST_REVISION;
        if (!information.source_path.ends_with('/'))
            information.source_path += '/';

        IObjectStorage * source_object_storage = disk->object_storage.get();
        if (information.source_namespace == disk->object_storage->getObjectsNamespace())
        {
            /// In this case we need to additionally cleanup S3 from objects with later revision.
            /// Will be simply just restore to different path.
            if (information.source_path == disk->remote_fs_root_path && information.revision != LATEST_REVISION)
                throw Exception("Restoring to the same bucket and path is allowed if revision is latest (0)", ErrorCodes::BAD_ARGUMENTS);

            /// This case complicates S3 cleanup in case of unsuccessful restore.
            if (information.source_path != disk->remote_fs_root_path && disk->remote_fs_root_path.starts_with(information.source_path))
                throw Exception("Restoring to the same bucket is allowed only if source path is not a sub-path of configured path in S3 disk", ErrorCodes::BAD_ARGUMENTS);
        }
        else
        {
            object_storage_from_another_namespace = disk->object_storage->cloneObjectStorage(information.source_namespace, config, config_prefix, context);
            source_object_storage = object_storage_from_another_namespace.get();
        }

        LOG_INFO(disk->log, "Starting to restore disk {}. Revision: {}, Source path: {}",
                 disk->name, information.revision, information.source_path);

        if (readSchemaVersion(source_object_storage, information.source_path) < RESTORABLE_SCHEMA_VERSION)
            throw Exception("Source bucket doesn't have restorable schema.", ErrorCodes::BAD_ARGUMENTS);

        LOG_INFO(disk->log, "Removing old metadata...");

        bool cleanup_s3 = information.source_path != disk->remote_fs_root_path;
        for (const auto & root : data_roots)
            if (disk->exists(root))
                disk->removeSharedRecursive(root + '/', !cleanup_s3, {});

        LOG_INFO(disk->log, "Old metadata removed, restoring new one");
        restoreFiles(source_object_storage, information);
        restoreFileOperations(source_object_storage, information);

        disk->metadata_disk->removeFile(RESTORE_FILE_NAME);

        saveSchemaVersion(RESTORABLE_SCHEMA_VERSION);

        LOG_INFO(disk->log, "Restore disk {} finished", disk->name);
    }
    catch (const Exception &)
    {
        tryLogCurrentException(disk->log, fmt::format("Failed to restore disk {}", disk->name));

        throw;
    }
}

void DiskObjectStorageMetadataHelper::readRestoreInformation(RestoreInformation & restore_information) /// NOLINT
{
    auto buffer = disk->metadata_disk->readFile(RESTORE_FILE_NAME, ReadSettings{}, 512);
    buffer->next();

    try
    {
        std::map<String, String> properties;

        while (buffer->hasPendingData())
        {
            String property;
            readText(property, *buffer);
            assertChar('\n', *buffer);

            auto pos = property.find('=');
            if (pos == std::string::npos || pos == 0 || pos == property.length())
                throw Exception(fmt::format("Invalid property {} in restore file", property), ErrorCodes::UNKNOWN_FORMAT);

            auto key = property.substr(0, pos);
            auto value = property.substr(pos + 1);

            auto it = properties.find(key);
            if (it != properties.end())
                throw Exception(fmt::format("Property key duplication {} in restore file", key), ErrorCodes::UNKNOWN_FORMAT);

            properties[key] = value;
        }

        for (const auto & [key, value] : properties)
        {
            ReadBufferFromString value_buffer(value);

            if (key == "revision")
                readIntText(restore_information.revision, value_buffer);
            else if (key == "source_bucket" || key == "source_namespace")
                readText(restore_information.source_namespace, value_buffer);
            else if (key == "source_path")
                readText(restore_information.source_path, value_buffer);
            else if (key == "detached")
                readBoolTextWord(restore_information.detached, value_buffer);
            else
                throw Exception(fmt::format("Unknown key {} in restore file", key), ErrorCodes::UNKNOWN_FORMAT);
        }
    }
    catch (const Exception &)
    {
        tryLogCurrentException(disk->log, "Failed to read restore information");
        throw;
    }
}

static String shrinkKey(const String & path, const String & key)
{
    if (!key.starts_with(path))
        throw Exception("The key " + key + " prefix mismatch with given " + path, ErrorCodes::LOGICAL_ERROR);

    return key.substr(path.length());
}

static std::tuple<UInt64, String> extractRevisionAndOperationFromKey(const String & key)
{
    String revision_str;
    String operation;
    /// Key has format: ../../r{revision}-{operation}
    static const re2::RE2 key_regexp {".*/r(\\d+)-(\\w+)$"};

    re2::RE2::FullMatch(key, key_regexp, &revision_str, &operation);

    return {(revision_str.empty() ? 0 : static_cast<UInt64>(std::bitset<64>(revision_str).to_ullong())), operation};
}

void DiskObjectStorageMetadataHelper::restoreFiles(IObjectStorage * source_object_storage, const RestoreInformation & restore_information)
{
    LOG_INFO(disk->log, "Starting restore files for disk {}", disk->name);

    std::vector<std::future<void>> results;
    auto restore_files = [this, &source_object_storage, &restore_information, &results](const BlobsPathToSize & keys)
    {
        std::vector<String> keys_names;
        for (const auto & [key, size] : keys)
        {

            LOG_INFO(disk->log, "Calling restore for key for disk {}", key);

            /// Skip file operations objects. They will be processed separately.
            if (key.find("/operations/") != String::npos)
                continue;

            const auto [revision, _] = extractRevisionAndOperationFromKey(key);
            /// Filter early if it's possible to get revision from key.
            if (revision > restore_information.revision)
                continue;

            keys_names.push_back(key);
        }

        if (!keys_names.empty())
        {
            auto result = disk->getExecutor().execute([this, &source_object_storage, &restore_information, keys_names]()
            {
                processRestoreFiles(source_object_storage, restore_information.source_path, keys_names);
            });

            results.push_back(std::move(result));
        }

        return true;
    };

    BlobsPathToSize children;
    source_object_storage->listPrefix(restore_information.source_path, children);

    restore_files(children);

    for (auto & result : results)
        result.wait();
    for (auto & result : results)
        result.get();

    LOG_INFO(disk->log, "Files are restored for disk {}", disk->name);

}

void DiskObjectStorageMetadataHelper::processRestoreFiles(IObjectStorage * source_object_storage, const String & source_path, const std::vector<String> & keys) const
{
    for (const auto & key : keys)
    {
        auto meta = source_object_storage->getObjectMetadata(key);
        auto object_attributes = meta.attributes;

        String path;
        if (object_attributes.has_value())
        {
            /// Restore file if object has 'path' in metadata.
            auto path_entry = object_attributes->find("path");
            if (path_entry == object_attributes->end())
            {
                /// Such keys can remain after migration, we can skip them.
                LOG_WARNING(disk->log, "Skip key {} because it doesn't have 'path' in metadata", key);
                continue;
            }

            path = path_entry->second;
        }
        else
            continue;


        disk->createDirectories(directoryPath(path));
        auto relative_key = shrinkKey(source_path, key);

        /// Copy object if we restore to different bucket / path.
        if (source_object_storage->getObjectsNamespace() != disk->object_storage->getObjectsNamespace() || disk->remote_fs_root_path != source_path)
            source_object_storage->copyObjectToAnotherObjectStorage(key, disk->remote_fs_root_path + relative_key, *disk->object_storage);

        auto updater = [relative_key, meta] (DiskObjectStorage::Metadata & metadata)
        {
            metadata.addObject(relative_key, meta.size_bytes);
            return true;
        };

        disk->createUpdateAndStoreMetadata(path, false, updater);

        LOG_TRACE(disk->log, "Restored file {}", path);
    }

}

void DiskObjectStorage::onFreeze(const String & path)
{
    createDirectories(path);
    auto revision_file_buf = metadata_disk->writeFile(path + "revision.txt", 32);
    writeIntText(metadata_helper->revision_counter.load(), *revision_file_buf);
    revision_file_buf->finalize();
}

static String pathToDetached(const String & source_path)
{
    if (source_path.ends_with('/'))
        return fs::path(source_path).parent_path().parent_path() / "detached/";
    return fs::path(source_path).parent_path() / "detached/";
}

void DiskObjectStorageMetadataHelper::restoreFileOperations(IObjectStorage * source_object_storage, const RestoreInformation & restore_information)
{
    /// Enable recording file operations if we restore to different bucket / path.
    bool send_metadata = source_object_storage->getObjectsNamespace() != disk->object_storage->getObjectsNamespace() || disk->remote_fs_root_path != restore_information.source_path;

    std::set<String> renames;
    auto restore_file_operations = [this, &source_object_storage, &restore_information, &renames, &send_metadata](const BlobsPathToSize & keys)
    {
        const String rename = "rename";
        const String hardlink = "hardlink";

        for (const auto & [key, _]: keys)
        {
            const auto [revision, operation] = extractRevisionAndOperationFromKey(key);
            if (revision == UNKNOWN_REVISION)
            {
                LOG_WARNING(disk->log, "Skip key {} with unknown revision", key);
                continue;
            }

            /// S3 ensures that keys will be listed in ascending UTF-8 bytes order (revision order).
            /// We can stop processing if revision of the object is already more than required.
            if (revision > restore_information.revision)
                return false;

            /// Keep original revision if restore to different bucket / path.
            if (send_metadata)
                revision_counter = revision - 1;

            auto object_attributes = *(source_object_storage->getObjectMetadata(key).attributes);
            if (operation == rename)
            {
                auto from_path = object_attributes["from_path"];
                auto to_path = object_attributes["to_path"];
                if (disk->exists(from_path))
                {
                    disk->moveFile(from_path, to_path, send_metadata);

                    LOG_TRACE(disk->log, "Revision {}. Restored rename {} -> {}", revision, from_path, to_path);

                    if (restore_information.detached && disk->isDirectory(to_path))
                    {
                        /// Sometimes directory paths are passed without trailing '/'. We should keep them in one consistent way.
                        if (!from_path.ends_with('/'))
                            from_path += '/';
                        if (!to_path.ends_with('/'))
                            to_path += '/';

                        /// Always keep latest actual directory path to avoid 'detaching' not existing paths.
                        auto it = renames.find(from_path);
                        if (it != renames.end())
                            renames.erase(it);

                        renames.insert(to_path);
                    }
                }
            }
            else if (operation == hardlink)
            {
                auto src_path = object_attributes["src_path"];
                auto dst_path = object_attributes["dst_path"];
                if (disk->exists(src_path))
                {
                    disk->createDirectories(directoryPath(dst_path));
                    disk->createHardLink(src_path, dst_path, send_metadata);
                    LOG_TRACE(disk->log, "Revision {}. Restored hardlink {} -> {}", revision, src_path, dst_path);
                }
            }
        }

        return true;
    };

    BlobsPathToSize children;
    source_object_storage->listPrefix(restore_information.source_path + "operations/", children);
    restore_file_operations(children);

    if (restore_information.detached)
    {
        Strings not_finished_prefixes{"tmp_", "delete_tmp_", "attaching_", "deleting_"};

        for (const auto & path : renames)
        {
            /// Skip already detached parts.
            if (path.find("/detached/") != std::string::npos)
                continue;

            /// Skip not finished parts. They shouldn't be in 'detached' directory, because CH wouldn't be able to finish processing them.
            fs::path directory_path(path);
            auto directory_name = directory_path.parent_path().filename().string();

            auto predicate = [&directory_name](String & prefix) { return directory_name.starts_with(prefix); };
            if (std::any_of(not_finished_prefixes.begin(), not_finished_prefixes.end(), predicate))
                continue;

            auto detached_path = pathToDetached(path);

            LOG_TRACE(disk->log, "Move directory to 'detached' {} -> {}", path, detached_path);

            fs::path from_path = fs::path(path);
            fs::path to_path = fs::path(detached_path);
            if (path.ends_with('/'))
                to_path /= from_path.parent_path().filename();
            else
                to_path /= from_path.filename();

            /// to_path may exist and non-empty in case for example abrupt restart, so remove it before rename
            if (disk->metadata_disk->exists(to_path))
                disk->metadata_disk->removeRecursive(to_path);

            disk->createDirectories(directoryPath(to_path));
            disk->metadata_disk->moveDirectory(from_path, to_path);
        }
    }

    LOG_INFO(disk->log, "File operations restored for disk {}", disk->name);
}

}
