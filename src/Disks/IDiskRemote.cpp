#include <Disks/IDiskRemote.h>

#include "Disks/DiskFactory.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <base/logger_useful.h>
#include <Common/checkStackSize.h>
#include <boost/algorithm/string.hpp>
#include <Common/filesystemHelpers.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Common/FileCache.h>


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
}


IDiskRemote::Metadata IDiskRemote::Metadata::readMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_)
{
    Metadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.load();
    return result;
}


IDiskRemote::Metadata IDiskRemote::Metadata::createAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync)
{
    Metadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.save(sync);
    return result;
}


IDiskRemote::Metadata IDiskRemote::Metadata::readUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, IDiskRemote::MetadataUpdater updater)
{
    Metadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.load();
    if (updater(result))
        result.save(sync);
    return result;
}


IDiskRemote::Metadata IDiskRemote::Metadata::createUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, IDiskRemote::MetadataUpdater updater)
{
    Metadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    updater(result);
    result.save(sync);
    return result;
}


IDiskRemote::Metadata IDiskRemote::Metadata::createAndStoreMetadataIfNotExists(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, bool overwrite)
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

void IDiskRemote::Metadata::load()
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

        throw Exception("Failed to read metadata file: " + metadata_file_path, e, ErrorCodes::UNKNOWN_FORMAT);
    }
}

/// Load metadata by path or create empty if `create` flag is set.
IDiskRemote::Metadata::Metadata(
        const String & remote_fs_root_path_,
        DiskPtr metadata_disk_,
        const String & metadata_file_path_)
    : RemoteMetadata(remote_fs_root_path_, metadata_file_path_)
    , metadata_disk(metadata_disk_)
    , total_size(0), ref_count(0)
{
}

void IDiskRemote::Metadata::addObject(const String & path, size_t size)
{
    total_size += size;
    remote_fs_objects.emplace_back(path, size);
}


void IDiskRemote::Metadata::saveToBuffer(WriteBuffer & buf, bool sync)
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
void IDiskRemote::Metadata::save(bool sync)
{
    auto buf = metadata_disk->writeFile(metadata_file_path, 1024);
    saveToBuffer(*buf, sync);
}

std::string IDiskRemote::Metadata::serializeToString()
{
    WriteBufferFromOwnString write_buf;
    saveToBuffer(write_buf, false);
    return write_buf.str();
}

IDiskRemote::Metadata IDiskRemote::readMetadataUnlocked(const String & path, std::shared_lock<std::shared_mutex> &) const
{
    return Metadata::readMetadata(remote_fs_root_path, metadata_disk, path);
}


IDiskRemote::Metadata IDiskRemote::readMetadata(const String & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path, lock);
}

IDiskRemote::Metadata IDiskRemote::readUpdateAndStoreMetadata(const String & path, bool sync, IDiskRemote::MetadataUpdater updater)
{
    std::unique_lock lock(metadata_mutex);
    return Metadata::readUpdateAndStoreMetadata(remote_fs_root_path, metadata_disk, path, sync, updater);
}


IDiskRemote::Metadata IDiskRemote::readOrCreateUpdateAndStoreMetadata(const String & path, WriteMode mode, bool sync, IDiskRemote::MetadataUpdater updater)
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

IDiskRemote::Metadata IDiskRemote::createAndStoreMetadata(const String & path, bool sync)
{
    return Metadata::createAndStoreMetadata(remote_fs_root_path, metadata_disk, path, sync);
}

IDiskRemote::Metadata IDiskRemote::createUpdateAndStoreMetadata(const String & path, bool sync, IDiskRemote::MetadataUpdater updater)
{
    return Metadata::createUpdateAndStoreMetadata(remote_fs_root_path, metadata_disk, path, sync, updater);
}


std::unordered_map<String, String> IDiskRemote::getSerializedMetadata(const std::vector<std::string> & file_paths) const
{
    std::unordered_map<String, String> metadatas;

    std::shared_lock lock(metadata_mutex);

    for (const auto & path : file_paths)
    {
        IDiskRemote::Metadata metadata = readMetadataUnlocked(path, lock);
        metadata.ref_count = 0;
        metadatas[path] = metadata.serializeToString();
    }

    return metadatas;
}

void IDiskRemote::removeMetadata(const String & path, std::vector<std::string> & paths_to_remove)
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
                    paths_to_remove.push_back(remote_fs_root_path + remote_fs_object_path);

                    if (cache)
                    {
                        auto key = cache->hash(remote_fs_object_path);
                        cache->remove(key);
                    }
                }

                return false;
            }
            else /// In other case decrement number of references, save metadata and delete hardlink.
            {
                --metadata.ref_count;
            }

            return true;
        };

        readUpdateAndStoreMetadata(path, false, metadata_updater);
        metadata_disk->removeFile(path);
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


void IDiskRemote::removeMetadataRecursive(const String & path, std::vector<String> & paths_to_remove)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    if (metadata_disk->isFile(path))
    {
        removeMetadata(path, paths_to_remove);
    }
    else
    {
        for (auto it = iterateDirectory(path); it->isValid(); it->next())
            removeMetadataRecursive(it->path(), paths_to_remove);

        metadata_disk->removeDirectory(path);
    }
}

std::vector<String> IDiskRemote::getRemotePaths(const String & local_path) const
{
    auto metadata = readMetadata(local_path);

    std::vector<String> remote_paths;
    for (const auto & [remote_path, _] : metadata.remote_fs_objects)
        remote_paths.push_back(fs::path(metadata.remote_fs_root_path) / remote_path);

    return remote_paths;
}

void IDiskRemote::getRemotePathsRecursive(const String & local_path, std::vector<LocalPathWithRemotePaths> & paths_map)
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
            IDiskRemote::getRemotePathsRecursive(fs::path(local_path) / it->name(), paths_map);
    }
}

DiskPtr DiskRemoteReservation::getDisk(size_t i) const
{
    if (i != 0)
        throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
    return disk;
}

void DiskRemoteReservation::update(UInt64 new_size)
{
    std::lock_guard lock(disk->reservation_mutex);
    disk->reserved_bytes -= size;
    size = new_size;
    disk->reserved_bytes += size;
}


DiskRemoteReservation::~DiskRemoteReservation()
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


IDiskRemote::IDiskRemote(
    const String & name_,
    const String & remote_fs_root_path_,
    DiskPtr metadata_disk_,
    FileCachePtr cache_,
    const String & log_name_,
    size_t thread_pool_size)
    : IDisk(std::make_unique<AsyncExecutor>(log_name_, thread_pool_size))
    , log(&Poco::Logger::get(log_name_))
    , name(name_)
    , remote_fs_root_path(remote_fs_root_path_)
    , metadata_disk(metadata_disk_)
    , cache(cache_)
{
}


String IDiskRemote::getCacheBasePath() const
{
    return cache ? cache->getBasePath() : "";
}


bool IDiskRemote::exists(const String & path) const
{
    return metadata_disk->exists(path);
}


bool IDiskRemote::isFile(const String & path) const
{
    return metadata_disk->isFile(path);
}


void IDiskRemote::createFile(const String & path)
{
    createAndStoreMetadata(path, false);
}


size_t IDiskRemote::getFileSize(const String & path) const
{
    return readMetadata(path).total_size;
}


void IDiskRemote::moveFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);

    metadata_disk->moveFile(from_path, to_path);
}


void IDiskRemote::replaceFile(const String & from_path, const String & to_path)
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

void IDiskRemote::removeSharedFile(const String & path, bool delete_metadata_only)
{
    std::vector<String> paths_to_remove;
    removeMetadata(path, paths_to_remove);

    if (!delete_metadata_only)
        removeFromRemoteFS(paths_to_remove);
}

void IDiskRemote::removeSharedFileIfExists(const String & path, bool delete_metadata_only)
{
    std::vector<String> paths_to_remove;
    if (metadata_disk->exists(path))
    {
        removeMetadata(path, paths_to_remove);
        if (!delete_metadata_only)
            removeFromRemoteFS(paths_to_remove);
    }
}

void IDiskRemote::removeSharedFiles(const RemoveBatchRequest & files, bool delete_metadata_only)
{
    std::vector<String> paths_to_remove;
    for (const auto & file : files)
    {
        bool skip = file.if_exists && !metadata_disk->exists(file.path);
        if (!skip)
            removeMetadata(file.path, paths_to_remove);
    }

    if (!delete_metadata_only)
        removeFromRemoteFS(paths_to_remove);
}

void IDiskRemote::removeSharedRecursive(const String & path, bool delete_metadata_only)
{
    std::vector<String> paths_to_remove;
    removeMetadataRecursive(path, paths_to_remove);

    if (!delete_metadata_only)
        removeFromRemoteFS(paths_to_remove);
}


void IDiskRemote::setReadOnly(const String & path)
{
    /// We should store read only flag inside metadata file (instead of using FS flag),
    /// because we modify metadata file when create hard-links from it.
    readUpdateAndStoreMetadata(path, false, [] (Metadata & metadata) { metadata.read_only = true; return true; });
}


bool IDiskRemote::isDirectory(const String & path) const
{
    return metadata_disk->isDirectory(path);
}


void IDiskRemote::createDirectory(const String & path)
{
    metadata_disk->createDirectory(path);
}


void IDiskRemote::createDirectories(const String & path)
{
    metadata_disk->createDirectories(path);
}


void IDiskRemote::clearDirectory(const String & path)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        if (isFile(it->path()))
            removeFile(it->path());
}


void IDiskRemote::removeDirectory(const String & path)
{
    metadata_disk->removeDirectory(path);
}


DiskDirectoryIteratorPtr IDiskRemote::iterateDirectory(const String & path)
{
    return metadata_disk->iterateDirectory(path);
}


void IDiskRemote::listFiles(const String & path, std::vector<String> & file_names)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        file_names.push_back(it->name());
}


void IDiskRemote::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    metadata_disk->setLastModified(path, timestamp);
}


Poco::Timestamp IDiskRemote::getLastModified(const String & path)
{
    return metadata_disk->getLastModified(path);
}


void IDiskRemote::createHardLink(const String & src_path, const String & dst_path)
{
    readUpdateAndStoreMetadata(src_path, false, [] (Metadata & metadata) { metadata.ref_count++; return true; });

    /// Create FS hardlink to metadata file.
    metadata_disk->createHardLink(src_path, dst_path);
}


ReservationPtr IDiskRemote::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};

    return std::make_unique<DiskRemoteReservation>(std::static_pointer_cast<IDiskRemote>(shared_from_this()), bytes);
}


bool IDiskRemote::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(reservation_mutex);
    if (bytes == 0)
    {
        LOG_TRACE(log, "Reserving 0 bytes on remote_fs disk {}", backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_TRACE(log, "Reserving {} on disk {}, having unreserved {}.",
            ReadableSize(bytes), backQuote(name), ReadableSize(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }
    return false;
}

String IDiskRemote::getUniqueId(const String & path) const
{
    LOG_TRACE(log, "Remote path: {}, Path: {}", remote_fs_root_path, path);
    auto metadata = readMetadata(path);
    String id;
    if (!metadata.remote_fs_objects.empty())
        id = metadata.remote_fs_root_path + metadata.remote_fs_objects[0].relative_path;
    return id;
}


AsynchronousReaderPtr IDiskRemote::getThreadPoolReader()
{
    constexpr size_t pool_size = 50;
    constexpr size_t queue_size = 1000000;
    static AsynchronousReaderPtr reader = std::make_shared<ThreadPoolRemoteFSReader>(pool_size, queue_size);
    return reader;
}

UInt32 IDiskRemote::getRefCount(const String & path) const
{
    return readMetadata(path).ref_count;
}

ThreadPool & IDiskRemote::getThreadPoolWriter()
{
    constexpr size_t pool_size = 100;
    constexpr size_t queue_size = 1000000;
    static ThreadPool writer(pool_size, pool_size, queue_size);
    return writer;
}

}
