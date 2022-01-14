#include <Disks/IDiskRemote.h>

#include "Disks/DiskFactory.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <common/logger_useful.h>
#include <Common/checkStackSize.h>
#include <boost/algorithm/string.hpp>
#include <Common/filesystemHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DISK_INDEX;
    extern const int UNKNOWN_FORMAT;
    extern const int FILE_ALREADY_EXISTS;
    extern const int PATH_ACCESS_DENIED;;
    extern const int CANNOT_DELETE_DIRECTORY;
}


/// Load metadata by path or create empty if `create` flag is set.
IDiskRemote::Metadata::Metadata(
        const String & remote_fs_root_path_,
        const String & disk_path_,
        const String & metadata_file_path_,
        bool create)
    : remote_fs_root_path(remote_fs_root_path_)
    , disk_path(disk_path_)
    , metadata_file_path(metadata_file_path_)
    , total_size(0), remote_fs_objects(0), ref_count(0)
{
    if (create)
        return;

    try
    {
        ReadBufferFromFile buf(disk_path + metadata_file_path, 1024); /* reasonable buffer size for small file */

        UInt32 version;
        readIntText(version, buf);

        if (version < VERSION_ABSOLUTE_PATHS || version > VERSION_READ_ONLY_FLAG)
            throw Exception(
                ErrorCodes::UNKNOWN_FORMAT,
                "Unknown metadata file version. Path: {}. Version: {}. Maximum expected version: {}",
                disk_path + metadata_file_path, toString(version), toString(VERSION_READ_ONLY_FLAG));

        assertChar('\n', buf);

        UInt32 remote_fs_objects_count;
        readIntText(remote_fs_objects_count, buf);
        assertChar('\t', buf);
        readIntText(total_size, buf);
        assertChar('\n', buf);
        remote_fs_objects.resize(remote_fs_objects_count);

        for (size_t i = 0; i < remote_fs_objects_count; ++i)
        {
            String remote_fs_object_path;
            size_t remote_fs_object_size;
            readIntText(remote_fs_object_size, buf);
            assertChar('\t', buf);
            readEscapedString(remote_fs_object_path, buf);
            if (version == VERSION_ABSOLUTE_PATHS)
            {
                if (!boost::algorithm::starts_with(remote_fs_object_path, remote_fs_root_path))
                    throw Exception(
                        ErrorCodes::UNKNOWN_FORMAT,
                        "Path in metadata does not correspond S3 root path. Path: {}, root path: {}, disk path: {}",
                        remote_fs_object_path, remote_fs_root_path, disk_path_);

                remote_fs_object_path = remote_fs_object_path.substr(remote_fs_root_path.size());
            }
            assertChar('\n', buf);
            remote_fs_objects[i] = {remote_fs_object_path, remote_fs_object_size};
        }

        readIntText(ref_count, buf);
        assertChar('\n', buf);

        if (version >= VERSION_READ_ONLY_FLAG)
        {
            readBoolText(read_only, buf);
            assertChar('\n', buf);
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_FORMAT)
            throw;

        throw Exception("Failed to read metadata file", e, ErrorCodes::UNKNOWN_FORMAT);
    }
}

void IDiskRemote::Metadata::addObject(const String & path, size_t size)
{
    total_size += size;
    remote_fs_objects.emplace_back(path, size);
}

/// Fsync metadata file if 'sync' flag is set.
void IDiskRemote::Metadata::save(bool sync)
{
    WriteBufferFromFile buf(disk_path + metadata_file_path, 1024);

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

IDiskRemote::Metadata IDiskRemote::readOrCreateMetaForWriting(const String & path, WriteMode mode)
{
    bool exist = exists(path);
    if (exist)
    {
        auto metadata = readMeta(path);
        if (metadata.read_only)
            throw Exception("File is read-only: " + path, ErrorCodes::PATH_ACCESS_DENIED);

        if (mode == WriteMode::Rewrite)
            removeFile(path); /// Remove for re-write.
        else
            return metadata;
    }

    auto metadata = createMeta(path);
    /// Save empty metadata to disk to have ability to get file size while buffer is not finalized.
    metadata.save();

    return metadata;
}


IDiskRemote::Metadata IDiskRemote::readMeta(const String & path) const
{
    return Metadata(remote_fs_root_path, metadata_path, path);
}


IDiskRemote::Metadata IDiskRemote::createMeta(const String & path) const
{
    return Metadata(remote_fs_root_path, metadata_path, path, true);
}


void IDiskRemote::removeMeta(const String & path, RemoteFSPathKeeperPtr fs_paths_keeper)
{
    LOG_DEBUG(log, "Remove file by path: {}", backQuote(metadata_path + path));

    fs::path file(metadata_path + path);

    if (!fs::is_regular_file(file))
        throw Exception(ErrorCodes::CANNOT_DELETE_DIRECTORY, "Path '{}' is a directory", path);

    try
    {
        auto metadata = readMeta(path);

        /// If there is no references - delete content from remote FS.
        if (metadata.ref_count == 0)
        {
            fs::remove(file);
            for (const auto & [remote_fs_object_path, _] : metadata.remote_fs_objects)
                fs_paths_keeper->addPath(remote_fs_root_path + remote_fs_object_path);
        }
        else /// In other case decrement number of references, save metadata and delete file.
        {
            --metadata.ref_count;
            metadata.save();
            fs::remove(file);
        }
    }
    catch (const Exception & e)
    {
        /// If it's impossible to read meta - just remove it from FS.
        if (e.code() == ErrorCodes::UNKNOWN_FORMAT)
        {
            LOG_WARNING(log,
                "Metadata file {} can't be read by reason: {}. Removing it forcibly.",
                backQuote(path), e.nested() ? e.nested()->message() : e.message());
            fs::remove(file);
        }
        else
            throw;
    }
}


void IDiskRemote::removeMetaRecursive(const String & path, RemoteFSPathKeeperPtr fs_paths_keeper)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    fs::path file = fs::path(metadata_path) / path;
    if (fs::is_regular_file(file))
    {
        removeMeta(path, fs_paths_keeper);
    }
    else
    {
        for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
            removeMetaRecursive(it->path(), fs_paths_keeper);
        fs::remove(file);
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
    const String & metadata_path_,
    const String & log_name_,
    size_t thread_pool_size)
    : IDisk(std::make_unique<AsyncExecutor>(log_name_, thread_pool_size))
    , log(&Poco::Logger::get(log_name_))
    , name(name_)
    , remote_fs_root_path(remote_fs_root_path_)
    , metadata_path(metadata_path_)
{
}


bool IDiskRemote::exists(const String & path) const
{
    return fs::exists(fs::path(metadata_path) / path);
}


bool IDiskRemote::isFile(const String & path) const
{
    return fs::is_regular_file(fs::path(metadata_path) / path);
}


void IDiskRemote::createFile(const String & path)
{
    /// Create empty metadata file.
    auto metadata = createMeta(path);
    metadata.save();
}


size_t IDiskRemote::getFileSize(const String & path) const
{
    auto metadata = readMeta(path);
    return metadata.total_size;
}


void IDiskRemote::moveFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);

    fs::rename(fs::path(metadata_path) / from_path, fs::path(metadata_path) / to_path);
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


void IDiskRemote::removeFileIfExists(const String & path)
{
    RemoteFSPathKeeperPtr fs_paths_keeper = createFSPathKeeper();
    if (fs::exists(fs::path(metadata_path) / path))
    {
        removeMeta(path, fs_paths_keeper);
        removeFromRemoteFS(fs_paths_keeper);
    }
}


void IDiskRemote::removeSharedFile(const String & path, bool keep_in_remote_fs)
{
    RemoteFSPathKeeperPtr fs_paths_keeper = createFSPathKeeper();
    removeMeta(path, fs_paths_keeper);
    if (!keep_in_remote_fs)
        removeFromRemoteFS(fs_paths_keeper);
}


void IDiskRemote::removeSharedRecursive(const String & path, bool keep_in_remote_fs)
{
    RemoteFSPathKeeperPtr fs_paths_keeper = createFSPathKeeper();
    removeMetaRecursive(path, fs_paths_keeper);
    if (!keep_in_remote_fs)
        removeFromRemoteFS(fs_paths_keeper);
}


void IDiskRemote::setReadOnly(const String & path)
{
    /// We should store read only flag inside metadata file (instead of using FS flag),
    /// because we modify metadata file when create hard-links from it.
    auto metadata = readMeta(path);
    metadata.read_only = true;
    metadata.save();
}


bool IDiskRemote::isDirectory(const String & path) const
{
    return fs::is_directory(fs::path(metadata_path) / path);
}


void IDiskRemote::createDirectory(const String & path)
{
    fs::create_directory(fs::path(metadata_path) / path);
}


void IDiskRemote::createDirectories(const String & path)
{
    fs::create_directories(fs::path(metadata_path) / path);
}


void IDiskRemote::clearDirectory(const String & path)
{
    for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
        if (isFile(it->path()))
            removeFile(it->path());
}


void IDiskRemote::removeDirectory(const String & path)
{
    fs::remove(fs::path(metadata_path) / path);
}


DiskDirectoryIteratorPtr IDiskRemote::iterateDirectory(const String & path)
{
    fs::path meta_path = fs::path(metadata_path) / path;
    if (fs::exists(meta_path) && fs::is_directory(meta_path))
        return std::make_unique<RemoteDiskDirectoryIterator>(meta_path, path);
    else
        return std::make_unique<RemoteDiskDirectoryIterator>();
}


void IDiskRemote::listFiles(const String & path, std::vector<String> & file_names)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        file_names.push_back(it->name());
}


void IDiskRemote::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    FS::setModificationTime(fs::path(metadata_path) / path, timestamp.epochTime());
}


Poco::Timestamp IDiskRemote::getLastModified(const String & path)
{
    return FS::getModificationTimestamp(fs::path(metadata_path) / path);
}


void IDiskRemote::createHardLink(const String & src_path, const String & dst_path)
{
    /// Increment number of references.
    auto src = readMeta(src_path);
    ++src.ref_count;
    src.save();

    /// Create FS hardlink to metadata file.
    DB::createHardLink(metadata_path + src_path, metadata_path + dst_path);
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
        LOG_DEBUG(log, "Reserving 0 bytes on remote_fs disk {}", backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(log, "Reserving {} on disk {}, having unreserved {}.",
            ReadableSize(bytes), backQuote(name), ReadableSize(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }
    return false;
}

}
