#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int PATH_ACCESS_DENIED;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

DiskObjectStorageMetadata DiskObjectStorageMetadata::readMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_)
{
    DiskObjectStorageMetadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.load();
    return result;
}


DiskObjectStorageMetadata DiskObjectStorageMetadata::createAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync)
{
    DiskObjectStorageMetadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.save(sync);
    return result;
}

DiskObjectStorageMetadata DiskObjectStorageMetadata::readUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, DiskObjectStorageMetadataUpdater updater)
{
    DiskObjectStorageMetadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.load();
    if (updater(result))
        result.save(sync);
    return result;
}

DiskObjectStorageMetadata DiskObjectStorageMetadata::createUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, DiskObjectStorageMetadataUpdater updater)
{
    DiskObjectStorageMetadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    updater(result);
    result.save(sync);
    return result;
}

DiskObjectStorageMetadata DiskObjectStorageMetadata::readUpdateStoreMetadataAndRemove(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, DiskObjectStorageMetadataUpdater updater)
{
    DiskObjectStorageMetadata result(remote_fs_root_path_, metadata_disk_, metadata_file_path_);
    result.load();
    if (updater(result))
        result.save(sync);
    metadata_disk_->removeFile(metadata_file_path_);

    return result;

}

DiskObjectStorageMetadata DiskObjectStorageMetadata::createAndStoreMetadataIfNotExists(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, bool overwrite)
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

void DiskObjectStorageMetadata::load()
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
DiskObjectStorageMetadata::DiskObjectStorageMetadata(
        const String & remote_fs_root_path_,
        DiskPtr metadata_disk_,
        const String & metadata_file_path_)
    : remote_fs_root_path(remote_fs_root_path_)
    , metadata_file_path(metadata_file_path_)
    , metadata_disk(metadata_disk_)
    , total_size(0), ref_count(0)
{
}

void DiskObjectStorageMetadata::addObject(const String & path, size_t size)
{
    total_size += size;
    remote_fs_objects.emplace_back(path, size);
}


void DiskObjectStorageMetadata::saveToBuffer(WriteBuffer & buf, bool sync)
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
void DiskObjectStorageMetadata::save(bool sync)
{
    auto buf = metadata_disk->writeFile(metadata_file_path, 1024);
    saveToBuffer(*buf, sync);
}

std::string DiskObjectStorageMetadata::serializeToString()
{
    WriteBufferFromOwnString write_buf;
    saveToBuffer(write_buf, false);
    return write_buf.str();
}


}
