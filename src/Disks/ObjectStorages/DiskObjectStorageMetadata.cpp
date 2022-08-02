#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Common/hex.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int LOGICAL_ERROR;
}

void DiskObjectStorageMetadata::deserialize(ReadBuffer & buf)
{
    UInt32 version;
    readIntText(version, buf);

    if (version < VERSION_ABSOLUTE_PATHS || version > VERSION_READ_ONLY_FLAG)
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT,
            "Unknown metadata file version. Path: {}. Version: {}. Maximum expected version: {}",
            common_metadata_path + metadata_file_path, toString(version), toString(VERSION_READ_ONLY_FLAG));

    assertChar('\n', buf);

    UInt32 storage_objects_count;
    readIntText(storage_objects_count, buf);
    assertChar('\t', buf);
    readIntText(total_size, buf);
    assertChar('\n', buf);
    storage_objects.resize(storage_objects_count);

    for (size_t i = 0; i < storage_objects_count; ++i)
    {
        String object_relative_path;
        size_t object_size;
        readIntText(object_size, buf);
        assertChar('\t', buf);
        readEscapedString(object_relative_path, buf);
        if (version == VERSION_ABSOLUTE_PATHS)
        {
            if (!object_relative_path.starts_with(object_storage_root_path))
                throw Exception(ErrorCodes::UNKNOWN_FORMAT,
                    "Path in metadata does not correspond to root path. Path: {}, root path: {}, disk path: {}",
                    object_relative_path, object_storage_root_path, common_metadata_path);

            object_relative_path = object_relative_path.substr(object_storage_root_path.size());
        }
        assertChar('\n', buf);

        storage_objects[i].relative_path = object_relative_path;
        storage_objects[i].bytes_size = object_size;
    }

    readIntText(ref_count, buf);
    assertChar('\n', buf);

    if (version >= VERSION_READ_ONLY_FLAG)
    {
        readBoolText(read_only, buf);
        assertChar('\n', buf);
    }
}

void DiskObjectStorageMetadata::deserializeFromString(const std::string & data)
{
    ReadBufferFromString buf(data);
    deserialize(buf);
}

void DiskObjectStorageMetadata::serialize(WriteBuffer & buf, bool sync) const
{
    writeIntText(VERSION_READ_ONLY_FLAG, buf);
    writeChar('\n', buf);

    writeIntText(storage_objects.size(), buf);
    writeChar('\t', buf);
    writeIntText(total_size, buf);
    writeChar('\n', buf);

    for (const auto & [object_relative_path, object_size] : storage_objects)
    {
        writeIntText(object_size, buf);
        writeChar('\t', buf);
        writeEscapedString(object_relative_path, buf);
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

std::string DiskObjectStorageMetadata::serializeToString() const
{
    WriteBufferFromOwnString result;
    serialize(result, false);
    return result.str();
}

/// Load metadata by path or create empty if `create` flag is set.
DiskObjectStorageMetadata::DiskObjectStorageMetadata(
        const std::string & common_metadata_path_,
        const String & object_storage_root_path_,
        const String & metadata_file_path_)
    : common_metadata_path(common_metadata_path_)
    , object_storage_root_path(object_storage_root_path_)
    , metadata_file_path(metadata_file_path_)
{
}

void DiskObjectStorageMetadata::addObject(const String & path, size_t size)
{
    if (!object_storage_root_path.empty() && path.starts_with(object_storage_root_path))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected relative path");

    total_size += size;
    storage_objects.emplace_back(path, size);
}

std::string DiskObjectStorageMetadata::getIndexPath() const
{
    if (storage_objects.empty())
        return "";

    UInt128 key = sipHash128(storage_objects[0].relative_path.data(), storage_objects[0].relative_path.size());

    auto key_str = getHexUIntLowercase(key);

    // There are a lot of files in metaindex (one for every object),
    // so here is two-level folders to avoid multiple files in one folder
    return fs::path(common_metadata_path) / "metaindex" / key_str.substr(0, 3) / key_str.substr(0, 6) / key_str;
}

}
