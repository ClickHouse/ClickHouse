#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int LOGICAL_ERROR;
}

DiskObjectStorageMetadata::DiskObjectStorageMetadata(std::string compatible_key_prefix_, std::string metadata_file_path_)
    : compatible_key_prefix(std::move(compatible_key_prefix_))
    , metadata_file_path(std::move(metadata_file_path_))
{
}

void DiskObjectStorageMetadata::deserialize(ReadBuffer & buf)
{
    readIntText(version, buf);
    assertChar('\n', buf);

    if (version < VERSION_ABSOLUTE_PATHS || version > VERSION_FULL_OBJECT_KEY)
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT,
            "Unknown metadata file version. Path: {}. Version: {}. Maximum expected version: {}",
            metadata_file_path, toString(version), toString(VERSION_FULL_OBJECT_KEY));

    UInt32 keys_count;
    readIntText(keys_count, buf);
    assertChar('\t', buf);
    objects.reserve(keys_count);

    int64_t serialized_total_size = 0;
    readIntText(serialized_total_size, buf);
    assertChar('\n', buf);

    int64_t serialized_objects_size = 0;
    for (UInt32 i = 0; i < keys_count; ++i)
    {
        int64_t object_size = 0;
        readIntText(object_size, buf);
        assertChar('\t', buf);

        std::string remote_path;
        readEscapedString(remote_path, buf);
        assertChar('\n', buf);

        if (version == VERSION_ABSOLUTE_PATHS)
        {
            if (!remote_path.starts_with(compatible_key_prefix))
                throw Exception(ErrorCodes::UNKNOWN_FORMAT,
                    "Remote path in metadata does not correspond to root path. Path: {}, Root path: {}, Metadata path: {}",
                    remote_path, compatible_key_prefix, metadata_file_path);

            remote_path = ObjectStorageKey::createAsRelative(compatible_key_prefix, remote_path.substr(compatible_key_prefix.size())).serialize();
        }
        else if (version < VERSION_FULL_OBJECT_KEY)
        {
            remote_path = ObjectStorageKey::createAsRelative(compatible_key_prefix, remote_path).serialize();
        }

        const StoredObject & object = objects.emplace_back(remote_path, metadata_file_path, object_size);
        serialized_objects_size += object.bytes_size;
    }

    if (serialized_total_size != serialized_objects_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Serialized total bytes size of metadata file '{}' is not equal to real sum of blob sizes ({} != {})",
            metadata_file_path, serialized_total_size, serialized_objects_size);

    readIntText(ref_count, buf);
    assertChar('\n', buf);

    if (version >= VERSION_READ_ONLY_FLAG)
    {
        readBoolText(read_only, buf);
        assertChar('\n', buf);
    }

    if (version >= VERSION_INLINE_DATA)
    {
        readEscapedString(inline_data, buf);
        assertChar('\n', buf);
    }
}

void DiskObjectStorageMetadata::deserializeFromString(const std::string & data)
try
{
    ReadBufferFromString buf(data);
    deserialize(buf);
}
catch (Exception & e)
{
    e.addMessage("while parsing: '{}'", data);
    throw;
}

bool DiskObjectStorageMetadata::tryDeserializeFromString(const std::string & data) noexcept
try
{
    ReadBufferFromString buf(data);
    deserialize(buf);
    return true;
}
catch (...)
{
    return false;
}

void DiskObjectStorageMetadata::serialize(WriteBuffer & buf) const
{
    constexpr UInt32 write_version = VERSION_FULL_OBJECT_KEY;

    writeIntText(write_version, buf);
    writeChar('\n', buf);

    writeIntText(objects.size(), buf);
    writeChar('\t', buf);
    writeIntText(getTotalSize(objects), buf);
    writeChar('\n', buf);

    for (const auto & object : objects)
    {
        writeIntText(object.bytes_size, buf);
        writeChar('\t', buf);

        writeEscapedString(object.remote_path, buf);
        writeChar('\n', buf);
    }

    writeIntText(ref_count, buf);
    writeChar('\n', buf);

    writeBoolText(read_only, buf);
    writeChar('\n', buf);

    if (write_version >= VERSION_INLINE_DATA)
    {
        writeEscapedString(inline_data, buf);
        writeChar('\n', buf);
    }
}

String DiskObjectStorageMetadata::serializeToString() const
{
    WriteBufferFromOwnString result;
    serialize(result);
    return result.str();
}

}
