#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/logger_useful.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsBool storage_metadata_write_full_object_key;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int LOGICAL_ERROR;
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
    keys_with_meta.resize(keys_count);

    readIntText(total_size, buf);
    assertChar('\n', buf);

    for (UInt32 i = 0; i < keys_count; ++i)
    {
        UInt64 object_size;
        readIntText(object_size, buf);
        assertChar('\t', buf);

        keys_with_meta[i].metadata.size_bytes = object_size;

        String key_value;
        readEscapedString(key_value, buf);
        assertChar('\n', buf);

        if (version == VERSION_ABSOLUTE_PATHS)
        {
            if (!key_value.starts_with(compatible_key_prefix))
                throw Exception(
                    ErrorCodes::UNKNOWN_FORMAT,
                    "Path in metadata does not correspond to root path. Path: {}, root path: {}, disk path: {}",
                    key_value,
                    compatible_key_prefix,
                    metadata_file_path);

            keys_with_meta[i].key = ObjectStorageKey::createAsRelative(
                compatible_key_prefix, key_value.substr(compatible_key_prefix.size()));
        }
        else if (version < VERSION_FULL_OBJECT_KEY)
        {
            keys_with_meta[i].key = ObjectStorageKey::createAsRelative(compatible_key_prefix, key_value);
        }
        else if (version >= VERSION_FULL_OBJECT_KEY)
        {
            keys_with_meta[i].key = ObjectStorageKey::createAsAbsolute(key_value);
        }
    }

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

void DiskObjectStorageMetadata::deserializeFromString(const String & data)
{
    ReadBufferFromString buf(data);
    deserialize(buf);
}

void DiskObjectStorageMetadata::serialize(WriteBuffer & buf, bool sync) const
{
    /// These are the changes for backward compatibility
    /// No new file should be written as VERSION_FULL_OBJECT_KEY until storage_metadata_write_full_object_key feature is enabled
    /// However, in case of rollback, once file had been written as VERSION_FULL_OBJECT_KEY
    /// it has to be always rewritten as VERSION_FULL_OBJECT_KEY

    bool storage_metadata_write_full_object_key = getWriteFullObjectKeySetting();

    if (version == VERSION_FULL_OBJECT_KEY && !storage_metadata_write_full_object_key)
    {
        LoggerPtr logger = getLogger("DiskObjectStorageMetadata");
        LOG_WARNING(
            logger,
            "Metadata file {} is written with VERSION_FULL_OBJECT_KEY version"
            "However storage_metadata_write_full_object_key is off.",
            metadata_file_path);
    }

    UInt32 write_version = version;
    if (storage_metadata_write_full_object_key)
        write_version = VERSION_FULL_OBJECT_KEY;

    if (!inline_data.empty() && write_version < VERSION_INLINE_DATA)
        write_version = VERSION_INLINE_DATA;

    chassert(write_version >= VERSION_ABSOLUTE_PATHS && write_version <= VERSION_FULL_OBJECT_KEY);
    writeIntText(write_version, buf);

    writeChar('\n', buf);

    writeIntText(keys_with_meta.size(), buf);
    writeChar('\t', buf);
    writeIntText(total_size, buf);
    writeChar('\n', buf);

    for (const auto & [object_key, object_meta] : keys_with_meta)
    {
        writeIntText(object_meta.size_bytes, buf);
        writeChar('\t', buf);

        if (write_version == VERSION_FULL_OBJECT_KEY)
        {
            /// if the metadata file has VERSION_FULL_OBJECT_KEY version
            /// all keys inside are written as absolute paths
            writeEscapedString(object_key.serialize(), buf);
            writeChar('\n', buf);
        }
        else
        {
            /// otherwise keys are written as relative paths
            /// therefore keys have to have suffix and prefix
            writeEscapedString(object_key.getSuffix(), buf);
            writeChar('\n', buf);
        }
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

    buf.finalize();
    if (sync)
        buf.sync();
}

String DiskObjectStorageMetadata::serializeToString() const
{
    WriteBufferFromOwnString result;
    serialize(result, false);
    return result.str();
}

/// Load metadata by path or create empty if `create` flag is set.
DiskObjectStorageMetadata::DiskObjectStorageMetadata(
    String compatible_key_prefix_,
    String metadata_file_path_)
    : compatible_key_prefix(std::move(compatible_key_prefix_))
    , metadata_file_path(std::move(metadata_file_path_))
{
}

void DiskObjectStorageMetadata::addObject(ObjectStorageKey key, size_t size)
{
    if (!key.hasPrefix())
    {
        version = VERSION_FULL_OBJECT_KEY;

        bool storage_metadata_write_full_object_key = getWriteFullObjectKeySetting();
        if (!storage_metadata_write_full_object_key)
        {
            LoggerPtr logger = getLogger("DiskObjectStorageMetadata");
            LOG_WARNING(
                logger,
                "Metadata file {} has at least one key {} without fixed common key prefix."
                "That forces using VERSION_FULL_OBJECT_KEY version for that metadata file."
                "However storage_metadata_write_full_object_key is off.",
                metadata_file_path,
                key.serialize());
        }
    }

    total_size += size;
    keys_with_meta.emplace_back(std::move(key), ObjectMetadata{size, {}, {}, {}});
}

ObjectKeyWithMetadata DiskObjectStorageMetadata::popLastObject()
{
    if (keys_with_meta.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't pop last object from metadata {}. Metadata already empty", metadata_file_path);

    ObjectKeyWithMetadata object = std::move(keys_with_meta.back());
    keys_with_meta.pop_back();
    total_size -= object.metadata.size_bytes;

    return object;
}

bool DiskObjectStorageMetadata::getWriteFullObjectKeySetting()
{
    return Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::storage_metadata_write_full_object_key];
}

}
