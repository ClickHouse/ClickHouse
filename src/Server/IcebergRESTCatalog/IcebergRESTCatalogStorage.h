#pragma once

#include <base/types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <IO/CompressionMethod.h>

namespace DB
{

class IObjectStorage;
struct ReadSettings;
struct WriteSettings;

/// Codec of a metadata file by its name, such as `v1-<uuid>.gz.metadata.json`. The codec sits before `.metadata.json`, not at the end.
CompressionMethod getMetadataFileCompressionMethod(const String & path);

/// Throws `LIMIT_EXCEEDED` if the decompressed content is larger than `max_size`.
String readObjectToString(
    const IObjectStorage & object_storage,
    const String & key,
    const ReadSettings & read_settings,
    size_t max_size,
    CompressionMethod compression_method);

/// Fails if `key` already exists.
void writeNewObject(
    IObjectStorage & object_storage,
    const String & key,
    const String & content,
    WriteSettings write_settings,
    CompressionMethod compression_method,
    int compression_level);

}
