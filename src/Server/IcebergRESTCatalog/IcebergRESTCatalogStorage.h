#pragma once

#include <base/types.h>
#include <base/unit.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <IO/CompressionMethod.h>

namespace DB
{

class IObjectStorage;
struct ReadSettings;
struct WriteSettings;

namespace IcebergRESTCatalog
{

/// Metadata files with many snapshots reach tens of megabytes.
constexpr size_t MAX_METADATA_FILE_SIZE = 64_MiB;

/// Codec of a metadata file by its name, such as `v1-<uuid>.gz.metadata.json`. The codec sits before `.metadata.json`, not at the end.
CompressionMethod getMetadataFileCompressionMethod(const String & path);

/// Throws `LIMIT_EXCEEDED` if the decompressed content is larger than `MAX_METADATA_FILE_SIZE`.
String readObjectToString(
    const IObjectStorage & object_storage,
    const String & key,
    const ReadSettings & read_settings,
    CompressionMethod compression_method);

/// Fails if `key` already exists or `content` is larger than `MAX_METADATA_FILE_SIZE`.
void writeNewObject(
    IObjectStorage & object_storage,
    const String & key,
    const String & content,
    WriteSettings write_settings,
    CompressionMethod compression_method,
    int compression_level);

}

}
