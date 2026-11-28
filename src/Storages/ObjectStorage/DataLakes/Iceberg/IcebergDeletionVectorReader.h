#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <map>
#include <memory>
#include <utility>

#include <roaring/roaring64map.hh>

namespace DB
{
class IObjectStorage;
using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;
}

namespace DB::Iceberg
{

class IcebergPathFromMetadata;

std::unique_ptr<roaring::Roaring64Map> readIcebergDeletionVector(
    const String & file_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const ObjectStoragePtr & object_storage,
    ContextPtr context,
    LoggerPtr log);

/// The blobs of the footer of a Puffin file, by (offset, length): type and, for `deletion-vector-v1`, `referenced-data-file`.
struct PuffinBlobDescriptors
{
    struct Blob
    {
        String type;
        String referenced_data_file;
    };
    std::map<std::pair<Int64, Int64>, Blob> blobs;
};

std::shared_ptr<const PuffinBlobDescriptors> readPuffinBlobDescriptors(
    const String & file_path, const ObjectStoragePtr & object_storage, ContextPtr context, LoggerPtr log);

/// Throws ICEBERG_SPECIFICATION_VIOLATION unless the footer has a deletion vector blob at exactly this range that
/// belongs to `data_file`.
void checkIcebergDeletionVectorBlob(
    const PuffinBlobDescriptors & descriptors,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const IcebergPathFromMetadata & data_file,
    const IcebergPathFromMetadata & puffin_file,
    const String & table_location);

}
