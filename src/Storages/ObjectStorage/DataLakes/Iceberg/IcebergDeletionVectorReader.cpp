#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDeletionVectorReader.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/PuffinBlockInputFormat.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Common/Exception.h>

#include <roaring/roaring64map.hh>

#include <limits>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
}

namespace DB::Iceberg
{

std::unique_ptr<roaring::Roaring64Map> readIcebergDeletionVector(
    const String & file_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const ObjectStoragePtr & object_storage,
    ContextPtr context,
    LoggerPtr log)
{
    if (content_offset < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg deletion vector content offset cannot be negative: {}", content_offset);
    if (content_size_in_bytes < 12)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Iceberg deletion vector blob is too small: {}", content_size_in_bytes);
    if (content_size_in_bytes > std::numeric_limits<Int64>::max() - content_offset)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg deletion vector content range overflows: offset {}, size {}", content_offset, content_size_in_bytes);

    RelativePathWithMetadata object_info(file_path);
    /// The blob is a small range inside a Puffin file shared by many data files. Read only that
    /// range, and skip the whole-file prefetch: it starts at byte 0 and the seek below would
    /// block on it before discarding it.
    auto read_settings = context->getReadSettings();
    read_settings.remote_fs_settings.prefetch = false;
    auto read_buffer = createReadBuffer(object_info, object_storage, context, log, read_settings);
    read_buffer->seek(content_offset, SEEK_SET);
    read_buffer->setReadUntilPosition(static_cast<size_t>(content_offset + content_size_in_bytes));

    String blob(static_cast<size_t>(content_size_in_bytes), '\0');
    read_buffer->readStrict(blob.data(), blob.size());

    /// The envelope and bitmap decoding are shared with the Puffin input format.
    auto bitmap = std::make_unique<roaring::Roaring64Map>();
    forEachDeletionVectorPosition(blob, [&](UInt64 position) { bitmap->add(position); });
    return bitmap;
}

}
