#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDeletionVectorReader.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/PuffinBlockInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Common/Exception.h>

#include <roaring/roaring64map.hh>

#include <limits>
#include <string_view>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
extern const int INCORRECT_DATA;
}

namespace DB::Iceberg
{

namespace
{
constexpr std::string_view DELETION_VECTOR_BLOB_TYPE = "deletion-vector-v1";
}

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

std::shared_ptr<const PuffinBlobDescriptors> readPuffinBlobDescriptors(
    const String & file_path, const ObjectStoragePtr & object_storage, ContextPtr context, LoggerPtr log)
{
    RelativePathWithMetadata object_info(file_path);
    auto read_settings = context->getReadSettings();
    read_settings.remote_fs_settings.random_access = true;
    auto read_buffer = createReadBuffer(object_info, object_storage, context, log, read_settings);
    /// The Puffin spec does not bound the footer, and Iceberg writers put all vectors of one write into one file.
    auto footer = readPuffinFooter(
        *read_buffer, /*seekable_read=*/ true, /*other_blob_types_allowed=*/ true, /*limit_footer_size=*/ false);

    auto descriptors = std::make_shared<PuffinBlobDescriptors>();
    for (auto & blob : footer.blobs)
    {
        PuffinBlobDescriptors::Blob descriptor;
        descriptor.type = std::move(blob.type);
        if (descriptor.type == DELETION_VECTOR_BLOB_TYPE)
            descriptor.referenced_data_file = blob.properties.at("referenced-data-file");
        if (!descriptors->blobs.emplace(std::pair{blob.offset, blob.length}, std::move(descriptor)).second)
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Puffin file '{}' describes two blobs at offset {} and size {}",
                file_path,
                blob.offset,
                blob.length);
    }
    return descriptors;
}

void checkIcebergDeletionVectorBlob(
    const PuffinBlobDescriptors & descriptors,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const IcebergPathFromMetadata & data_file,
    const IcebergPathFromMetadata & puffin_file,
    const String & table_location)
{
    auto it = descriptors.blobs.find({content_offset, content_size_in_bytes});
    if (it == descriptors.blobs.end() || it->second.type != DELETION_VECTOR_BLOB_TYPE)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Iceberg deletion vector of data file '{}' points at offset {} and size {} of Puffin file '{}', "
            "which is not a deletion vector blob in its footer",
            data_file.serialize(),
            content_offset,
            content_size_in_bytes,
            puffin_file.serialize());

    std::string_view referenced_data_file = it->second.referenced_data_file;
    std::string_view data_path = data_file.serialize();
    if (referenced_data_file == data_path)
        return;

    std::string_view location = table_location;
    if (location.ends_with('/'))
        location.remove_suffix(1);
    auto is_below_location = [&](std::string_view path)
    { return path.size() > location.size() + 1 && path.starts_with(location) && path[location.size()] == '/'; };

    /// Relocating a table changes the paths up to its location, and a footer that was not rewritten keeps the old prefix,
    /// so the path below the location must still match. A footer path below the current location must match exactly.
    if (is_below_location(data_path) && !is_below_location(referenced_data_file))
    {
        std::string_view relative_path = data_path.substr(location.size() + 1);
        if (referenced_data_file == relative_path
            || (referenced_data_file.ends_with(relative_path)
                && referenced_data_file[referenced_data_file.size() - relative_path.size() - 1] == '/'))
            return;
    }

    throw Exception(
        ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
        "Iceberg deletion vector of data file '{}' points at the blob at offset {} of Puffin file '{}', "
        "whose `referenced-data-file` is '{}'",
        data_file.serialize(),
        content_offset,
        puffin_file.serialize(),
        referenced_data_file);
}

}
