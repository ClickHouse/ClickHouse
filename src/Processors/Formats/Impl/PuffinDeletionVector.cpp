#include <Processors/Formats/Impl/PuffinDeletionVector.h>
#include <Processors/Formats/Impl/PuffinWriter.h>

#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <zlib.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void validateDeletionVectorPosition(UInt64 position)
{
    if (position > static_cast<UInt64>(DELETION_VECTOR_MAX_POSITION))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deleted row position {} exceeds the maximum deletion vector position {}",
            position,
            DELETION_VECTOR_MAX_POSITION);
}

String serializeDeletionVectorBlob(roaring::Roaring64Map & positions)
{
    positions.runOptimize();
    const size_t vector_size = positions.getSizeInBytes(true);
    const size_t combined_length = sizeof(DELETION_VECTOR_MAGIC) + vector_size;
    if (combined_length > static_cast<size_t>(std::numeric_limits<Int32>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector of {} bytes does not fit into a Puffin blob", vector_size);

    String vector_bytes(vector_size, '\0');
    positions.write(vector_bytes.data(), true);

    WriteBufferFromOwnString buf;
    writeBinaryBigEndian(static_cast<UInt32>(combined_length), buf);
    buf.write(reinterpret_cast<const char *>(DELETION_VECTOR_MAGIC), sizeof(DELETION_VECTOR_MAGIC));
    buf.write(vector_bytes.data(), vector_bytes.size());

    const std::string_view written = buf.stringView();
    const auto crc = static_cast<UInt32>(crc32_z(
        0L, reinterpret_cast<const unsigned char *>(written.data() + sizeof(UInt32)), written.size() - sizeof(UInt32)));
    writeBinaryBigEndian(crc, buf);
    return buf.str();
}

PuffinBlob makeDeletionVectorBlobMetadata(const String & referenced_data_file, UInt64 cardinality)
{
    PuffinBlob blob;
    blob.type = PUFFIN_DELETION_VECTOR_BLOB_TYPE;
    blob.fields = {ICEBERG_ROW_POSITION_FIELD_ID};
    blob.snapshot_id = -1;
    blob.sequence_number = -1;
    blob.properties.emplace("referenced-data-file", referenced_data_file);
    blob.properties.emplace("cardinality", toString(cardinality));
    return blob;
}

PuffinBlob writeDeletionVectorBlob(PuffinWriter & writer, const String & referenced_data_file, roaring::Roaring64Map & positions)
{
    const String bytes = serializeDeletionVectorBlob(positions);
    return writer.addBlob(makeDeletionVectorBlobMetadata(referenced_data_file, positions.cardinality()), bytes);
}

}
