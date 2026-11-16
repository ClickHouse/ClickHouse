#include <Processors/Formats/Impl/PuffinDeletionVector.h>
#include <Processors/Formats/Impl/PuffinWriter.h>

#include <Columns/ColumnArray.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>

#include <zlib.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr Int32 ICEBERG_ROW_POSITION_FIELD_ID = std::numeric_limits<Int32>::max() - 2;

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

void validateDeletionVectorPosition(UInt64 position)
{
    if (position > static_cast<UInt64>(DELETION_VECTOR_MAX_POSITION))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deleted row position {} exceeds the maximum deletion vector position {}",
            position,
            DELETION_VECTOR_MAX_POSITION);
}

}

PuffinDeletionVectorBuilder::PuffinDeletionVectorBuilder(const Block & header)
{
    if (header.columns() != 2)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin output format requires two columns: the referenced data file (a string) and deleted row positions "
            "(an integer or an array of integers), got {}",
            header.dumpNames());

    const auto & file_type = header.getByPosition(0).type;
    if (!isString(removeLowCardinality(file_type)))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin output format requires the first column with the referenced data file to be a string, got {}",
            file_type->getName());

    DataTypePtr positions_type = header.getByPosition(1).type;
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(positions_type.get()))
    {
        positions_are_arrays = true;
        positions_type = array_type->getNestedType();
    }

    WhichDataType which(positions_type);
    if (!which.isNativeUInt() && !which.isNativeInt())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Puffin output format requires the second column with deleted row positions to be an integer or an array of integers, got {}",
            header.getByPosition(1).type->getName());
    positions_are_unsigned = which.isNativeUInt();
}

void PuffinDeletionVectorBuilder::addPositions(roaring::Roaring64Map & bitmap, const IColumn & positions, size_t begin, size_t end) const
{
    for (size_t i = begin; i < end; ++i)
    {
        UInt64 position = 0;
        if (positions_are_unsigned)
        {
            position = positions.getUInt(i);
        }
        else
        {
            const Int64 value = positions.getInt(i);
            if (value < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deleted row position {} is negative", value);
            position = static_cast<UInt64>(value);
        }
        validateDeletionVectorPosition(position);
        bitmap.add(position);
    }
}

void PuffinDeletionVectorBuilder::consume(const Chunk & chunk)
{
    const auto & columns = chunk.getColumns();
    const auto files = columns[0]->convertToFullColumnIfLowCardinality();
    const IColumn & positions = *columns[1];
    const auto * positions_array = positions_are_arrays ? &assert_cast<const ColumnArray &>(positions) : nullptr;
    const auto * positions_offsets = positions_array ? &positions_array->getOffsets() : nullptr;

    roaring::Roaring64Map * bitmap = nullptr;
    std::string_view current_file;
    for (size_t row = 0; row < chunk.getNumRows(); ++row)
    {
        const std::string_view file = files->getDataAt(row);
        if (!bitmap || file != current_file)
        {
            if (file.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Referenced data file of a deletion vector must not be empty");
            bitmap = &deletion_vectors[String(file)];
            current_file = file;
        }

        if (positions_array)
            addPositions(*bitmap, positions_array->getData(), (*positions_offsets)[row - 1], (*positions_offsets)[row]);
        else
            addPositions(*bitmap, positions, row, row + 1);
    }
}

void PuffinDeletionVectorBuilder::write(PuffinWriter & writer)
{
    for (auto & [referenced_data_file, positions] : deletion_vectors)
    {
        const String bytes = serializeDeletionVectorBlob(positions);
        writer.addBlob(makeDeletionVectorBlobMetadata(referenced_data_file, positions.cardinality()), bytes);
    }
}

void PuffinDeletionVectorBuilder::reset()
{
    deletion_vectors.clear();
}

}
