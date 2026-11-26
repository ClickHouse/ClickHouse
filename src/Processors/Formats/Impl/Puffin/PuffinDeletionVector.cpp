#include <Processors/Formats/Impl/Puffin/PuffinDeletionVector.h>
#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>
#include <Processors/Formats/Impl/Puffin/PuffinWriter.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>

#include <roaring/roaring64map.hh>
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

void validateDeletionVectorPosition(UInt64 position)
{
    if (position > static_cast<UInt64>(DELETION_VECTOR_MAX_POSITION))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deleted row position {} exceeds the maximum deletion vector position {}",
            position,
            DELETION_VECTOR_MAX_POSITION);
}

template <typename T>
void addPositions(roaring::Roaring64Map & bitmap, const IColumn & positions, size_t begin, size_t end, PaddedPODArray<UInt64> & buffer)
{
    const auto & data = assert_cast<const ColumnVector<T> &>(positions).getData();
    auto to_position = [&](size_t i)
    {
        if constexpr (is_signed_v<T>)
        {
            if (data[i] < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deleted row position {} is negative", static_cast<Int64>(data[i]));
        }
        const auto position = static_cast<UInt64>(data[i]);
        validateDeletionVectorPosition(position);
        return position;
    };

    if (end - begin == 1)
    {
        bitmap.add(to_position(begin));
        return;
    }

    buffer.resize(end - begin);
    for (size_t i = begin; i < end; ++i)
        buffer[i - begin] = to_position(i);
    bitmap.addMany(buffer.size(), buffer.data());
}

class PuffinDeletionVectorBuilder final : public IPuffinBlobBuilder
{
public:
    explicit PuffinDeletionVectorBuilder(const Block & header);

    void consume(const Chunk & chunk) override;
    void write(PuffinWriter & writer) override;
    void clear() override;

private:
    using AddPositions = void (*)(roaring::Roaring64Map &, const IColumn &, size_t, size_t, PaddedPODArray<UInt64> &);

    bool positions_are_arrays = false;
    AddPositions add_positions = nullptr;
    PaddedPODArray<UInt64> positions_buffer;
    MapWithMemoryTracking<String, roaring::Roaring64Map> deletion_vectors;
};

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

    switch (WhichDataType(positions_type).idx)
    {
        case TypeIndex::UInt8: add_positions = &addPositions<UInt8>; break;
        case TypeIndex::UInt16: add_positions = &addPositions<UInt16>; break;
        case TypeIndex::UInt32: add_positions = &addPositions<UInt32>; break;
        case TypeIndex::UInt64: add_positions = &addPositions<UInt64>; break;
        case TypeIndex::Int8: add_positions = &addPositions<Int8>; break;
        case TypeIndex::Int16: add_positions = &addPositions<Int16>; break;
        case TypeIndex::Int32: add_positions = &addPositions<Int32>; break;
        case TypeIndex::Int64: add_positions = &addPositions<Int64>; break;
        default:
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Puffin output format requires the second column with deleted row positions to be an integer or an array of integers, got {}",
                header.getByPosition(1).type->getName());
    }
}

void PuffinDeletionVectorBuilder::consume(const Chunk & chunk)
{
    const auto & columns = chunk.getColumns();
    const auto files = columns[0]->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();
    const auto positions = columns[1]->convertToFullColumnIfConst();
    const auto * positions_array = positions_are_arrays ? &assert_cast<const ColumnArray &>(*positions) : nullptr;

    const size_t rows = chunk.getNumRows();
    size_t run_begin = 0;
    while (run_begin < rows)
    {
        const std::string_view file = files->getDataAt(run_begin);
        if (file.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Referenced data file of a deletion vector must not be empty");

        size_t run_end = run_begin + 1;
        while (run_end < rows && files->getDataAt(run_end) == file)
            ++run_end;

        auto & bitmap = deletion_vectors[String(file)];
        if (positions_array)
        {
            const auto & offsets = positions_array->getOffsets();
            add_positions(bitmap, positions_array->getData(), offsets[static_cast<ssize_t>(run_begin) - 1], offsets[run_end - 1], positions_buffer);
        }
        else
        {
            add_positions(bitmap, *positions, run_begin, run_end, positions_buffer);
        }
        run_begin = run_end;
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

void PuffinDeletionVectorBuilder::clear()
{
    deletion_vectors.clear();
}


PuffinBlobBuilderPtr createPuffinDeletionVectorBuilder(const Block & header)
{
    return std::make_unique<PuffinDeletionVectorBuilder>(header);
}

}
