#include <Processors/Formats/Impl/PuffinBlockOutputFormat.h>
#include <Processors/Formats/Impl/PuffinDeletionVector.h>
#include <Processors/Formats/Impl/PuffinWriter.h>

#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

PuffinBlockOutputFormat::PuffinBlockOutputFormat(WriteBuffer & out_, SharedHeader header_)
    : IOutputFormat(header_, out_)
{
    const Block & header = getPort(PortKind::Main).getHeader();
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

void PuffinBlockOutputFormat::addPositions(roaring::Roaring64Map & bitmap, const IColumn & positions, size_t begin, size_t end) const
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

void PuffinBlockOutputFormat::consume(Chunk chunk)
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

void PuffinBlockOutputFormat::finalizeImpl()
{
    PuffinWriter writer(out);
    for (auto & [referenced_data_file, positions] : deletion_vectors)
        writeDeletionVectorBlob(writer, referenced_data_file, positions);
    writer.finalize();
}

void PuffinBlockOutputFormat::resetFormatterImpl()
{
    deletion_vectors.clear();
}

void registerOutputFormatPuffin(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Puffin",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings &, FormatFilterInfoPtr)
        { return std::make_shared<PuffinBlockOutputFormat>(buf, std::make_shared<const Block>(sample)); });
    factory.markFormatHasNoAppendSupport("Puffin");
    factory.markOutputFormatNotTTYFriendly("Puffin");
    factory.setContentType("Puffin", "application/octet-stream");
}

}
