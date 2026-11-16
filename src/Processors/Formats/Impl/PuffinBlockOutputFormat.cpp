#include <Processors/Formats/Impl/PuffinBlockOutputFormat.h>
#include <Processors/Formats/Impl/PuffinDeletionVector.h>
#include <Processors/Formats/Impl/PuffinWriter.h>

#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

PuffinBlockOutputFormat::PuffinBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_)
    , format_settings(format_settings_)
{
    const Block & header = getPort(PortKind::Main).getHeader();
    const String & blob_type = format_settings.puffin.output_blob_type;
    if (blob_type == PUFFIN_DELETION_VECTOR_BLOB_TYPE)
    {
        initDeletionVectors(header);
    }
    else if (blob_type == PUFFIN_DATASKETCHES_THETA_BLOB_TYPE)
    {
        write_theta_sketches = true;
        initThetaSketches(header);
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unknown Puffin blob type '{}' in setting `output_format_puffin_blob_type`, supported types: '{}', '{}'",
            blob_type,
            PUFFIN_DELETION_VECTOR_BLOB_TYPE,
            PUFFIN_DATASKETCHES_THETA_BLOB_TYPE);
    }
}

void PuffinBlockOutputFormat::initThetaSketches(const Block & header)
{
#if USE_DATASKETCHES
    if (header.columns() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin output format requires at least one column to write Theta sketches");

    const String & field_ids_setting = format_settings.puffin.output_field_ids;
    if (field_ids_setting.empty())
    {
        for (size_t i = 0; i < header.columns(); ++i)
            theta_field_ids.push_back(static_cast<Int32>(i + 1));
    }
    else
    {
        std::vector<String> parts;
        boost::split(parts, field_ids_setting, boost::is_any_of(","));
        std::unordered_set<Int32> seen;
        for (auto & part : parts)
        {
            boost::trim(part);
            Int32 field_id = 0;
            if (!tryParse(field_id, part) || field_id <= 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Setting `output_format_puffin_field_ids` must be a comma-separated list of positive integers, got '{}'",
                    field_ids_setting);
            if (!seen.insert(field_id).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `output_format_puffin_field_ids` contains field id {} twice", field_id);
            theta_field_ids.push_back(field_id);
        }
        if (theta_field_ids.size() != header.columns())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Setting `output_format_puffin_field_ids` has {} field ids, but {} columns are written: {}",
                theta_field_ids.size(),
                header.columns(),
                header.dumpNames());
    }

    theta_sketches.reserve(header.columns());
    for (const auto & column : header)
        theta_sketches.emplace_back(column.name, column.type);
#else
    UNUSED(header);
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Writing Theta sketches to Puffin files is disabled, ClickHouse was built without DataSketches");
#endif
}

void PuffinBlockOutputFormat::initDeletionVectors(const Block & header)
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
    if (write_theta_sketches)
    {
#if USE_DATASKETCHES
        const auto & columns = chunk.getColumns();
        for (size_t i = 0; i < columns.size(); ++i)
            theta_sketches[i].add(*columns[i]);
#endif
        return;
    }
    consumeDeletionVectors(chunk);
}

void PuffinBlockOutputFormat::consumeDeletionVectors(const Chunk & chunk)
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
#if USE_DATASKETCHES
    for (size_t i = 0; i < theta_sketches.size(); ++i)
        theta_sketches[i].write(
            writer, theta_field_ids[i], format_settings.puffin.output_snapshot_id, format_settings.puffin.output_sequence_number);
#endif
    for (auto & [referenced_data_file, positions] : deletion_vectors)
        writeDeletionVectorBlob(writer, referenced_data_file, positions);
    writer.finalize();
}

void PuffinBlockOutputFormat::resetFormatterImpl()
{
    deletion_vectors.clear();
#if USE_DATASKETCHES
    for (auto & sketch : theta_sketches)
        sketch.reset();
#endif
}

void registerOutputFormatPuffin(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Puffin",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & format_settings, FormatFilterInfoPtr)
        { return std::make_shared<PuffinBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings); });
    factory.markFormatHasNoAppendSupport("Puffin");
    factory.markOutputFormatNotTTYFriendly("Puffin");
    factory.setContentType("Puffin", "application/octet-stream");
}

}
