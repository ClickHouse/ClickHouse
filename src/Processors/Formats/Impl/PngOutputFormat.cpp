#include "Formats/PngSerializer.h"
#include "base/types.h"

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/PngWriter.h>
#include <Interpreters/ProcessList.h>
#include <Common/Exception.h>
#include "PngOutputFormat.h"

#include <absl/strings/match.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT;
extern const int NOT_IMPLEMENTED;
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int LOGICAL_ERROR;
extern const int CANNOT_CONVERT_TYPE;
extern const int TOO_MANY_ROWS;
}

namespace
{

constexpr auto FORMAT_NAME = "PNG";

struct FormatNameMapping
{
    std::string_view name;
    PngPixelFormat format;
    int png_color_type;
    int channels;
    bool force_8bit = false;
};

constexpr std::array<FormatNameMapping, 4> format_mappings
    = {{{"BINARY", PngPixelFormat::BINARY, PNG_COLOR_TYPE_GRAY, 1, /* force_8bit */ true},
        {"GRAYSCALE", PngPixelFormat::GRAYSCALE, PNG_COLOR_TYPE_GRAY, 1},
        {"RGB", PngPixelFormat::RGB, PNG_COLOR_TYPE_RGB, 3},
        {"RGBA", PngPixelFormat::RGBA, PNG_COLOR_TYPE_RGBA, 4}}};

const FormatNameMapping & lookupFormat(const String & mode_str)
{
    std::string_view mode_sv = mode_str;
    for (const auto & mapping : format_mappings)
    {
        if (absl::EqualsIgnoreCase(mode_sv, mapping.name))
            return mapping;
    }

    throw Exception(
        ErrorCodes::UNKNOWN_FORMAT,
        "Invalid pixel mode: '{}'. Supported modes are BINARY, GRAYSCALE, RGB, RGBA (case-insensitive)",
        mode_str);
}
}

PngOutputFormat::PngOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_)
    , format_settings{settings_}
    , serializations(header_.getSerializations())
{
    const auto & mapping = lookupFormat(format_settings.png_image.pixel_output_format);

    output_format = mapping.format;
    int png_color_type = mapping.png_color_type;

    int requested_bit_depth = format_settings.png_image.bit_depth;
    int bit_depth = mapping.force_8bit ? 8 : requested_bit_depth;

    if (bit_depth != 8 && bit_depth != 16)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Currently supported only 8 or 16 bit depth, got {}", bit_depth);
    }
    else if (mapping.force_8bit && requested_bit_depth != 8)
    {
        // LOG_WARNING(getLogger("PngOutputFormat"), "Bit depth overridden to 8 for 'BINARY' format");
    }

    DataTypes data_types = header_.getDataTypes();
    max_width = format_settings.png_image.max_width;
    max_height = format_settings.png_image.max_height;

    auto compression_level = format_settings.png_image.compression_level;

    writer = std::make_unique<PngWriter>(out_, bit_depth, png_color_type, compression_level);

    png_serializer = PngSerializer::create(data_types, max_width, max_height, output_format, *writer, bit_depth);
}

void PngOutputFormat::writePrefix()
{
    if (png_serializer)
        png_serializer->reset();
}

void PngOutputFormat::consume(Chunk chunk)
{
    const auto & cols = chunk.getColumns();
    const auto num_rows = chunk.getNumRows();

    if (cols.empty() || num_rows == 0)
        return;

    std::vector<ColumnPtr> column_ptrs;
    column_ptrs.reserve(cols.size());
    for (const auto & c : cols)
        column_ptrs.push_back(c);

    png_serializer->setColumns(column_ptrs.data(), column_ptrs.size());

    for (size_t i = 0; i < num_rows; ++i)
    {
        png_serializer->writeRow(i);
    }
}

void PngOutputFormat::writeSuffix()
{
    size_t total_row = png_serializer->getRowCount();

    if (total_row > max_height * max_width)
    {
        throw Exception(
            ErrorCodes::TOO_MANY_ROWS,
            "The number of pixels ({}) exceeds the maximum allowed resolution ({}x{} = {}). "
            "Adjust your query or the maximum allowed resolution in settings",
            total_row,
            max_width,
            max_height,
            max_width * max_height);
    }

    size_t ideal_width = static_cast<size_t>(std::floor(std::sqrt(static_cast<double>(total_row))));
    size_t final_width = std::clamp(ideal_width, static_cast<size_t>(1), max_width);

    size_t final_height = std::min(max_height, (total_row + final_width - 1) / final_width);
    try
    {
        png_serializer->finalizeWrite(final_width, final_height);
    }
    catch ([[maybe_unused]] const Poco::Exception & e)
    {
        // LOG_ERROR(getLogger("PngOutputFormat"), "Failed to write png image: {}", e.what());
        throw;
    }
}

void registerOutputFormatPNG(FormatFactory & factory)
{
    factory.registerOutputFormat(
        FORMAT_NAME,
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings)
        { return std::make_shared<PngOutputFormat>(buf, sample, settings); });
}

}
