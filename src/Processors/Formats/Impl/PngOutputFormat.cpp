
#include "base/types.h"

#include "PngOutputFormat.h"
#include <Formats/FormatFactory.h>
#include <Interpreters/ProcessList.h>
#include <Formats/FormatSettings.h>
#include <Common/Exception.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Formats/PngSerializer.h>
#include <iostream>

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
}

PngOutputFormat::PngOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_), 
    format_settings{settings_}, 
    serializations(header_.getSerializations()) 
{   
    max_width = format_settings.png_image.max_width;
    max_height = format_settings.png_image.max_height;
    output_format = validateFormat(format_settings.png_image.pixel_output_format);
    writer = std::make_unique<PngWriter>(out_);

    /// TODO: Extra fields {column_names & data_types could be used in serializer for debug information}, i.e PngSerializer::dump method
    Strings column_names = header_.getNames();
    DataTypes data_types = header_.getDataTypes();
    png_serializer = PngSerializer::create(column_names, data_types, max_width, max_height, output_format, *writer);
}

PngPixelFormat PngOutputFormat::validateFormat(const String & mode)
{
    if (mode == "BINARY") 
        return PngPixelFormat::BINARY;
    else if (mode == "GRAYSCALE")
        return PngPixelFormat::GRAYSCALE;
    else if (mode == "RGB")
        return PngPixelFormat::RGB;
    else if (mode == "RGBA")
        return PngPixelFormat::RGBA;
    else
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT, 
            "Invalid pixel mode: {}", mode
        );
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
    size_t ideal_width = static_cast<size_t>(
        std::floor(std::sqrt(
        static_cast<double>(total_row))
    ));
    size_t final_width = std::clamp(ideal_width, static_cast<size_t>(1), max_width);
    size_t final_height = std::min(max_height, (total_row + final_width - 1) / final_width);
    
    LOG_DEBUG(
        getLogger("PngOutputFormat"),
        "Read rows: {}, Ideal png image width: {}, final width: {}, final height: {}",
        total_row, ideal_width, final_width, final_height
    );
    
    png_serializer->finalizeWrite(final_width, final_height);
}

void registerOutputFormatPNG(FormatFactory & factory)
{
    factory.registerOutputFormat(FORMAT_NAME, [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & settings)
    {
        return std::make_shared<PngOutputFormat>(buf, sample, settings);
    });
}

}
