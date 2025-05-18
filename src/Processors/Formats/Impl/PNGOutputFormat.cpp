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

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT;
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_ROWS;
}

namespace
{
constexpr auto FORMAT_NAME = "PNG";
}

PNGOutputFormat::PNGOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_)
{    
    writer = std::make_unique<PNGWriter>(out_, settings_);
    png_serializer = std::make_unique<PngSerializer>(header_, settings_, *writer);  

    log = getLogger("PngOutputFormat");
}   

void PNGOutputFormat::writePrefix()
{
    if (png_serializer)
        png_serializer->reset();
}

void PNGOutputFormat::consume(Chunk chunk)
{
    const auto & cols = chunk.getColumns();
    const auto num_rows = chunk.getNumRows();

    if (cols.empty() || num_rows == 0)
        return;

    std::vector<ColumnPtr> column_ptrs;
    column_ptrs.reserve(cols.size());
    for (const auto & c : cols)
        column_ptrs.push_back(c);

    try
    {
        png_serializer->setColumns(column_ptrs.data(), column_ptrs.size());
    }
    catch (const Poco::Exception & e)
    {
        LOG_ERROR(log, "Failed to set columns for png image: {}", e.what());
        throw;
    }

    for (size_t i = 0; i < num_rows; ++i)
    {
        try
        {
            png_serializer->writeRow(i);
        }
        catch(const Poco::Exception & e)
        {
            LOG_ERROR(log, "Failed to write png image: {}", e.what());
            throw;
        }    
    }
}

void PNGOutputFormat::writeSuffix()
{
    try
    {
        png_serializer->finalizeWrite();
    }
    catch (const Poco::Exception & e)
    {
        LOG_ERROR(log, "Failed to write png image: {}", e.what());
        throw;
    }
}

void registerOutputFormatPNG(FormatFactory & factory)
{
    factory.registerOutputFormat(
        FORMAT_NAME,
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings)
        { return std::make_shared<PNGOutputFormat>(buf, sample, settings); });
}

}
