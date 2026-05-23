#include <Processors/Formats/Impl/PNGOutputFormat.h>

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/PNGSerializer.h>
#include <Formats/PNGWriter.h>

namespace DB
{

namespace
{
constexpr auto FORMAT_NAME = "PNG";
}

PNGOutputFormat::PNGOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_)
    , writer(std::make_unique<PNGWriter>(out_))
    , serializer(std::make_unique<PNGSerializer>(*header_, settings_, *writer))
{
}

void PNGOutputFormat::consume(Chunk chunk)
{
    const auto & cols = chunk.getColumns();
    const auto num_rows = chunk.getNumRows();
    if (cols.empty() || num_rows == 0)
        return;

    serializer->setColumns(cols.data(), cols.size());
    for (size_t i = 0; i < num_rows; ++i)
        serializer->writeRow(i);
}

void PNGOutputFormat::finalizeImpl()
{
    serializer->finalizeWrite();
}

void registerOutputFormatPNG(FormatFactory & factory)
{
    factory.registerOutputFormat(
        FORMAT_NAME,
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr)
        {
            return std::make_shared<PNGOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
        });
}

}
