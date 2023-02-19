#include <Processors/Formats/Impl/RawBLOBRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>

namespace DB
{


RawBLOBRowOutputFormat::RawBLOBRowOutputFormat(
    WriteBuffer & out_,
    const Block & header_)
    : IRowOutputFormat(header_, out_)
{
}


void RawBLOBRowOutputFormat::writeField(const IColumn & column, const ISerialization &, size_t row_num)
{
    if (!column.isNullAt(row_num))
    {
        auto value = column.getDataAt(row_num);
        out.write(value.data, value.size);
    }
}


void registerOutputFormatRawBLOB(FormatFactory & factory)
{
    factory.registerOutputFormat("RawBLOB", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings &)
    {
        return std::make_shared<RawBLOBRowOutputFormat>(buf, sample);
    });
}

}
