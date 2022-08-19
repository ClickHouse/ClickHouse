#include <Processors/Formats/Impl/RawBLOBRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>

namespace DB
{


RawBLOBRowOutputFormat::RawBLOBRowOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    const RowOutputFormatParams & params_)
    : IRowOutputFormat(header_, out_, params_)
{
}


void RawBLOBRowOutputFormat::writeField(const IColumn & column, const ISerialization &, size_t row_num)
{
    StringRef value = column.getDataAt(row_num);
    out.write(value.data, value.size);
}


void registerOutputFormatRawBLOB(FormatFactory & factory)
{
    factory.registerOutputFormat("RawBLOB", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings &)
    {
        return std::make_shared<RawBLOBRowOutputFormat>(buf, sample, params);
    });
}

}

