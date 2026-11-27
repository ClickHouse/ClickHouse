#include <Columns/ColumnAggregateFunction.h>
#include <Columns/IColumn.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Processors/Formats/Impl/RawBLOBRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>

namespace DB
{


RawBLOBRowOutputFormat::RawBLOBRowOutputFormat(
    WriteBuffer & out_,
    SharedHeader header_,
    const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_)
    , format_settings(format_settings_)
{
}


void RawBLOBRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    /// `getDataAt` of `ColumnAggregateFunction` returns the bytes of the pointer to the state, not the state itself.
    /// Write the serialized state, the same as its conversion to `String`.
    if (typeid_cast<const ColumnAggregateFunction *>(&column))
    {
        serialization.serializeBinary(column, row_num, out, format_settings);
        return;
    }

    if (!column.isNullAt(row_num))
    {
        auto value = column.getDataAt(row_num);
        out.write(value.data(), value.size());
    }
}


void registerOutputFormatRawBLOB(FormatFactory & factory);
void registerOutputFormatRawBLOB(FormatFactory & factory)
{
    factory.registerOutputFormat("RawBLOB", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<RawBLOBRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
    });

    /// The output is a verbatim copy of the column bytes, which are not guaranteed to be valid UTF-8 text.
    factory.markOutputFormatMayProduceRawBytes("RawBLOB");
}

}
