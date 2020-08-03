#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Processors/Formats/Impl/BinaryRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

BinaryRowOutputFormat::BinaryRowOutputFormat(WriteBuffer & out_, const Block & header, bool with_names_, bool with_types_, FormatFactory::WriteCallback callback)
    : IRowOutputFormat(header, out_, callback), with_names(with_names_), with_types(with_types_)
{
}

void BinaryRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();
    size_t columns = header.columns();

    if (with_names || with_types)
    {
        writeVarUInt(columns, out);
    }

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeStringBinary(header.safeGetByPosition(i).name, out);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeStringBinary(header.safeGetByPosition(i).type->getName(), out);
        }
    }
}

void BinaryRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeBinary(column, row_num, out);
}


void registerOutputFormatProcessorRowBinary(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("RowBinary", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback callback,
        const FormatSettings &)
    {
        return std::make_shared<BinaryRowOutputFormat>(buf, sample, false, false, callback);
    });

    factory.registerOutputFormatProcessor("RowBinaryWithNamesAndTypes", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback callback,
        const FormatSettings &)
    {
        return std::make_shared<BinaryRowOutputFormat>(buf, sample, true, true, callback);
    });
}

}
