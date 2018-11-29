#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/BinaryRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>


namespace DB
{

BinaryRowOutputStream::BinaryRowOutputStream(WriteBuffer & ostr_)
    : ostr(ostr_)
{
}

void BinaryRowOutputStream::flush()
{
    ostr.next();
}

void BinaryRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeBinary(column, row_num, ostr);
}

void registerOutputFormatRowBinary(FormatFactory & factory)
{
    factory.registerOutputFormat("RowBinary", [](
        WriteBuffer & buf,
        const Block & sample,
        const Context &,
        const FormatSettings &)
    {
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<BinaryRowOutputStream>(buf), sample);
    });
}

}
