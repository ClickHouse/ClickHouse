#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/BinaryRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>


namespace DB
{

BinaryRowOutputStream::BinaryRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_, bool with_types_)
    : ostr(ostr_), with_names(with_names_), with_types(with_types_), sample(sample_)
{
}

void BinaryRowOutputStream::writePrefix()
{
    size_t columns = sample.columns();

    if (with_names || with_types)
    {
        writeVarUInt(columns, ostr);
    }

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeStringBinary(sample.safeGetByPosition(i).name, ostr);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeStringBinary(sample.safeGetByPosition(i).type->getName(), ostr);
        }
    }
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
            std::make_shared<BinaryRowOutputStream>(buf, sample, false, false), sample);
    });

    factory.registerOutputFormat("RowBinaryWithNamesAndTypes", [](
        WriteBuffer & buf,
        const Block & sample,
        const Context &,
        const FormatSettings &)
    {
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<BinaryRowOutputStream>(buf, sample, true, true), sample);
    });
}

}
