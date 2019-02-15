#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Formats/BinaryRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>


namespace DB
{

BinaryRowInputStream::BinaryRowInputStream(ReadBuffer & istr_, const Block & header_, bool with_names_, bool with_types_)
    : istr(istr_), header(header_), with_names(with_names_), with_types(with_types_)
{
}


bool BinaryRowInputStream::read(MutableColumns & columns, RowReadExtension &)
{
    if (istr.eof())
        return false;

    size_t num_columns = columns.size();
    for (size_t i = 0; i < num_columns; ++i)
        header.getByPosition(i).type->deserializeBinary(*columns[i], istr);

    return true;
}


void BinaryRowInputStream::readPrefix()
{
    /// NOTE The header is completely ignored. This can be easily improved.

    UInt64 columns = 0;
    String tmp;

    if (with_names || with_types)
    {
        readVarUInt(columns, istr);
    }

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            readStringBinary(tmp, istr);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            readStringBinary(tmp, istr);
        }
    }
}


void registerInputFormatRowBinary(FormatFactory & factory)
{
    factory.registerInputFormat("RowBinary", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context &,
        UInt64 max_block_size,
        const FormatSettings & settings)
    {
        return std::make_shared<BlockInputStreamFromRowInputStream>(
            std::make_shared<BinaryRowInputStream>(buf, sample, false, false),
            sample, max_block_size, settings);
    });

    factory.registerInputFormat("RowBinaryWithNamesAndTypes", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context &,
        size_t max_block_size,
        const FormatSettings & settings)
    {
        return std::make_shared<BlockInputStreamFromRowInputStream>(
            std::make_shared<BinaryRowInputStream>(buf, sample, true, true),
            sample, max_block_size, settings);
    });
}

}
