#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <Formats/BinaryRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>


namespace DB
{

BinaryRowInputStream::BinaryRowInputStream(ReadBuffer & istr_, const Block & header_)
    : istr(istr_), header(header_)
{
}


bool BinaryRowInputStream::read(MutableColumns & columns)
{
    if (istr.eof())
        return false;

    size_t num_columns = columns.size();
    for (size_t i = 0; i < num_columns; ++i)
        header.getByPosition(i).type->deserializeBinary(*columns[i], istr);

    return true;
}


void registerInputFormatRowBinary(FormatFactory & factory)
{
    factory.registerInputFormat("RowBinary", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context &,
        size_t max_block_size,
        const FormatSettings & settings)
    {
        return std::make_shared<BlockInputStreamFromRowInputStream>(
            std::make_shared<BinaryRowInputStream>(buf, sample),
            sample, max_block_size, settings);
    });
}

}
