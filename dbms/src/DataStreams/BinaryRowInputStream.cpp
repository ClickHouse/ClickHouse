#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <DataStreams/BinaryRowInputStream.h>


namespace DB
{

BinaryRowInputStream::BinaryRowInputStream(ReadBuffer & istr_)
    : istr(istr_)
{
}


bool BinaryRowInputStream::read(Block & block)
{
    if (istr.eof())
        return false;

    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        MutableColumnPtr column = block.getByPosition(i).column->mutate();
        block.getByPosition(i).type->deserializeBinary(*column, istr);
        block.getByPosition(i).column = std::move(column);
    }

    return true;
}

}
