#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <DataStreams/BinaryRowInputStream.h>


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

}
