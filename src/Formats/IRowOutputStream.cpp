#include <Common/Exception.h>
#include <Core/Block.h>
#include <Formats/IRowOutputStream.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


void IRowOutputStream::write(const Block & block, size_t row_num)
{
    size_t columns = block.columns();

    writeRowStartDelimiter();

    for (size_t i = 0; i < columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        const auto & col = block.getByPosition(i);
        writeField(*col.column, *col.type, row_num);
    }

    writeRowEndDelimiter();
}

void IRowOutputStream::writeField(const IColumn &, const IDataType &, size_t)
{
    throw Exception("Method writeField is not implemented for output format", ErrorCodes::NOT_IMPLEMENTED);
}

}
