#include <Core/Block.h>
#include <Formats/IRowOutputStream.h>


namespace DB
{

void IRowOutputStream::write(const Block & block, size_t row_num)
{
    size_t columns = block.columns();

    writeRowStartDelimiter();

    for (size_t i = 0; i < columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        auto & col = block.getByPosition(i);
        writeField(*col.column, *col.type, row_num);
    }

    writeRowEndDelimiter();
}

}
