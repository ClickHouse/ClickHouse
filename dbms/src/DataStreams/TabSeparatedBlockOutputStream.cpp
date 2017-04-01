#include <IO/WriteBuffer.h>
#include <Core/Block.h>
#include <DataStreams/TabSeparatedBlockOutputStream.h>


namespace DB
{

void TabSeparatedBlockOutputStream::flush()
{
    ostr.next();
}

void TabSeparatedBlockOutputStream::write(const Block & block)
{
    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & col = block.safeGetByPosition(i);

        size_t rows = block.rows();
        for (size_t j = 0; j < rows; ++j)
        {
            if (j != 0)
                ostr.write('\t');
            col.type->serializeTextEscaped(*col.column.get(), j, ostr);
        }
        ostr.write('\n');
    }
    ostr.write('\n');
}

}
