#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Core/Block.h>
#include <DataStreams/ODBCDriverBlockOutputStream.h>


namespace DB
{

ODBCDriverBlockOutputStream::ODBCDriverBlockOutputStream(WriteBuffer & out_, const Block & sample_)
    : out(out_)
    , sample(sample_)
{
}

void ODBCDriverBlockOutputStream::flush()
{
    out.next();
}

void ODBCDriverBlockOutputStream::write(const Block & block)
{
    const size_t rows = block.rows();
    const size_t columns = block.columns();
    String text_value;

    for (size_t i = 0; i < rows; ++i)
    {
        for (size_t j = 0; j < columns; ++j)
        {
            text_value.resize(0);
            const ColumnWithTypeAndName & col = block.getByPosition(j);

            {
                WriteBufferFromString text_out(text_value);
                col.type->serializeText(*col.column.get(), i, text_out);
            }

            writeStringBinary(text_value, out);
        }
    }
}

void ODBCDriverBlockOutputStream::writePrefix()
{
    const size_t columns = sample.columns();

    /// Number of columns.
    writeVarUInt(columns, out);

    /// Names and types of columns.
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & col = sample.getByPosition(i);

        writeStringBinary(col.name, out);
        writeStringBinary(col.type->getName(), out);
    }
}

}
