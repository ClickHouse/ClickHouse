#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Core/Block.h>
#include <Formats/ODBCDriverBlockOutputStream.h>
#include <Formats/FormatFactory.h>


namespace DB
{

ODBCDriverBlockOutputStream::ODBCDriverBlockOutputStream(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings)
    : out(out_), header(header_), format_settings(format_settings)
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
                col.type->serializeText(*col.column, i, text_out, format_settings);
            }

            writeStringBinary(text_value, out);
        }
    }
}

void ODBCDriverBlockOutputStream::writePrefix()
{
    const size_t columns = header.columns();

    /// Number of columns.
    writeVarUInt(columns, out);

    /// Names and types of columns.
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & col = header.getByPosition(i);

        writeStringBinary(col.name, out);
        writeStringBinary(col.type->getName(), out);
    }
}


void registerOutputFormatODBCDriver(FormatFactory & factory)
{
    factory.registerOutputFormat("ODBCDriver", [](
        WriteBuffer & buf,
        const Block & sample,
        const Context &,
        const FormatSettings & format_settings)
    {
        return std::make_shared<ODBCDriverBlockOutputStream>(buf, sample, format_settings);
    });
}

}
