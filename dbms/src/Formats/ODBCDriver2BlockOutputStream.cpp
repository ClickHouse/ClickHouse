#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Formats/ODBCDriver2BlockOutputStream.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


#include <Core/iostream_debug_helpers.h>


namespace DB
{
ODBCDriver2BlockOutputStream::ODBCDriver2BlockOutputStream(
    WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings)
    : out(out_), header(header_), format_settings(format_settings)
{
}

void ODBCDriver2BlockOutputStream::flush()
{
    out.next();
}

void writeODBCString(WriteBuffer & out, const std::string & str)
{
    writeIntBinary(Int32(str.size()), out);
    out.write(str.data(), str.size());
}

void ODBCDriver2BlockOutputStream::write(const Block & block)
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

            if (col.column->isNullAt(i))
            {
                writeIntBinary(Int32(-1), out);
            }
            else
            {
                {
                    WriteBufferFromString text_out(text_value);
                    col.type->serializeText(*col.column, i, text_out, format_settings);
                }
                writeODBCString(out, text_value);
            }
        }
    }
}

void ODBCDriver2BlockOutputStream::writePrefix()
{
    const size_t columns = header.columns();

    /// Number of header rows.
    writeIntBinary(Int32(3), out);

    /// Names of columns.
    /// Number of columns + 1 for first name column.
    writeIntBinary(Int32(columns + 1), out);
    writeODBCString(out, "name");
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & col = header.getByPosition(i);
        writeODBCString(out, col.name);
    }

    /// Types of columns.
    writeIntBinary(Int32(columns + 1), out);
    writeODBCString(out, "type");
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & col = header.getByPosition(i);
        writeODBCString(out, col.type->getName());
    }

    /// Nullable.
    writeIntBinary(Int32(columns + 1), out);
    writeODBCString(out, "null");
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & col = header.getByPosition(i);
        writeODBCString(out, col.type->isNullable() ? "1" : "0");
    }
}


void registerOutputFormatODBCDriver2(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "ODBCDriver2", [](WriteBuffer & buf, const Block & sample, const Context &, const FormatSettings & format_settings)
        {
            return std::make_shared<ODBCDriver2BlockOutputStream>(buf, sample, format_settings);
        });
}

}
