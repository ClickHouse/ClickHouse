#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Formats/ODBCDriver2BlockOutputStream.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeLowCardinality.h>

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

static void writeRow(const Block & block, size_t row_idx, WriteBuffer & out, const FormatSettings & format_settings, std::string & buffer)
{
    size_t columns = block.columns();
    for (size_t column_idx = 0; column_idx < columns; ++column_idx)
    {
        buffer.clear();
        const ColumnWithTypeAndName & col = block.getByPosition(column_idx);

        if (col.column->isNullAt(row_idx))
        {
            writeIntBinary(Int32(-1), out);
        }
        else
        {
            {
                WriteBufferFromString text_out(buffer);
                col.type->serializeAsText(*col.column, row_idx, text_out, format_settings);
            }
            writeODBCString(out, buffer);
        }
    }
}

void ODBCDriver2BlockOutputStream::write(const Block & block)
{
    String text_value;
    const size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
        writeRow(block, i, out, format_settings, text_value);
}

void ODBCDriver2BlockOutputStream::writeSuffix()
{
    if (totals)
        write(totals);
}

void ODBCDriver2BlockOutputStream::writePrefix()
{
    const size_t columns = header.columns();

    /// Number of header rows.
    writeIntBinary(Int32(2), out);

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
        auto type = header.getByPosition(i).type;
        if (type->lowCardinality())
            type = recursiveRemoveLowCardinality(type);
        writeODBCString(out, type->getName());
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
