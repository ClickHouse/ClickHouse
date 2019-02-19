#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ODBCDriver2BlockOutputFormat.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


#include <Core/iostream_debug_helpers.h>


namespace DB
{
ODBCDriver2BlockOutputFormat::ODBCDriver2BlockOutputFormat(
    WriteBuffer & out, Block header, const FormatSettings & format_settings)
    : IOutputFormat(std::move(header), out), format_settings(format_settings)
{
}

void writeODBCString(WriteBuffer & out, const std::string & str)
{
    writeIntBinary(Int32(str.size()), out);
    out.write(str.data(), str.size());
}

void ODBCDriver2BlockOutputFormat::writeRow(const Block & header, const Columns & columns, size_t row_idx, std::string & buffer)
{
    size_t num_columns = columns.size();
    for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
    {
        buffer.clear();
        auto & column = columns[column_idx];

        if (column->isNullAt(row_idx))
        {
            writeIntBinary(Int32(-1), out);
        }
        else
        {
            {
                WriteBufferFromString text_out(buffer);
                header.getByPosition(row_idx).type->serializeAsText(*column, row_idx, text_out, format_settings);
            }
            writeODBCString(out, buffer);
        }
    }
}

void ODBCDriver2BlockOutputFormat::write(Chunk chunk, PortKind port_kind)
{
    String text_value;
    auto & header = getPort(port_kind).getHeader();
    auto & columns = chunk.getColumns();
    const size_t rows = chunk.getNumRows();
    for (size_t i = 0; i < rows; ++i)
        writeRow(header, columns, i, text_value);
}

void ODBCDriver2BlockOutputFormat::consume(Chunk chunk)
{
    writePrefixIfNot();
    write(std::move(chunk), PortKind::Main);
}

void ODBCDriver2BlockOutputFormat::consumeTotals(Chunk chunk)
{
    writePrefixIfNot();
    write(std::move(chunk), PortKind::Totals);
}

void ODBCDriver2BlockOutputFormat::finalize()
{
    writePrefixIfNot();
}

void ODBCDriver2BlockOutputFormat::writePrefix()
{
    auto & header = getPort(PortKind::Main).getHeader();
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
        const ColumnWithTypeAndName & col = header.getByPosition(i);
        writeODBCString(out, col.type->getName());
    }
}


void registerOutputFormatProcessorODBCDriver2(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor(
        "ODBCDriver2", [](WriteBuffer & buf, const Block & sample, const Context &, const FormatSettings & format_settings)
        {
            return std::make_shared<ODBCDriver2BlockOutputFormat>(buf, sample, format_settings);
        });
}

}
