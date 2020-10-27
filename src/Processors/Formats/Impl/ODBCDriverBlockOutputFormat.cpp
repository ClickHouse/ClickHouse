#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Core/Block.h>
#include <Processors/Formats/Impl/ODBCDriverBlockOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

ODBCDriverBlockOutputFormat::ODBCDriverBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), format_settings(format_settings_)
{
}

void ODBCDriverBlockOutputFormat::consume(Chunk chunk)
{
    writePrefixIfNot();

    const size_t num_rows = chunk.getNumRows();
    const size_t num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();
    const auto & header = getPort(PortKind::Main).getHeader();
    String text_value;

    for (size_t i = 0; i < num_rows; ++i)
    {
        for (size_t j = 0; j < num_columns; ++j)
        {
            text_value.resize(0);
            const auto & column = columns[j];
            const auto & type = header.getByPosition(j).type;

            {
                WriteBufferFromString text_out(text_value);
                type->serializeAsText(*column, i, text_out, format_settings);
            }

            writeStringBinary(text_value, out);
        }
    }
}

void ODBCDriverBlockOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();
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

void ODBCDriverBlockOutputFormat::finalize()
{
    writePrefixIfNot();
}

void registerOutputFormatProcessorODBCDriver(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("ODBCDriver", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback,
        const FormatSettings & format_settings)
    {
        return std::make_shared<ODBCDriverBlockOutputFormat>(buf, sample, format_settings);
    });
}

}
