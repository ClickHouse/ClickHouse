#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Formats/JSONEachRowRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>


namespace DB
{


JSONEachRowRowOutputStream::JSONEachRowRowOutputStream(WriteBuffer & ostr_, const Block & sample, const FormatSettings & settings)
    : ostr(ostr_), settings(settings)
{
    size_t columns = sample.columns();
    fields.resize(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        WriteBufferFromString out(fields[i]);
        writeJSONString(sample.getByPosition(i).name, out, settings);
    }
}


void JSONEachRowRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeString(fields[field_number], ostr);
    writeChar(':', ostr);
    type.serializeTextJSON(column, row_num, ostr, settings);
    ++field_number;
}


void JSONEachRowRowOutputStream::writeFieldDelimiter()
{
    writeChar(',', ostr);
}


void JSONEachRowRowOutputStream::writeRowStartDelimiter()
{
    writeChar('{', ostr);
}


void JSONEachRowRowOutputStream::writeRowEndDelimiter()
{
    writeCString("}\n", ostr);
    field_number = 0;
}


void registerOutputFormatJSONEachRow(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONEachRow", [](
        WriteBuffer & buf,
        const Block & sample,
        const Context &,
        const FormatSettings & format_settings)
    {
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<JSONEachRowRowOutputStream>(buf, sample, format_settings), sample);
    });
}

}
