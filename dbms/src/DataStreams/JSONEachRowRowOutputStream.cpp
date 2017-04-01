#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <DataStreams/JSONEachRowRowOutputStream.h>


namespace DB
{


JSONEachRowRowOutputStream::JSONEachRowRowOutputStream(WriteBuffer & ostr_, const Block & sample, bool force_quoting_64bit_integers_)
    : ostr(ostr_), force_quoting_64bit_integers(force_quoting_64bit_integers_)
{
    size_t columns = sample.columns();
    fields.resize(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        WriteBufferFromString out(fields[i]);
        writeJSONString(sample.getByPosition(i).name, out);
    }
}


void JSONEachRowRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeString(fields[field_number], ostr);
    writeChar(':', ostr);
    type.serializeTextJSON(column, row_num, ostr, force_quoting_64bit_integers);
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

}
