#include <IO/WriteHelpers.h>
#include <DataStreams/TSKVRowOutputStream.h>


namespace DB
{

TSKVRowOutputStream::TSKVRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
    : TabSeparatedRowOutputStream(ostr_, sample_)
{
    NamesAndTypesList columns(sample_.getColumnsList());
    fields.assign(columns.begin(), columns.end());

    for (auto & field : fields)
    {
        String prepared_field_name;
        {
            WriteBufferFromString wb(prepared_field_name);
            writeAnyEscapedString<'='>(field.name.data(), field.name.data() + field.name.size(), wb);
            writeCString("=", wb);
        }
        field.name = prepared_field_name;
    }
}


void TSKVRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeString(fields[field_number].name, ostr);
    type.serializeTextEscaped(column, row_num, ostr);
    ++field_number;
}


void TSKVRowOutputStream::writeRowEndDelimiter()
{
    writeChar('\n', ostr);
    field_number = 0;
}


}
