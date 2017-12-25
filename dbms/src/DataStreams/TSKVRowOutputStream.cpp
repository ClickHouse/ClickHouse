#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <DataStreams/TSKVRowOutputStream.h>


namespace DB
{

TSKVRowOutputStream::TSKVRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
    : TabSeparatedRowOutputStream(ostr_, sample_)
{
    NamesAndTypesList columns(sample_.getNamesAndTypesList());
    fields.assign(columns.begin(), columns.end());

    for (auto & field : fields)
    {
        WriteBufferFromOwnString wb;
        writeAnyEscapedString<'='>(field.name.data(), field.name.data() + field.name.size(), wb);
        writeCString("=", wb);
        field.name = wb.str();
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
