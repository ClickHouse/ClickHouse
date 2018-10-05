#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/TSKVRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>



namespace DB
{

TSKVRowOutputStream::TSKVRowOutputStream(WriteBuffer & ostr_, const Block & sample_, const FormatSettings & format_settings)
    : TabSeparatedRowOutputStream(ostr_, sample_, false, false, format_settings)
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
    type.serializeTextEscaped(column, row_num, ostr, format_settings);
    ++field_number;
}


void TSKVRowOutputStream::writeRowEndDelimiter()
{
    writeChar('\n', ostr);
    field_number = 0;
}


void registerOutputFormatTSKV(FormatFactory & factory)
{
    factory.registerOutputFormat("TSKV", [](
        WriteBuffer & buf,
        const Block & sample,
        const Context &,
        const FormatSettings & settings)
    {
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<TSKVRowOutputStream>(buf, sample, settings), sample);
    });
}

}
