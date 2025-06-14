#include <Columns/IColumn.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/JSONObjectEachRowRowInputFormat.h>
#include <Processors/Formats/Impl/JSONObjectEachRowRowOutputFormat.h>

namespace DB
{

JSONObjectEachRowRowOutputFormat::JSONObjectEachRowRowOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_)
    : JSONEachRowRowOutputFormat(out_, header_, settings_), field_index_for_object_name(getColumnIndexForJSONObjectEachRowObjectName(header_, settings_))
{
}

void JSONObjectEachRowRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row)
{
    if (field_number == field_index_for_object_name)
    {
        ++field_number;
        return;
    }
    JSONEachRowRowOutputFormat::writeField(column, serialization, row);
}

void JSONObjectEachRowRowOutputFormat::write(const Columns & columns, size_t row)
{
    if (field_index_for_object_name)
        object_name = columns[*field_index_for_object_name]->getDataAt(row).toString();
    else
        object_name = "row_" + std::to_string(getRowsReadBefore() + rows + 1);

    ++rows;
    RowOutputFormatWithExceptionHandlerAdaptor::write(columns, row);
}

void JSONObjectEachRowRowOutputFormat::writeFieldDelimiter()
{
    /// We should not write comma before column that is used for
    /// object name and also after it if it's in the first place
    if (field_number != field_index_for_object_name && !(field_index_for_object_name == 0 && field_number == 1))
        JSONEachRowRowOutputFormat::writeFieldDelimiter();
}

void JSONObjectEachRowRowOutputFormat::writePrefix()
{
    JSONUtils::writeObjectStart(*ostr);
}

void JSONObjectEachRowRowOutputFormat::writeRowStartDelimiter()
{
    JSONUtils::writeCompactObjectStart(*ostr, 1, object_name.c_str());
}

void JSONObjectEachRowRowOutputFormat::writeRowEndDelimiter()
{
    JSONUtils::writeCompactObjectEnd(*ostr);
    field_number = 0;
}

void JSONObjectEachRowRowOutputFormat::writeRowBetweenDelimiter()
{
    JSONUtils::writeFieldDelimiter(*ostr, 1);
}

void JSONObjectEachRowRowOutputFormat::writeSuffix()
{
    if (!exception_message.empty())
    {
        if (haveWrittenData())
            writeRowBetweenDelimiter();
        JSONUtils::writeException(exception_message, *ostr, settings, 1);
    }

    JSONUtils::writeObjectEnd(*ostr);
    writeChar('\n', *ostr);
}

void registerOutputFormatJSONObjectEachRow(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONObjectEachRow", [](
                       WriteBuffer & buf,
                       const Block & sample,
                       const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONObjectEachRowRowOutputFormat>(buf, sample, settings);
    });
    factory.markOutputFormatSupportsParallelFormatting("JSONObjectEachRow");
    factory.markFormatHasNoAppendSupport("JSONObjectEachRow");
    factory.setContentType("JSONObjectEachRow", "application/json; charset=UTF-8");
}

}
