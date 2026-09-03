#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/JSONObjectEachRowRowInputFormat.h>
#include <Processors/Formats/Impl/JSONObjectEachRowRowOutputFormat.h>

namespace DB
{

JSONObjectEachRowRowOutputFormat::JSONObjectEachRowRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
    : JSONEachRowRowOutputFormat(out_, header_, settings_), field_index_for_object_name(getColumnIndexForJSONObjectEachRowObjectName(*header_, settings_))
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
        object_name = columns[*field_index_for_object_name]->getDataAt(row);
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

void registerOutputFormatJSONObjectEachRow(FormatFactory & factory);
void registerOutputFormatJSONObjectEachRow(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONObjectEachRow", [](
                       WriteBuffer & buf,
                       const Block & sample,
                       const FormatSettings & _format_settings,
                       FormatFilterInfoPtr /*format_filter_info*/)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONObjectEachRowRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
    });
    factory.markOutputFormatSupportsParallelFormatting("JSONObjectEachRow");
    factory.markFormatHasNoAppendSupport("JSONObjectEachRow");
    factory.setContentType("JSONObjectEachRow", "application/json; charset=UTF-8");
    /// The field names are emitted as the inner JSON object keys every row via `makeNamesValidJSONStrings`
    /// with `output_format_json_validate_utf8` (the path inherited from `JSONEachRow`). When validation
    /// is off, a name that is not valid UTF-8 (a quoted alias with arbitrary bytes) makes the keys, and
    /// hence the output, non-textual. This is knowable from the header, so the text framings reject or
    /// base64-encode accordingly. The column selected by `format_json_object_each_row_column_for_object_name`
    /// is not emitted as an inner object key (its values become the outer object names instead), so its
    /// name does not participate in the check - but its values do, see below.
    factory.registerOutputFormatMayProduceRawBytesChecker(
        "JSONObjectEachRow",
        [](const FormatSettings & settings, const Block & header)
        {
            const String & column_for_object_name = settings.json_object_each_row.column_for_object_name;

            /// The values of the column selected by `format_json_object_each_row_column_for_object_name`
            /// become the outer object keys, and they are written verbatim: `writeRowStartDelimiter`
            /// passes them to `JSONUtils::writeCompactObjectStart`, which emits the title with
            /// no escaping and no UTF-8 validation. So an arbitrary `String` value
            /// (a quote, a newline, or non-UTF-8 bytes) makes the whole output non-textual, and that
            /// is data-dependent, not knowable from the header. Fail close: whenever such a column is
            /// selected, the carrier counts as possibly producing raw bytes, so the text framings
            /// reject it or switch to a base64 payload.
            if (!column_for_object_name.empty())
                return true;

            /// Without such a column the object names are synthesized as `row_N`, which is always
            /// textual, and every column of the header is emitted as an inner object key. The field
            /// values can synthesize further object keys from named `Tuple` element names (see
            /// `tupleElementNamesMayProduceRawBytesInJSON`).
            return JSONUtils::namesMayProduceRawBytesInJSON(header.getNames(), settings, settings.json.validate_utf8)
                || JSONUtils::tupleElementNamesMayProduceRawBytesInJSON(header, settings, settings.json.validate_utf8);
        });
}

}
