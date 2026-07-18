#include <Processors/Formats/Impl/CustomSeparatedRowOutputFormat.h>

#include <Common/isValidUTF8.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/FlattenTupleForCSVHeader.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <IO/WriteHelpers.h>
#include <Processors/Port.h>


namespace DB
{

CustomSeparatedRowOutputFormat::CustomSeparatedRowOutputFormat(
    SharedHeader header_, WriteBuffer & out_, const FormatSettings & format_settings_, bool with_names_, bool with_types_)
    : IRowOutputFormat(header_, out_)
    , with_names(with_names_)
    , with_types(with_types_)
    , format_settings(format_settings_)
    , escaping_rule(format_settings.custom.escaping_rule)
{
}

void CustomSeparatedRowOutputFormat::writeLine(const std::vector<String> & values)
{
    writeRowStartDelimiter();
    for (size_t i = 0; i != values.size(); ++i)
    {
        writeStringByEscapingRule(values[i], out, escaping_rule, format_settings);
        if (i + 1 != values.size())
            writeFieldDelimiter();
    }
    writeRowEndDelimiter();
}

void CustomSeparatedRowOutputFormat::writePrefix()
{
    writeString(format_settings.custom.result_before_delimiter, out);

    const auto & header = getPort(PortKind::Main).getHeader();

    /// Tuple values are flattened into separate columns only under the CSV escaping rule, and the
    /// tuple elements are joined with csv.tuple_delimiter. CustomSeparated joins fields with
    /// custom.field_delimiter, so flattening the header only matches the data when that delimiter is
    /// the same single character as csv.tuple_delimiter; otherwise a tuple value stays one custom
    /// field while a flattened header would emit several (issue #107342).
    const bool flatten = escaping_rule == EscapingRule::CSV
        && format_settings.csv.serialize_tuple_into_separate_columns
        && format_settings.csv.header_serialize_tuple_into_separate_columns
        && format_settings.custom.field_delimiter.size() == 1
        && format_settings.custom.field_delimiter[0] == format_settings.csv.tuple_delimiter;

    Names names;
    Names type_names;
    getCSVHeaderNamesAndTypes(header, flatten, names, type_names);

    if (with_names)
    {
        writeLine(names);
        writeRowBetweenDelimiter();
    }

    if (with_types)
    {
        writeLine(type_names);
        writeRowBetweenDelimiter();
    }
}

void CustomSeparatedRowOutputFormat::writeSuffix()
{
    writeString(format_settings.custom.result_after_delimiter, out);
}

void CustomSeparatedRowOutputFormat::writeRowStartDelimiter()
{
    writeString(format_settings.custom.row_before_delimiter, out);
}

void CustomSeparatedRowOutputFormat::writeFieldDelimiter()
{
    writeString(format_settings.custom.field_delimiter, out);
}

void CustomSeparatedRowOutputFormat::writeRowEndDelimiter()
{
    writeString(format_settings.custom.row_after_delimiter, out);
}

void CustomSeparatedRowOutputFormat::writeRowBetweenDelimiter()
{
    writeString(format_settings.custom.row_between_delimiter, out);
}

void CustomSeparatedRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serializeFieldByEscapingRule(column, serialization, out, row_num, escaping_rule, format_settings);
}

void registerOutputFormatCustomSeparated(FormatFactory & factory);
void registerOutputFormatCustomSeparated(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerOutputFormat(format_name, [with_names, with_types](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & settings,
            FormatFilterInfoPtr /*format_filter_info*/)
        {
            return std::make_shared<CustomSeparatedRowOutputFormat>(std::make_shared<const Block>(sample), buf, settings, with_names, with_types);
        });

        factory.markOutputFormatSupportsParallelFormatting(format_name);

        factory.registerAppendSupportChecker(format_name, [](const FormatSettings & settings)
        {
            return settings.custom.result_after_delimiter.empty();
        });

        /// With the `Raw` escaping rule the fields are written verbatim (like `TSVRaw`), so the output
        /// is not guaranteed to be valid UTF-8 text and cannot be embedded into a text framing format.
        /// The literal delimiters are written verbatim regardless of the escaping rule, so a delimiter
        /// that is not valid UTF-8 (for example `format_custom_row_after_delimiter` set to a non-UTF-8
        /// byte sequence) makes the output non-textual as well. The `*WithNames*` variants also write
        /// the column names (and data type names) into the header, and neither the escaping rule nor
        /// the delimiters validate UTF-8, so a name that is not valid UTF-8 makes the output
        /// non-textual too. All of this is knowable from the settings and the header, so it is detected
        /// here rather than relying on the payload being valid UTF-8.
        factory.registerOutputFormatMayProduceRawBytesChecker(
            format_name,
            [with_names, with_types](const FormatSettings & settings, const Block & header)
        {
            const auto & custom = settings.custom;
            if (custom.escaping_rule == FormatSettings::EscapingRule::Raw)
                return true;
            auto is_not_valid_utf8 = [](const std::string & s)
            {
                return !UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(s.data()), s.size());
            };
            return is_not_valid_utf8(custom.result_before_delimiter)
                || is_not_valid_utf8(custom.result_after_delimiter)
                || is_not_valid_utf8(custom.row_before_delimiter)
                || is_not_valid_utf8(custom.row_after_delimiter)
                || is_not_valid_utf8(custom.row_between_delimiter)
                || is_not_valid_utf8(custom.field_delimiter)
                || headerNamesMayProduceRawBytes(header, with_names, with_types);
        });

        /// The `CSV` and `XML` escaping rules pass a carriage return in a `String` value through
        /// verbatim, and the literal delimiters may contain one themselves. A raw carriage return
        /// cannot survive the text `EventStream` framing, so such output is base64-encoded there
        /// (the `Raw` escaping rule is already covered by the raw-bytes check above).
        factory.registerOutputFormatMayEmitCarriageReturnChecker(format_name, [](const FormatSettings & settings)
        {
            const auto & custom = settings.custom;
            return custom.escaping_rule == FormatSettings::EscapingRule::CSV
                || custom.escaping_rule == FormatSettings::EscapingRule::XML
                || custom.result_before_delimiter.contains('\r')
                || custom.result_after_delimiter.contains('\r')
                || custom.row_before_delimiter.contains('\r')
                || custom.row_after_delimiter.contains('\r')
                || custom.row_between_delimiter.contains('\r')
                || custom.field_delimiter.contains('\r');
        });
    };

    registerWithNamesAndTypes("CustomSeparated", register_func);
}

}
