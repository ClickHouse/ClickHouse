#include <Processors/Formats/Impl/CustomSeparatedRowOutputFormat.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/EscapingRuleUtils.h>
#include <IO/WriteHelpers.h>


namespace DB
{

CustomSeparatedRowOutputFormat::CustomSeparatedRowOutputFormat(
    const Block & header_, WriteBuffer & out_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_, bool with_names_, bool with_types_)
    : IRowOutputFormat(header_, out_, params_)
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
    if (with_names)
    {
        writeLine(header.getNames());
        writeRowBetweenDelimiter();
    }

    if (with_types)
    {
        writeLine(header.getDataTypeNames());
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

void registerOutputFormatCustomSeparated(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerOutputFormat(format_name, [with_names, with_types](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams & params,
            const FormatSettings & settings)
        {
            return std::make_shared<CustomSeparatedRowOutputFormat>(sample, buf, params, settings, with_names, with_types);
        });

        factory.markOutputFormatSupportsParallelFormatting(format_name);

        factory.registerAppendSupportChecker(format_name, [](const FormatSettings & settings)
        {
            return settings.custom.result_after_delimiter.empty();
        });
    };

    registerWithNamesAndTypes("CustomSeparated", register_func);
}

}
