#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONCompactEachRowRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/JSONUtils.h>

namespace DB
{


JSONCompactEachRowRowOutputFormat::JSONCompactEachRowRowOutputFormat(WriteBuffer & out_,
        SharedHeader header_,
        const FormatSettings & settings_,
        bool with_names_,
        bool with_types_)
    : RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>(header_, out_, settings_.json.valid_output_on_exception, settings_.json.validate_utf8)
    , settings(settings_)
    , with_names(with_names_)
    , with_types(with_types_)
{
    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();
}


void JSONCompactEachRowRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    if (settings.json.serialize_as_strings)
    {
        WriteBufferFromOwnString buf;

        serialization.serializeText(column, row_num, buf, settings);
        writeJSONString(buf.str(), *ostr, settings);
    }
    else
        serialization.serializeTextJSON(column, row_num, *ostr, settings);
}


void JSONCompactEachRowRowOutputFormat::writeFieldDelimiter()
{
    writeCString(", ", *ostr);
}


void JSONCompactEachRowRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('[', *ostr);
}


void JSONCompactEachRowRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("]\n", *ostr);
}

void JSONCompactEachRowRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeChar('\n', *ostr);
    size_t columns_size = columns.size();
    writeRowStartDelimiter();
    for (size_t i = 0; i < columns_size; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }
    writeRowEndDelimiter();
}

void JSONCompactEachRowRowOutputFormat::writeLine(const std::vector<String> & values)
{
    JSONUtils::makeNamesValidJSONStrings(values, settings, settings.json.validate_utf8);
    writeRowStartDelimiter();
    for (size_t i = 0; i < values.size(); ++i)
    {
        writeChar('\"', *ostr);
        writeString(values[i], *ostr);
        writeChar('\"', *ostr);
        if (i + 1 != values.size())
            writeFieldDelimiter();
    }
    writeRowEndDelimiter();
}

void JSONCompactEachRowRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();

    if (with_names)
        writeLine(JSONUtils::makeNamesValidJSONStrings(header.getNames(), settings, settings.json.validate_utf8));

    if (with_types)
        writeLine(JSONUtils::makeNamesValidJSONStrings(header.getDataTypeNames(), settings, settings.json.validate_utf8));
}

void JSONCompactEachRowRowOutputFormat::writeSuffix()
{
    if (!exception_message.empty())
    {
        if (haveWrittenData())
            writeRowBetweenDelimiter();

        writeRowStartDelimiter();
        writeJSONString(exception_message, *ostr, settings);
        writeRowEndDelimiter();
    }
}

void JSONCompactEachRowRowOutputFormat::resetFormatterImpl()
{
    RowOutputFormatWithExceptionHandlerAdaptor::resetFormatterImpl();
    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();
}

void registerOutputFormatJSONCompactEachRow(FormatFactory & factory)
{
    for (bool yield_strings : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerOutputFormat(format_name, [yield_strings, with_names, with_types](
                WriteBuffer & buf,
                const Block & sample,
                const FormatSettings & format_settings,
                FormatFilterInfoPtr /*format_filter_info*/)
            {
                FormatSettings settings = format_settings;
                settings.json.serialize_as_strings = yield_strings;

                return std::make_shared<JSONCompactEachRowRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings, with_names, with_types);
            });

            factory.markOutputFormatSupportsParallelFormatting(format_name);
        };

        registerWithNamesAndTypes(yield_strings ? "JSONCompactStringsEachRow" : "JSONCompactEachRow", register_func);
    }
}


}
