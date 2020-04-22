#include <Processors/Formats/Impl/TabSeparatedRowOutputFormat.h>
#include <Processors/Formats/Impl/TabSeparatedRawRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>


namespace DB
{
TabSeparatedRowOutputFormat::TabSeparatedRowOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    bool with_names_,
    bool with_types_,
    FormatFactory::WriteCallback callback,
    const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, callback), with_names(with_names_), with_types(with_types_), format_settings(format_settings_)
{
}


void TabSeparatedRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();
    size_t columns = header.columns();

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeEscapedString(header.safeGetByPosition(i).name, out);
            writeChar(i == columns - 1 ? '\n' : '\t', out);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeEscapedString(header.safeGetByPosition(i).type->getName(), out);
            writeChar(i == columns - 1 ? '\n' : '\t', out);
        }
    }
}


void TabSeparatedRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeAsTextEscaped(column, row_num, out, format_settings);
}


void TabSeparatedRowOutputFormat::writeFieldDelimiter()
{
    writeChar('\t', out);
}


void TabSeparatedRowOutputFormat::writeRowEndDelimiter()
{
    if (format_settings.tsv.crlf_end_of_line)
        writeChar('\r', out);
    writeChar('\n', out);
}

void TabSeparatedRowOutputFormat::writeBeforeTotals()
{
    writeChar('\n', out);
}

void TabSeparatedRowOutputFormat::writeBeforeExtremes()
{
    writeChar('\n', out);
}


void registerOutputFormatProcessorTabSeparated(FormatFactory & factory)
{
    for (const auto * name : {"TabSeparated", "TSV"})
    {
        factory.registerOutputFormatProcessor(name, [](
            WriteBuffer & buf,
            const Block & sample,
            FormatFactory::WriteCallback callback,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRowOutputFormat>(buf, sample, false, false, callback, settings);
        });
    }

    for (const auto * name : {"TabSeparatedRaw", "TSVRaw"})
    {
        factory.registerOutputFormatProcessor(name, [](
            WriteBuffer & buf,
            const Block & sample,
            FormatFactory::WriteCallback callback,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRawRowOutputFormat>(buf, sample, false, false, callback, settings);
        });
    }

    for (const auto * name : {"TabSeparatedWithNames", "TSVWithNames"})
    {
        factory.registerOutputFormatProcessor(name, [](
            WriteBuffer & buf,
            const Block & sample,
            FormatFactory::WriteCallback callback,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRowOutputFormat>(buf, sample, true, false, callback, settings);
        });
    }

    for (const auto * name : {"TabSeparatedWithNamesAndTypes", "TSVWithNamesAndTypes"})
    {
        factory.registerOutputFormatProcessor(name, [](
            WriteBuffer & buf,
            const Block & sample,
            FormatFactory::WriteCallback callback,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRowOutputFormat>(buf, sample, true, true, callback, settings);
        });
    }
}

}
