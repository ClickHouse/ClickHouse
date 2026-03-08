#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>


namespace DB
{

JSONRowOutputFormat::JSONRowOutputFormat(
    WriteBuffer & out_,
    SharedHeader header,
    const FormatSettings & settings_,
    bool yield_strings_)
    : RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>(header, out_, settings_.json.valid_output_on_exception, true), settings(settings_), yield_strings(yield_strings_)
{
    names = JSONUtils::makeNamesValidJSONStrings(header->getNames(), settings, true);
    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();
    settings.json.pretty_print_indent = '\t';
    settings.json.pretty_print_indent_multiplier = 1;
}


void JSONRowOutputFormat::writePrefix()
{
    JSONUtils::writeObjectStart(*ostr);
    JSONUtils::writeMetadata(names, types, settings, *ostr);
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeArrayStart(*ostr, 1, "data");
}


void JSONRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    JSONUtils::writeFieldFromColumn(column, serialization, row_num, yield_strings, settings, *ostr, names[field_number], 3, " ", settings.json.pretty_print);
    ++field_number;
}

void JSONRowOutputFormat::writeFieldDelimiter()
{
    JSONUtils::writeFieldDelimiter(*ostr);
}


void JSONRowOutputFormat::writeRowStartDelimiter()
{
    JSONUtils::writeObjectStart(*ostr, 2);
}


void JSONRowOutputFormat::writeRowEndDelimiter()
{
    JSONUtils::writeObjectEnd(*ostr, 2);
    field_number = 0;
    ++row_count;
}


void JSONRowOutputFormat::writeRowBetweenDelimiter()
{
    JSONUtils::writeFieldDelimiter(*ostr);
}


void JSONRowOutputFormat::writeSuffix()
{
    JSONUtils::writeArrayEnd(*ostr, 1);
}

void JSONRowOutputFormat::writeBeforeTotals()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "totals");
}

void JSONRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    JSONUtils::writeColumns(columns, names, serializations, row_num, yield_strings, settings, *ostr, 2);
}

void JSONRowOutputFormat::writeAfterTotals()
{
    JSONUtils::writeObjectEnd(*ostr, 1);
}

void JSONRowOutputFormat::writeBeforeExtremes()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "extremes");
}

void JSONRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    JSONUtils::writeObjectStart(*ostr, 2, title);
    JSONUtils::writeColumns(columns, names, serializations, row_num, yield_strings, settings, *ostr, 3);
    JSONUtils::writeObjectEnd(*ostr, 2);
}

void JSONRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("min", columns, row_num);
}

void JSONRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("max", columns, row_num);
}

void JSONRowOutputFormat::writeAfterExtremes()
{
    JSONUtils::writeObjectEnd(*ostr, 1);
}

void JSONRowOutputFormat::finalizeImpl()
{
    bool should_write_statistics = settings.write_statistics && exception_message.empty();

    JSONUtils::writeAdditionalInfo(
        row_count,
        statistics.rows_before_limit,
        statistics.applied_limit,
        statistics.rows_before_aggregation,
        statistics.applied_aggregation,
        statistics.watch,
        statistics.progress,
        should_write_statistics && !hasDeferredStatistics(),
        *ostr);

    if (!exception_message.empty())
    {
        writeCString(",\n\n", *ostr);
        JSONUtils::writeException(exception_message, *ostr, settings, 1);
    }

    /// When statistics are deferred, skip closing the document here.
    /// It will be done in writeDeferredStatisticsAndFinalize().
    if (hasDeferredStatistics())
        return;

    JSONUtils::writeObjectEnd(*ostr);
    writeChar('\n', *ostr);
    ostr->next();
}

bool JSONRowOutputFormat::hasDeferredStatistics() const
{
    return settings.write_statistics && exception_message.empty();
}

void JSONRowOutputFormat::writeDeferredStatisticsAndFinalize()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "statistics");

    writeCString("\t\t\"elapsed\": ", *ostr);
    writeText(statistics.watch.elapsedSeconds(), *ostr);
    JSONUtils::writeFieldDelimiter(*ostr);

    writeCString("\t\t\"rows_read\": ", *ostr);
    writeText(statistics.progress.read_rows.load(), *ostr);
    JSONUtils::writeFieldDelimiter(*ostr);

    writeCString("\t\t\"bytes_read\": ", *ostr);
    writeText(statistics.progress.read_bytes.load(), *ostr);

    JSONUtils::writeObjectEnd(*ostr, 1);

    JSONUtils::writeObjectEnd(*ostr);
    writeChar('\n', *ostr);
    ostr->next();
}

void JSONRowOutputFormat::resetFormatterImpl()
{
    RowOutputFormatWithExceptionHandlerAdaptor::resetFormatterImpl();
    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();
    row_count = 0;
    statistics = Statistics();
}

void registerOutputFormatJSON(FormatFactory & factory)
{
    factory.registerOutputFormat("JSON", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<JSONRowOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings, false);
    });

    factory.markOutputFormatSupportsParallelFormatting("JSON");
    factory.markFormatHasNoAppendSupport("JSON");
    factory.setContentType("JSON", "application/json; charset=UTF-8");

    factory.registerOutputFormat("JSONStrings", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<JSONRowOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings, true);
    });

    factory.markOutputFormatSupportsParallelFormatting("JSONStrings");
    factory.markFormatHasNoAppendSupport("JSONStrings");
    factory.setContentType("JSONStrings", "application/json; charset=UTF-8");
}

}
