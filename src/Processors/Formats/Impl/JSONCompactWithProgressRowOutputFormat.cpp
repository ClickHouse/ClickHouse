#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <Processors/Formats/Impl/JSONCompactWithProgressRowOutputFormat.h>

#include <IO/WriteHelpers.h>


namespace DB
{

JSONCompactWithProgressRowOutputFormat::JSONCompactWithProgressRowOutputFormat(
    WriteBuffer & out_, const Block & header, const FormatSettings & settings_, bool yield_strings_)
    : JSONRowOutputFormat(out_, header, settings_, yield_strings_)
{
}

void JSONCompactWithProgressRowOutputFormat::writePrefix()
{
    JSONUtils::writeCompactObjectStart(*ostr);
    JSONUtils::writeCompactMetadata(names, types, settings, *ostr);
    JSONUtils::writeCompactObjectEnd(*ostr);
    writeCString("\n", *ostr);
}

void JSONCompactWithProgressRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    JSONUtils::writeFieldFromColumn(column, serialization, row_num, yield_strings, settings, *ostr);
    ++field_number;
}

void JSONCompactWithProgressRowOutputFormat::writeFieldDelimiter()
{
    JSONUtils::writeFieldCompactDelimiter(*ostr);
}

void JSONCompactWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    if (has_progress)
        writeProgress();
    writeCString("{\"data\":", *ostr);
    JSONUtils::writeCompactArrayStart(*ostr);
}

void JSONCompactWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    JSONUtils::writeCompactArrayEnd(*ostr);
    writeCString("}\n", *ostr);
    field_number = 0;
    ++row_count;
}

void JSONCompactWithProgressRowOutputFormat::writeRowBetweenDelimiter()
{
}

void JSONCompactWithProgressRowOutputFormat::writeBeforeTotals()
{
    JSONUtils::writeCompactObjectStart(*ostr);
    JSONUtils::writeCompactArrayStart(*ostr, 0, "totals");
}

void JSONCompactWithProgressRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    JSONUtils::writeCompactColumns(columns, serializations, row_num, yield_strings, settings, *ostr);
}

void JSONCompactWithProgressRowOutputFormat::writeAfterTotals()
{
    JSONUtils::writeCompactArrayEnd(*ostr);
    JSONUtils::writeCompactObjectEnd(*ostr);
    writeCString("\n", *ostr);
}

void JSONCompactWithProgressRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    JSONUtils::writeCompactArrayStart(*ostr, 2, title);
    JSONUtils::writeCompactColumns(columns, serializations, row_num, yield_strings, settings, *ostr);
    JSONUtils::writeCompactArrayEnd(*ostr);
}

void JSONCompactWithProgressRowOutputFormat::onProgress(const Progress & value)
{
    statistics.progress.incrementPiecewiseAtomically(value);
    String progress_line;
    WriteBufferFromString buf(progress_line);
    writeCString("{\"progress\":", buf);
    statistics.progress.writeJSON(buf);
    writeCString("}\n", buf);
    buf.finalize();
    std::lock_guard lock(progress_lines_mutex);
    progress_lines.emplace_back(std::move(progress_line));
    has_progress = true;
}


void JSONCompactWithProgressRowOutputFormat::flush()
{
    if (has_progress)
        writeProgress();
    JSONRowOutputFormat::flush();
}

void JSONCompactWithProgressRowOutputFormat::writeSuffix()
{
    if (has_progress)
        writeProgress();
}

void JSONCompactWithProgressRowOutputFormat::writeProgress()
{
    std::lock_guard lock(progress_lines_mutex);
    for (const auto & progress_line : progress_lines)
        writeString(progress_line, *ostr);
    progress_lines.clear();
    has_progress = false;
}

void JSONCompactWithProgressRowOutputFormat::finalizeImpl()
{
    if (exception_message.empty())
    {
        JSONUtils::writeCompactAdditionalInfo(
            row_count,
            statistics.rows_before_limit,
            statistics.applied_limit,
            statistics.watch,
            statistics.progress,
            settings.write_statistics,
            *ostr);
    }
    else
    {
        JSONUtils::writeCompactObjectStart(*ostr);
        JSONUtils::writeException(exception_message, *ostr, settings, 0);
        JSONUtils::writeCompactObjectEnd(*ostr);
    }
    writeCString("\n", *ostr);
    ostr->next();
}

void registerOutputFormatJSONCompactWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "JSONCompactWithProgress",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & format_settings)
        { return std::make_shared<JSONCompactWithProgressRowOutputFormat>(buf, sample, format_settings, false); });

    factory.registerOutputFormat(
        "JSONCompactWithProgressStrings",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & format_settings)
        { return std::make_shared<JSONCompactWithProgressRowOutputFormat>(buf, sample, format_settings, true); });
}

}
