#include <Processors/Formats/Impl/JSONCompactWithProgressRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>

#include <IO/WriteHelpers.h>

#include <Common/logger_useful.h>



namespace DB
{

JSONCompactWithProgressRowOutputFormat::JSONCompactWithProgressRowOutputFormat(
    WriteBuffer & out_,
    const Block & header,
    const FormatSettings & settings_,
    bool yield_strings_)
    : JSONRowOutputFormat(out_, header, settings_, yield_strings_)
{
}

void JSONCompactWithProgressRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    JSONUtils::writeFieldFromColumn(column, serialization, row_num, yield_strings, settings, *ostr);
    ++field_number;
    LOG_DEBUG(getLogger("JSONCompactWithProgressRowOutputFormat"), "Field number: {}", field_number);
}

void JSONCompactWithProgressRowOutputFormat::writeFieldDelimiter()
{
    JSONUtils::writeFieldCompactDelimiter(*ostr);
}

void JSONCompactWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    if (has_progress)
        writeProgress();
    JSONUtils::writeCompactArrayStart(*ostr, 2);
}

void JSONCompactWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    JSONUtils::writeCompactArrayEnd(*ostr);
    field_number = 0;
    ++row_count;
}

void JSONCompactWithProgressRowOutputFormat::writeBeforeTotals()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeCompactArrayStart(*ostr, 1, "totals");
}

void JSONCompactWithProgressRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    JSONUtils::writeCompactColumns(columns, serializations, row_num, yield_strings, settings, *ostr);
}

void JSONCompactWithProgressRowOutputFormat::writeAfterTotals()
{
    JSONUtils::writeCompactArrayEnd(*ostr);
}

void JSONCompactWithProgressRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    JSONUtils::writeCompactArrayStart(*ostr, 2, title);
    JSONUtils::writeCompactColumns(columns, serializations, row_num, yield_strings, settings, *ostr);
    JSONUtils::writeCompactArrayEnd(*ostr);
}

void JSONCompactWithProgressRowOutputFormat::onProgress(const Progress & value)
{
    LOG_DEBUG(getLogger("JSONCompactWithProgressRowOutputFormat"), "onProgress: {}", value.read_rows);

    progress.incrementPiecewiseAtomically(value);
    String progress_line;
    WriteBufferFromString buf(progress_line);
    writeCString("{\"progress\":", buf);
    progress.writeJSON(buf);
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
    JSONRowOutputFormat::writeSuffix();
}

void JSONCompactWithProgressRowOutputFormat::writeProgress()
{
    std::lock_guard lock(progress_lines_mutex);
    for (const auto & progress_line : progress_lines)
        writeString(progress_line,  *ostr);
    progress_lines.clear();
    has_progress = false;
}

void registerOutputFormatJSONCompactWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONCompactWithProgress", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONCompactWithProgressRowOutputFormat>(buf, sample, format_settings, false);
    });

    factory.markOutputFormatSupportsParallelFormatting("JSONCompactWithProgress");
}

}
