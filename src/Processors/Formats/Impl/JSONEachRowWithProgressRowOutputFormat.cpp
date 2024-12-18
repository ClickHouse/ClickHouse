#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h>
#include <Formats/FormatFactory.h>

namespace DB
{


void JSONEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    if (has_progress)
        writeProgress();
    writeCString("{\"row\":{", *ostr);
}

void JSONEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("}}\n", *ostr);
    field_number = 0;
}

void JSONEachRowWithProgressRowOutputFormat::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
    String progress_line;
    {
        WriteBufferFromString buf(progress_line);
        writeCString("{\"progress\":", buf);
        progress.writeJSON(buf);
        writeCString("}\n", buf);
    }

    std::lock_guard lock(progress_lines_mutex);
    progress_lines.emplace_back(std::move(progress_line));
    has_progress = true;
}

void JSONEachRowWithProgressRowOutputFormat::flush()
{
    if (has_progress)
        writeProgress();
    JSONEachRowRowOutputFormat::flush();
}

void JSONEachRowWithProgressRowOutputFormat::writeSuffix()
{
    if (has_progress)
        writeProgress();
    JSONEachRowRowOutputFormat::writeSuffix();
}

void JSONEachRowWithProgressRowOutputFormat::writeProgress()
{
    std::lock_guard lock(progress_lines_mutex);
    for (const auto & progress_line : progress_lines)
        writeString(progress_line,  *ostr);
    progress_lines.clear();
    has_progress = false;
}

void registerOutputFormatJSONEachRowWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buf, sample, settings);
    });

    factory.registerOutputFormat("JSONStringsEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = true;
        return std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buf, sample, settings);
    });
}

}
