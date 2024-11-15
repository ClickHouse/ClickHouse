#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h>
#include <Formats/FormatFactory.h>

namespace DB
{


void JSONEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    progress_lock.lock();

    if (has_progress)
        writeProgress();
    writeCString("{\"row\":{", *ostr);
}

void JSONEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("}}\n", *ostr);
    field_number = 0;

    if (has_progress)
        writeProgress();

    progress_lock.unlock();
}

void JSONEachRowWithProgressRowOutputFormat::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
    has_progress = true;

    if (progress_lock.try_lock())
    {
        writeProgress();
        progress_lock.unlock();
    }
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
    writeCString("{\"progress\":", *ostr);
    progress.writeJSON(*ostr);
    writeCString("}\n", *ostr);
    ostr->next();

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
