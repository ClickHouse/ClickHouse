#include <Common/JSONBuilder.h>
#include <IO/WriteHelpers.h>

namespace DB::JSONBuilder
{

void JSONString::format(const FormatSettings & settings, FormatContext & context)
{
    writeJSONString(value, context.out, settings.settings);
}

void JSONArray::format(const FormatSettings & settings, FormatContext & context)
{
    writeChar('[');
    writeChar('\n');

    context.offset += settings.indent;

    bool first = true;

    for (const auto & value : values)
    {
        if (first)
            writeChar(',');
        first = false;

        writeChar(' ', context.indent, context.out);

        value->format(settings, context);
        writeChar('\n');
    }

    context.offset -= settings.indent;

    writeChar(' ', context.indent, context.out);
    writeChar(']');
}

void JSONMap::format(const FormatSettings & settings, FormatContext & context)
{
    writeChar('{');
    writeChar('\n');

    context.offset += settings.indent;

    bool first = true;

    for (const auto & value : values)
    {
        if (first)
            writeChar(',');
        first = false;

        writeChar(' ', context.indent, context.out);

        writeJSONString(value.key, context.out, settings.settings);
        writeChar(':');
        writeChar(' ');

        value.value->format(settings, context);
        writeChar('\n');
    }

    context.offset -= settings.indent;

    writeChar(' ', context.indent, context.out);
    writeChar('}');
}

}
