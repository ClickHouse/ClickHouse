#include <Common/JSONBuilder.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteHelpers.h>
#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>

namespace DB::JSONBuilder
{

std::string toCamelCaseKey(std::string_view key)
{
    std::string result;
    result.reserve(key.size());

    size_t pos = 0;
    while (pos < key.size())
    {
        if (!isAlphaNumericASCII(key[pos]))
        {
            ++pos;
            continue;
        }

        size_t end = pos;
        bool has_lower = false;
        while (end < key.size() && isAlphaNumericASCII(key[end]))
        {
            has_lower |= isLowerAlphaASCII(key[end]);
            ++end;
        }

        /// The first letter of the word decides the case; the rest is kept, unless the word is in capitals only.
        const bool first_word = result.empty();
        result += first_word ? toLowerIfAlphaASCII(key[pos]) : toUpperIfAlphaASCII(key[pos]);
        for (size_t i = pos + 1; i < end; ++i)
            result += has_lower ? key[i] : toLowerIfAlphaASCII(key[i]);
        pos = end;
    }
    return result;
}

static bool isArrayOrMap(const IItem & item)
{
    return typeid_cast<const JSONArray *>(&item) || typeid_cast<const JSONMap *>(&item);
}

static bool isSimpleArray(const std::vector<ItemPtr> & values)
{
    for (const auto & value : values)
        if (isArrayOrMap(*value))
            return false;

    return true;
}

void JSONString::format(const FormatSettings & settings, FormatContext & context)
{
    writeJSONString(value, context.out, settings.settings);
}

void JSONBool::format(const FormatSettings &, FormatContext & context)
{
    writeString(value ? "true" : "false", context.out);
}

void JSONArray::format(const FormatSettings & settings, FormatContext & context)
{
    writeChar('[', context.out);

    context.offset += settings.indent;

    bool single_row = settings.solid || (settings.print_simple_arrays_in_single_row && isSimpleArray(values));
    bool first = true;

    for (const auto & value : values)
    {
        if (!first)
            writeChar(',', context.out);

        if (!single_row)
        {
            writeChar('\n', context.out);
            writeChar(' ', context.offset, context.out);
        }
        else if (!first && !settings.solid)
            writeChar(' ', context.out);

        first = false;
        value->format(settings, context);
    }

    context.offset -= settings.indent;

    if (!single_row)
    {
        writeChar('\n', context.out);
        writeChar(' ', context.offset, context.out);
    }

    writeChar(']', context.out);
}

void JSONMap::format(const FormatSettings & settings, FormatContext & context)
{
    writeChar('{', context.out);

    context.offset += settings.indent;

    bool first = true;

    for (const auto & value : values)
    {
        if (!first)
            writeChar(',', context.out);
        first = false;

        if (!settings.solid)
        {
            writeChar('\n', context.out);
            writeChar(' ', context.offset, context.out);
        }
        if (settings.camel_case_keys)
            writeJSONString(toCamelCaseKey(value.key), context.out, settings.settings);
        else
            writeJSONString(value.key, context.out, settings.settings);

        writeChar(':', context.out);
        if (!settings.solid)
            writeChar(' ', context.out);

        value.value->format(settings, context);
    }

    context.offset -= settings.indent;

    if (!settings.solid)
    {
        writeChar('\n', context.out);
        writeChar(' ', context.offset, context.out);
    }
    writeChar('}', context.out);
}

void JSONNull::format(const FormatSettings &, FormatContext & context)
{
    writeString("null", context.out);
}

}
