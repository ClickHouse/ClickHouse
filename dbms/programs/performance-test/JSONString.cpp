#include "JSONString.h"

#include <regex>
namespace DB
{

namespace
{
String pad(size_t padding)
{
    return String(padding * 4, ' ');
}

const std::regex NEW_LINE{"\n"};
}

void JSONString::set(const String key, String value, bool wrap)
{
    if (value.empty())
        value = "null";

    bool reserved = (value[0] == '[' || value[0] == '{' || value == "null");
    if (!reserved && wrap)
        value = '"' + std::regex_replace(value, NEW_LINE, "\\n") + '"';

    content[key] = value;
}

void JSONString::set(const String key, const std::vector<JSONString> & run_infos)
{
    String value = "[\n";

    for (size_t i = 0; i < run_infos.size(); ++i)
    {
        value += pad(padding + 1) + run_infos[i].asString(padding + 2);
        if (i != run_infos.size() - 1)
            value += ',';

        value += "\n";
    }

    value += pad(padding) + ']';
    content[key] = value;
}

String JSONString::asString(size_t cur_padding) const
{
    String repr = "{";

    for (auto it = content.begin(); it != content.end(); ++it)
    {
        if (it != content.begin())
            repr += ',';
        /// construct "key": "value" string with padding
        repr += "\n" + pad(cur_padding) + '"' + it->first + '"' + ": " + it->second;
    }

    repr += "\n" + pad(cur_padding - 1) + '}';
    return repr;
}


}
