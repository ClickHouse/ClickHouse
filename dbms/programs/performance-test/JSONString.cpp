#include "JSONString.h"

#include <regex>
#include <sstream>
namespace DB
{

namespace
{
std::string pad(size_t padding)
{
    return std::string(padding * 4, ' ');
}

const std::regex NEW_LINE{"\n"};
}

void JSONString::set(const std::string & key, std::string value, bool wrap)
{
    if (value.empty())
        value = "null";

    bool reserved = (value[0] == '[' || value[0] == '{' || value == "null");
    if (!reserved && wrap)
        value = '"' + std::regex_replace(value, NEW_LINE, "\\n") + '"';

    content[key] = value;
}

void JSONString::set(const std::string & key, const std::vector<std::vector<JSONString>> & run_infos)
{
    std::ostringstream value;
    value << "[\n";

    for (size_t i = 0; i < run_infos.size(); ++i)
    {
        if (run_infos[i].empty())
            continue;

        value << pad(padding + 1) << "[\n";
        for (size_t j = 0; j < run_infos[i].size(); ++j)
        {
            value << pad(padding + 2) + run_infos[i][j].asString(padding + 3);
            if (j != run_infos[i].size() - 1)
                value << ',';

            value << "\n";
        }
        value << pad(padding + 1) << ']';
        if (i != run_infos.size() - 1)
            value << ',';

        value << "\n";
    }

    value << pad(padding) << ']';
    content[key] = value.str();
}

std::string JSONString::asString(size_t cur_padding) const
{
    std::ostringstream repr;
    repr << "{";

    for (auto it = content.begin(); it != content.end(); ++it)
    {
        if (it != content.begin())
            repr << ',';
        /// construct "key": "value" string with padding
        repr << "\n" << pad(cur_padding) << '"' << it->first << '"' << ": " << it->second;
    }

    repr << "\n" << pad(cur_padding - 1) << '}';
    return repr.str();
}


}
