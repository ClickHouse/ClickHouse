#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Macros.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

Macros::Macros() {}

Macros::Macros(const Poco::Util::AbstractConfiguration & config, const String & root_key)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(root_key, keys);
    for (const String & key : keys)
    {
        macros[key] = config.getString(root_key + "." + key);
    }
}

String Macros::expand(const String & s, size_t level) const
{
    if (s.find('{') == String::npos)
        return s;

    if (level && s.size() > 65536)
        throw Exception("Too long string while expanding macros", ErrorCodes::SYNTAX_ERROR);

    if (level >= 10)
        throw Exception("Too deep recursion while expanding macros: '" + s + "'", ErrorCodes::SYNTAX_ERROR);

    String res;
    size_t pos = 0;
    while (true)
    {
        size_t begin = s.find('{', pos);

        if (begin == String::npos)
        {
            res.append(s, pos, String::npos);
            break;
        }
        else
        {
            res.append(s, pos, begin - pos);
        }

        ++begin;
        size_t end = s.find('}', begin);
        if (end == String::npos)
            throw Exception("Unbalanced { and } in string with macros: '" + s + "'", ErrorCodes::SYNTAX_ERROR);

        String macro_name = s.substr(begin, end - begin);

        auto it = macros.find(macro_name);
        if (it == macros.end())
            throw Exception("No macro " + macro_name + " in config", ErrorCodes::SYNTAX_ERROR);

        res += it->second;

        pos = end + 1;
    }

    return expand(res, level + 1);
}

Names Macros::expand(const Names & source_names, size_t level) const
{
    Names result_names;
    result_names.reserve(source_names.size());

    for (const String & name : source_names)
        result_names.push_back(expand(name, level));

    return result_names;
}
}
