#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Macros.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

Macros::Macros(const Poco::Util::AbstractConfiguration & config, const String & root_key)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(root_key, keys);
    for (const String & key : keys)
    {
        macros[key] = config.getString(root_key + "." + key);
    }
}

String Macros::expand(const String & s, size_t level, const String & database_name, const String & table_name) const
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

        /// Prefer explicit macros over implicit.
        if (it != macros.end())
            res += it->second;
        else if (macro_name == "database" && !database_name.empty())
            res += database_name;
        else if (macro_name == "table" && !table_name.empty())
            res += table_name;
        else
            throw Exception("No macro '" + macro_name +
                "' in config while processing substitutions in '" + s + "' at "
                + toString(begin), ErrorCodes::SYNTAX_ERROR);

        pos = end + 1;
    }

    return expand(res, level + 1, database_name, table_name);
}

String Macros::getValue(const String & key) const
{
    if (auto it = macros.find(key); it != macros.end())
        return it->second;
    throw Exception("No macro " + key + " in config", ErrorCodes::SYNTAX_ERROR);
}

String Macros::expand(const String & s, const String & database_name, const String & table_name) const
{
    return expand(s, 0, database_name, table_name);
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
