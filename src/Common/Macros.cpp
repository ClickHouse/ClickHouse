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

String Macros::expand(const String & s,
                      MacroExpansionInfo & info) const
{
    if (s.find('{') == String::npos)
        return s;

    if (info.level && s.size() > 65536)
        throw Exception("Too long string while expanding macros", ErrorCodes::SYNTAX_ERROR);

    if (info.level >= 10)
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
        else if (macro_name == "database" && !info.database_name.empty())
            res += info.database_name;
        else if (macro_name == "table" && !info.table_name.empty())
            res += info.table_name;
        else if (macro_name == "uuid")
        {
            if (info.uuid == UUIDHelpers::Nil)
                throw Exception("Macro 'uuid' and empty arguments of ReplicatedMergeTree "
                                "are supported only for ON CLUSTER queries with Atomic database engine",
                                ErrorCodes::SYNTAX_ERROR);
            /// For ON CLUSTER queries we don't want to require all macros definitions in initiator's config.
            /// However, initiator must check that for cross-replication cluster zookeeper_path does not contain {uuid} macro.
            /// It becomes impossible to check if {uuid} is contained inside some unknown macro.
            if (info.level)
                throw Exception("Macro 'uuid' should not be inside another macro", ErrorCodes::SYNTAX_ERROR);
            res += toString(info.uuid);
            info.expanded_uuid = true;
        }
        else if (info.ignore_unknown)
        {
            res += macro_name;
            info.has_unknown = true;
        }
        else
            throw Exception("No macro '" + macro_name +
                "' in config while processing substitutions in '" + s + "' at '"
                + toString(begin) + "' or macro is not supported here", ErrorCodes::SYNTAX_ERROR);

        pos = end + 1;
    }

    ++info.level;
    return expand(res, info);
}

String Macros::getValue(const String & key) const
{
    if (auto it = macros.find(key); it != macros.end())
        return it->second;
    throw Exception("No macro " + key + " in config", ErrorCodes::SYNTAX_ERROR);
}


String Macros::expand(const String & s) const
{
    MacroExpansionInfo info;
    return expand(s, info);
}

String Macros::expand(const String & s, const StorageID & table_id, bool allow_uuid) const
{
    MacroExpansionInfo info;
    info.database_name = table_id.database_name;
    info.table_name = table_id.table_name;
    info.uuid = allow_uuid ? table_id.uuid : UUIDHelpers::Nil;
    return expand(s, info);
}

Names Macros::expand(const Names & source_names, size_t level) const
{
    Names result_names;
    result_names.reserve(source_names.size());

    MacroExpansionInfo info;
    for (const String & name : source_names)
    {
        info.level = level;
        result_names.push_back(expand(name, info));
    }

    return result_names;
}
}
