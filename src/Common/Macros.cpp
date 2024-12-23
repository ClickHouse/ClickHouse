#include <algorithm>
#include <unordered_map>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Macros.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/ServerUUID.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

Macros::Macros(const Poco::Util::AbstractConfiguration & config, const String & root_key, Poco::Logger * log)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(root_key, keys);
    for (const String & key : keys)
    {
        macros[key] = config.getString(root_key + "." + key);
        if (key == "database" || key == "table" || key == "uuid")
        {
            if (log)
                LOG_WARNING(log,
                    "Config file contains '{}' macro. This macro has special meaning "
                    "and it's explicit definition is not recommended. Implicit unfolding for "
                    "'database', 'table' and 'uuid' macros will be disabled.",
                    key);
            enable_special_macros = false;
        }
    }
}

Macros::Macros(const Poco::Util::AbstractConfiguration & config, const String & root_key, LoggerPtr log)
    : Macros(config, root_key, log.get())
{}

Macros::Macros(std::map<String, String> map)
{
    macros = std::move(map);
}

String Macros::expand(const String & s,
                      MacroExpansionInfo & info) const
{
    /// Do not allow recursion if we expand only special macros, because it will be infinite recursion
    assert(info.level == 0 || !info.expand_special_macros_only);

    if (s.find('{') == String::npos)
        return s;

    if (info.level && s.size() > 65536)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Too long string while expanding macros");

    if (info.level >= 10)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Too deep recursion while expanding macros: '{}'", s);

    /// If config file contains explicit special macro, then we do not expand it in this mode.
    if (!enable_special_macros && info.expand_special_macros_only)
        return s;

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

        res.append(s, pos, begin - pos);


        ++begin;
        size_t end = s.find('}', begin);
        if (end == String::npos)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Unbalanced {{ and }} in string with macros: '{}'", s);

        String macro_name = s.substr(begin, end - begin);
        auto it = macros.find(macro_name);

        /// Prefer explicit macros over implicit.
        if (it != macros.end() && !info.expand_special_macros_only)
        {
            res += it->second;
            info.expanded_other = true;
        }
        else if (macro_name == "database" && !info.table_id.database_name.empty())
        {
            res += info.table_id.database_name;
            info.expanded_database = true;
        }
        else if (macro_name == "table" && !info.table_id.table_name.empty())
        {
            res += info.table_id.table_name;
            info.expanded_table = true;
        }
        else if (macro_name == "uuid" && !info.expand_special_macros_only)
        {
            if (info.table_id.uuid == UUIDHelpers::Nil)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Macro 'uuid' and empty arguments of ReplicatedMergeTree "
                                "are supported only for ON CLUSTER queries with Atomic database engine");
            /// For ON CLUSTER queries we don't want to require all macros definitions in initiator's config.
            /// However, initiator must check that for cross-replication cluster zookeeper_path does not contain {uuid} macro.
            /// It becomes impossible to check if {uuid} is contained inside some unknown macro.
            if (info.level)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Macro 'uuid' should not be inside another macro");
            res += toString(info.table_id.uuid);
            info.expanded_uuid = true;
        }
        else if (macro_name == "server_uuid")
        {
            auto uuid = ServerUUID::get();
            if (UUIDHelpers::Nil == uuid)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Macro {{server_uuid}} expanded to zero, which means the UUID is not initialized (most likely it's not a server application)");
            res += toString(uuid);
            info.expanded_other = true;
        }
        else if (info.shard && macro_name == "shard")
        {
            res += *info.shard;
            info.expanded_other = true;
        }
        else if (info.replica && macro_name == "replica")
        {
            res += *info.replica;
            info.expanded_other = true;
        }
        else if (info.ignore_unknown || info.expand_special_macros_only)
        {
            if (info.expand_special_macros_only)
                res += '{';
            res += macro_name;
            if (info.expand_special_macros_only)
                res += '}';
            info.has_unknown = true;
        }
        else
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No macro '{}' in config while processing substitutions in "
                            "'{}' at '{}' or macro is not supported here", macro_name, s, toString(begin));

        pos = end + 1;
    }

    ++info.level;
    if (info.expand_special_macros_only)
        return res;

    return expand(res, info);
}

String Macros::getValue(const String & key) const
{
    if (auto it = macros.find(key); it != macros.end())
        return it->second;
    throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No macro {} in config", key);
}


String Macros::expand(const String & s) const
{
    MacroExpansionInfo info;
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
