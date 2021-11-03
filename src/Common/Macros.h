#pragma once

#include <base/types.h>
#include <Core/Names.h>
#include <Interpreters/StorageID.h>

#include <map>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
    class Logger;
}


namespace DB
{

/** Apply substitutions from the macros in config to the string.
  */
class Macros
{
public:
    Macros() = default;
    Macros(const Poco::Util::AbstractConfiguration & config, const String & key, Poco::Logger * log = nullptr);

    struct MacroExpansionInfo
    {
        /// Settings
        StorageID table_id = StorageID::createEmpty();
        bool ignore_unknown = false;
        bool expand_special_macros_only = false;

        /// Information about macro expansion
        size_t level = 0;
        bool expanded_database = false;
        bool expanded_table = false;
        bool expanded_uuid = false;
        bool expanded_other = false;
        bool has_unknown = false;
    };

    /** Replace the substring of the form {macro_name} with the value for macro_name, obtained from the config file.
      * If {database} and {table} macros aren`t defined explicitly, expand them as database_name and table_name respectively.
      * level - the level of recursion.
      */
    String expand(const String & s,
                  MacroExpansionInfo & info) const;

    String expand(const String & s) const;

    String expand(const String & s, const StorageID & table_id, bool allow_uuid) const;


    /** Apply expand for the list.
      */
    Names expand(const Names & source_names, size_t level = 0) const;

    using MacroMap = std::map<String, String>;
    const MacroMap getMacroMap() const { return macros; }

    String getValue(const String & key) const;

private:
    MacroMap macros;
    bool enable_special_macros = true;
};


}
