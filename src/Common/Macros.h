#pragma once

#include <Core/Types.h>
#include <Core/Names.h>

#include <map>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}


namespace DB
{

/** Apply substitutions from the macros in config to the string.
  */
class Macros
{
public:
    Macros() = default;
    Macros(const Poco::Util::AbstractConfiguration & config, const String & key);

    /** Replace the substring of the form {macro_name} with the value for macro_name, obtained from the config file.
      * If {database} and {table} macros aren`t defined explicitly, expand them as database_name and table_name respectively.
      * level - the level of recursion.
      */
    String expand(const String & s, size_t level = 0, const String & database_name = "", const String & table_name = "") const;

    String expand(const String & s, const String & database_name, const String & table_name) const;


    /** Apply expand for the list.
      */
    Names expand(const Names & source_names, size_t level = 0) const;

    using MacroMap = std::map<String, String>;
    const MacroMap getMacroMap() const { return macros; }

    String getValue(const String & key) const;

private:
    MacroMap macros;
};


}
