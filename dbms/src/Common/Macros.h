#pragma once

#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <map>


namespace DB
{

/** Apply substitutions from the macros in config to the string.
  */
class Macros
{
public:
    Macros();
    Macros(const Poco::Util::AbstractConfiguration & config, const String & key);

    /** Replace the substring of the form {macro_name} with the value for macro_name, obtained from the config file.
      * level - the level of recursion.
      */
    String expand(const String & s, size_t level = 0) const;

private:
    using MacroMap = std::map<String, String>;

    MacroMap macros;
};

}
