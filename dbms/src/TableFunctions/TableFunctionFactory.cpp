#include <Common/Exception.h>
#include <Interpreters/Context.h>

#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int READONLY;
    extern const int UNKNOWN_FUNCTION;
}


TableFunctionPtr TableFunctionFactory::get(
    const std::string & name,
    const Context & context) const
{
    if (context.getSettings().limits.readonly == 1)        /** For example, for readonly = 2 - allowed. */
        throw Exception("Table functions are forbidden in readonly mode", ErrorCodes::READONLY);

    auto it = functions.find(name);
    if (it == functions.end())
        throw Exception("Unknown table function " + name, ErrorCodes::UNKNOWN_FUNCTION);
    return it->second();
}

}
