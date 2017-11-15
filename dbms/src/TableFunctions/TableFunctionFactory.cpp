#include <TableFunctions/TableFunctionFactory.h>

#include <Interpreters/Context.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int READONLY;
    extern const int UNKNOWN_FUNCTION;
    extern const int LOGICAL_ERROR;
}


void TableFunctionFactory::registerFunction(const std::string & name, Creator creator)
{
    if (!functions.emplace(name, std::move(creator)).second)
        throw Exception("TableFunctionFactory: the table function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
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
