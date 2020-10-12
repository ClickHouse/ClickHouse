#include <TableFunctions/TableFunctionFactory.h>

#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
    extern const int LOGICAL_ERROR;
}


void TableFunctionFactory::registerFunction(const std::string & name, Value creator, CaseSensitiveness case_sensitiveness)
{
    if (!table_functions.emplace(name, creator).second)
        throw Exception("TableFunctionFactory: the table function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_table_functions.emplace(Poco::toLower(name), creator).second)
        throw Exception("TableFunctionFactory: the case insensitive table function name '" + name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

TableFunctionPtr TableFunctionFactory::get(
    const std::string & name,
    const Context & context) const
{
    auto res = tryGet(name, context);
    if (!res)
    {
        auto hints = getHints(name);
        if (!hints.empty())
            throw Exception("Unknown table function " + name + ". Maybe you meant: " + toString(hints), ErrorCodes::UNKNOWN_FUNCTION);
        else
            throw Exception("Unknown table function " + name, ErrorCodes::UNKNOWN_FUNCTION);
    }

    return res;
}

TableFunctionPtr TableFunctionFactory::tryGet(
        const std::string & name_param,
        const Context &) const
{
    String name = getAliasToOrName(name_param);

    auto it = table_functions.find(name);
    if (table_functions.end() != it)
        return it->second();

    it = case_insensitive_table_functions.find(Poco::toLower(name));
    if (case_insensitive_table_functions.end() != it)
        return it->second();

    return {};
}

bool TableFunctionFactory::isTableFunctionName(const std::string & name) const
{
    return table_functions.count(name);
}

TableFunctionFactory & TableFunctionFactory::instance()
{
    static TableFunctionFactory ret;
    return ret;
}

}
