#include <Poco/String.h>
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
}


void FunctionFactory::registerFunction(const String & name, Creator creator, CaseSensitiveness case_sensitiveness)
{
    if (!functions.emplace(name, creator).second)
        throw Exception("FunctionFactory: the function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_functions.emplace(Poco::toLower(name), creator).second)
        throw Exception("FunctionFactory: the case insensitive function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}


FunctionPtr FunctionFactory::get(
    const std::string & name,
    const Context & context) const
{
    auto res = tryGet(name, context);
    if (!res)
        throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
    return res;
}


FunctionPtr FunctionFactory::tryGet(
    const std::string & name,
    const Context & context) const
{
    auto it = functions.find(name);
    if (functions.end() != it)
        return it->second(context);

    it = case_insensitive_functions.find(Poco::toLower(name));
    if (case_insensitive_functions.end() != it)
        return it->second(context);

    return {};
}

}
