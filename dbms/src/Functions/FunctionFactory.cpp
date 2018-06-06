#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Common/Exception.h>

#include <Poco/String.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
    extern const int LOGICAL_ERROR;
}


void FunctionFactory::registerFunction(const
    std::string & name,
    Creator creator,
    CaseSensitiveness case_sensitiveness)
{
    if (!functions.emplace(name, creator).second)
        throw Exception("FunctionFactory: the function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_functions.emplace(Poco::toLower(name), creator).second)
        throw Exception("FunctionFactory: the case insensitive function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}


FunctionBuilderPtr FunctionFactory::get(
    const std::string & name,
    const Context & context) const
{
    auto res = tryGet(name, context);
    if (!res)
        throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
    return res;
}


FunctionBuilderPtr FunctionFactory::tryGet(
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
