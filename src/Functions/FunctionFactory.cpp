#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Common/Exception.h>

#include <Poco/String.h>

#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
    extern const int LOGICAL_ERROR;
}


void FunctionFactory::registerFunction(const
    std::string & name,
    Value creator,
    CaseSensitiveness case_sensitiveness)
{
    if (!functions.emplace(name, creator).second)
        throw Exception("FunctionFactory: the function name '" + name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);

    String function_name_lowercase = Poco::toLower(name);
    if (isAlias(name) || isAlias(function_name_lowercase))
        throw Exception("FunctionFactory: the function name '" + name + "' is already registered as alias",
                        ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_functions.emplace(function_name_lowercase, creator).second)
        throw Exception("FunctionFactory: the case insensitive function name '" + name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}


FunctionOverloadResolverImplPtr FunctionFactory::getImpl(
    const std::string & name,
    const Context & context) const
{
    auto res = tryGetImpl(name, context);
    if (!res)
    {
        auto hints = this->getHints(name);
        if (!hints.empty())
            throw Exception("Unknown function " + name + ". Maybe you meant: " + toString(hints),
                            ErrorCodes::UNKNOWN_FUNCTION);
        else
            throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
    }
    return res;
}

std::vector<std::string> FunctionFactory::getAllNames() const
{
    std::vector<std::string> res;
    res.reserve(functions.size());
    for (const auto & func : functions)
        res.emplace_back(func.first);
    return res;
}

FunctionOverloadResolverPtr FunctionFactory::get(
    const std::string & name,
    const Context & context) const
{
    return std::make_shared<FunctionOverloadResolverAdaptor>(getImpl(name, context));
}

FunctionOverloadResolverImplPtr FunctionFactory::tryGetImpl(
    const std::string & name_param,
    const Context & context) const
{
    String name = getAliasToOrName(name_param);

    auto it = functions.find(name);
    if (functions.end() != it)
        return it->second(context);

    it = case_insensitive_functions.find(Poco::toLower(name));
    if (case_insensitive_functions.end() != it)
        return it->second(context);

    return {};
}

FunctionOverloadResolverPtr FunctionFactory::tryGet(
        const std::string & name,
        const Context & context) const
{
    auto impl = tryGetImpl(name, context);
    return impl ? std::make_shared<FunctionOverloadResolverAdaptor>(std::move(impl))
                : nullptr;
}

FunctionFactory & FunctionFactory::instance()
{
    static FunctionFactory ret;
    return ret;
}

}
