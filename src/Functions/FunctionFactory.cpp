#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Core/Settings.h>

#include <Poco/String.h>

#include <IO/WriteHelpers.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool log_queries;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
    extern const int LOGICAL_ERROR;
}

const String & getFunctionCanonicalNameIfAny(const String & name)
{
    return FunctionFactory::instance().getCanonicalNameIfAny(name);
}

void FunctionFactory::registerFunction(
    const std::string & name,
    FunctionCreator creator,
    FunctionDocumentation doc,
    Case case_sensitiveness)
{
    if (!functions.emplace(name, FunctionFactoryData{creator, doc}).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionFactory: the function name '{}' is not unique", name);

    String function_name_lowercase = Poco::toLower(name);
    if (isAlias(name) || isAlias(function_name_lowercase))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionFactory: the function name '{}' is already registered as alias",
                        name);

    if (case_sensitiveness == Case::Insensitive)
    {
        if (!case_insensitive_functions.emplace(function_name_lowercase, FunctionFactoryData{creator, doc}).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionFactory: the case insensitive function name '{}' is not unique",
                name);
        case_insensitive_name_mapping[function_name_lowercase] = name;
    }
}

void FunctionFactory::registerFunction(
    const std::string & name,
    FunctionSimpleCreator creator,
    FunctionDocumentation doc,
    Case case_sensitiveness)
{
    registerFunction(name, [my_creator = std::move(creator)](ContextPtr context)
    {
        return std::make_unique<FunctionToOverloadResolverAdaptor>(my_creator(context));
    }, std::move(doc), std::move(case_sensitiveness));
}


FunctionOverloadResolverPtr FunctionFactory::getImpl(
    const std::string & name,
    ContextPtr context) const
{
    auto res = tryGetImpl(name, context);
    if (!res)
    {
        String extra_info;
        if (AggregateFunctionFactory::instance().hasNameOrAlias(name))
            extra_info = ". There is an aggregate function with the same name, but ordinary function is expected here";

        auto hints = this->getHints(name);
        if (!hints.empty())
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown function {}{}. Maybe you meant: {}", name, extra_info, toString(hints));
        else
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown function {}{}", name, extra_info);
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
    ContextPtr context) const
{
    return getImpl(name, context);
}

bool FunctionFactory::has(const std::string & name) const
{
    String canonical_name = getAliasToOrName(name);
    if (functions.contains(canonical_name))
        return true;
    canonical_name = Poco::toLower(canonical_name);
    return case_insensitive_functions.contains(canonical_name);
}

FunctionOverloadResolverPtr FunctionFactory::tryGetImpl(
    const std::string & name_param,
    ContextPtr context) const
{
    String name = getAliasToOrName(name_param);
    FunctionOverloadResolverPtr res;

    auto it = functions.find(name);
    if (functions.end() != it)
        res = it->second.first(context);
    else
    {
        name = Poco::toLower(name);
        it = case_insensitive_functions.find(name);
        if (case_insensitive_functions.end() != it)
            res = it->second.first(context);
    }

    if (!res)
        return nullptr;

    if (CurrentThread::isInitialized())
    {
        auto query_context = CurrentThread::get().getQueryContext();
        if (query_context && query_context->getSettingsRef()[Setting::log_queries])
            query_context->addQueryFactoriesInfo(Context::QueryLogFactories::Function, name);
    }

    return res;
}

FunctionOverloadResolverPtr FunctionFactory::tryGet(
    const std::string & name,
    ContextPtr context) const
{
    auto impl = tryGetImpl(name, context);
    return impl ? std::move(impl) : nullptr;
}

FunctionFactory & FunctionFactory::instance()
{
    static FunctionFactory ret;
    return ret;
}

FunctionDocumentation FunctionFactory::getDocumentation(const std::string & name) const
{
    auto it = functions.find(name);
    if (it == functions.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown function {}", name);

    return it->second.second;
}

}
