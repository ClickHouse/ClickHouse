#include "UserDefinedExecutableFunctionFactory.h"

#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
}

UserDefinedExecutableFunctionFactory & UserDefinedExecutableFunctionFactory::instance()
{
    static UserDefinedExecutableFunctionFactory result;
    return result;
}

void UserDefinedExecutableFunctionFactory::registerFunction(const String & function_name, Creator creator)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

    if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

    std::lock_guard lock(mutex);
    auto [_, inserted] = function_name_to_creator.emplace(function_name, std::move(creator));
    if (!inserted)
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS,
            "The function name '{}' is not unique",
            function_name);
}

void UserDefinedExecutableFunctionFactory::unregisterFunction(const String & function_name)
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_creator.find(function_name);
    if (it == function_name_to_creator.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::get(const String & function_name, ContextPtr context) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_creator.find(function_name);
    if (it == function_name_to_creator.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);

    return it->second(context);
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::tryGet(const String & function_name, ContextPtr context) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_creator.find(function_name);
    if (it == function_name_to_creator.end())
        return nullptr;

    return it->second(context);
}

std::vector<String> UserDefinedExecutableFunctionFactory::getAllRegisteredNames() const
{
    std::vector<std::string> registered_names;

    std::lock_guard lock(mutex);
    registered_names.reserve(function_name_to_creator.size());

    for (const auto & [name, _] : function_name_to_creator)
        registered_names.emplace_back(name);

    return registered_names;
}

}
