#include "UserDefinedFunctionFactory.h"

#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
    extern const int CANNOT_DROP_SYSTEM_FUNCTION;
}

UserDefinedFunctionFactory & UserDefinedFunctionFactory::instance()
{
    static UserDefinedFunctionFactory result;
    return result;
}

void UserDefinedFunctionFactory::registerFunction(const String & function_name, ASTPtr create_function_query)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

    if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

    std::lock_guard lock(mutex);

    auto [_, inserted] = function_name_to_create_query.emplace(function_name, std::move(create_function_query));
    if (!inserted)
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS,
            "The function name '{}' is not unique",
            function_name);
}

void UserDefinedFunctionFactory::unregisterFunction(const String & function_name)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name) ||
        AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_SYSTEM_FUNCTION, "Cannot drop system function '{}'", function_name);

    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);

    function_name_to_create_query.erase(it);
}

ASTPtr UserDefinedFunctionFactory::get(const String & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);

    return it->second;
}

ASTPtr UserDefinedFunctionFactory::tryGet(const std::string & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        return nullptr;

    return it->second;
}

std::vector<std::string> UserDefinedFunctionFactory::getAllRegisteredNames() const
{
    std::vector<std::string> registered_names;

    std::lock_guard lock(mutex);
    registered_names.reserve(function_name_to_create_query.size());

    for (const auto & [name, _] : function_name_to_create_query)
        registered_names.emplace_back(name);

    return registered_names;
}

}
