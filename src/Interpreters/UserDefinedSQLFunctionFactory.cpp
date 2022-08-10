#include "UserDefinedSQLFunctionFactory.h"

#include <Common/quoteString.h>

#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/UserDefinedExecutableFunctionFactory.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
    extern const int CANNOT_DROP_FUNCTION;
}

UserDefinedSQLFunctionFactory & UserDefinedSQLFunctionFactory::instance()
{
    static UserDefinedSQLFunctionFactory result;
    return result;
}

void UserDefinedSQLFunctionFactory::registerFunction(ContextPtr context, const String & function_name, ASTPtr create_function_query, bool replace, bool if_not_exists, bool persist)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name))
    {
        if (if_not_exists)
            return;

        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);
    }

    if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
    {
        if (if_not_exists)
            return;

        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);
    }

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context))
    {
        if (if_not_exists)
            return;

        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined executable function '{}' already exists", function_name);
    }

    std::lock_guard lock(mutex);

    auto [it, inserted] = function_name_to_create_query.emplace(function_name, create_function_query);

    if (!inserted)
    {
        if (if_not_exists)
            return;

        if (replace)
            it->second = create_function_query;
        else
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS,
                "The function name '{}' is not unique",
                function_name);
    }

    if (persist)
    {
        try
        {
            UserDefinedSQLObjectsLoader::instance().storeObject(context, UserDefinedSQLObjectType::Function, function_name, *create_function_query, replace);
        }
        catch (Exception & exception)
        {
            function_name_to_create_query.erase(it);
            exception.addMessage(fmt::format("while storing user defined function {} on disk", backQuote(function_name)));
            throw;
        }
    }
}

void UserDefinedSQLFunctionFactory::unregisterFunction(ContextPtr context, const String & function_name, bool if_exists)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name) ||
        AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined executable function '{}'", function_name);

    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
    {
        if (if_exists)
            return;

        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);
    }

    try
    {
        UserDefinedSQLObjectsLoader::instance().removeObject(context, UserDefinedSQLObjectType::Function, function_name);
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while removing user defined function {} from disk", backQuote(function_name)));
        throw;
    }

    function_name_to_create_query.erase(it);
}

ASTPtr UserDefinedSQLFunctionFactory::get(const String & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);

    return it->second;
}

ASTPtr UserDefinedSQLFunctionFactory::tryGet(const std::string & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        return nullptr;

    return it->second;
}

bool UserDefinedSQLFunctionFactory::has(const String & function_name) const
{
    return tryGet(function_name) != nullptr;
}

std::vector<std::string> UserDefinedSQLFunctionFactory::getAllRegisteredNames() const
{
    std::vector<std::string> registered_names;

    std::lock_guard lock(mutex);
    registered_names.reserve(function_name_to_create_query.size());

    for (const auto & [name, _] : function_name_to_create_query)
        registered_names.emplace_back(name);

    return registered_names;
}

bool UserDefinedSQLFunctionFactory::empty() const
{
    std::lock_guard lock(mutex);
    return function_name_to_create_query.size() == 0;
}
}
