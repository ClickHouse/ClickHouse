#include "UserDefinedSQLFunctionFactory.h"

#include <Interpreters/UserDefinedSQLObjectsLoader.h>
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

UserDefinedSQLFunctionFactory::Lock UserDefinedSQLFunctionFactory::getLock() const
{
    return Lock(mutex);
}

void UserDefinedSQLFunctionFactory::registerFunction(
    Lock &,
    ContextPtr context,
    const String & function_name,
    ASTPtr create_function_query,
    bool replace,
    bool persist)
{
    FunctionCreateQuery create_query { create_function_query, persist };

    auto [it, inserted] = function_name_to_create_query.emplace(function_name, create_query);

    if (!inserted)
    {
        if (replace)
            it->second = create_query;
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

void UserDefinedSQLFunctionFactory::registerFunction(
    ContextPtr context,
    const String & function_name,
    ASTPtr create_function_query,
    bool replace,
    bool persist)
{
    auto lock = getLock();
    registerFunction(lock, std::move(context), function_name, create_function_query, replace, persist);
}

void UserDefinedSQLFunctionFactory::unregisterFunction(Lock &, ContextPtr context, const String & function_name)
{
    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
    {
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION,
            "The function name '{}' is not registered",
            function_name);
    }

    if (it->second.persisted)
    {
        try
        {
            UserDefinedSQLObjectsLoader::instance().removeObject(context, UserDefinedSQLObjectType::Function, function_name);
        }
        catch (Exception & exception)
        {
            exception.addMessage(fmt::format("while removing user defined function {} from disk", backQuote(function_name)));
            throw;
        }
    }

    function_name_to_create_query.erase(it);
}

void UserDefinedSQLFunctionFactory::unregisterFunction(ContextPtr context, const String & function_name)
{
    auto lock = getLock();
    unregisterFunction(lock, std::move(context), function_name);
}

ASTPtr UserDefinedSQLFunctionFactory::get(Lock &, const String & function_name) const
{
    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);

    auto result = it->second.create_query;

    return result;
}

ASTPtr UserDefinedSQLFunctionFactory::get(const String & function_name) const
{
    auto lock = getLock();
    return get(lock, function_name);
}

ASTPtr UserDefinedSQLFunctionFactory::tryGet(Lock &, const std::string & function_name) const
{
    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        return nullptr;

    auto result = it->second.create_query;

    return result;
}

ASTPtr UserDefinedSQLFunctionFactory::tryGet(const std::string & function_name) const
{
    auto lock = getLock();
    return tryGet(lock, function_name);
}

bool UserDefinedSQLFunctionFactory::has(Lock & lock, const String & function_name) const
{
    return tryGet(lock, function_name) != nullptr;
}

bool UserDefinedSQLFunctionFactory::has(const String & function_name) const
{
    return tryGet(function_name) != nullptr;
}

std::vector<std::string> UserDefinedSQLFunctionFactory::getAllRegisteredNames(Lock &) const
{
    std::vector<std::string> registered_names;
    registered_names.reserve(function_name_to_create_query.size());

    for (const auto & [name, _] : function_name_to_create_query)
        registered_names.emplace_back(name);

    return registered_names;
}

std::vector<std::string> UserDefinedSQLFunctionFactory::getAllRegisteredNames() const
{
    auto lock = getLock();
    return getAllRegisteredNames(lock);
}

}
