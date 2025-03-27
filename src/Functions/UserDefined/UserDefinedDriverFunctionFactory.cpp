#include "UserDefinedDriverFunctionFactory.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Parsers/ASTCreateDriverFunctionQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int CANNOT_DROP_FUNCTION;
    extern const int UNSUPPORTED_DRIVER;
}

UserDefinedDriverFunctionFactory::UserDefinedDriverFunctionFactory()
{
    registerDrivers();
}

UserDefinedDriverFunctionFactory & UserDefinedDriverFunctionFactory::instance()
{
    static UserDefinedDriverFunctionFactory result;
    return result;
}

void UserDefinedDriverFunctionFactory::checkCanBeRegistered(const ContextPtr & context, const String & function_name, const ASTPtr & query) const
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

    if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

    if (UserDefinedSQLFunctionFactory::instance().has(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined SQL function '{}' already exists", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context)) /// NOLINT(readability-static-accessed-through-instance)
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined executable function '{}' already exists", function_name);

    checkDriverExists(query);
}

void UserDefinedDriverFunctionFactory::checkCanBeUnregistered(const ContextPtr & context, const String & function_name)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name) ||
        AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (UserDefinedSQLFunctionFactory::instance().has(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined SQL function '{}'", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context)) /// NOLINT(readability-static-accessed-through-instance)
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined executable function '{}'", function_name);
}

void UserDefinedDriverFunctionFactory::checkDriverExists(const ASTPtr & query) const
{
    auto create_driver_function = query->as<ASTCreateDriverFunctionQuery &>();
    auto engine_name = create_driver_function.getEngineName();

    if (drivers.find(engine_name) == drivers.end())
        throw Exception(ErrorCodes::UNSUPPORTED_DRIVER, "Cannot find a driver with engine name '{}'", engine_name);
}

void UserDefinedDriverFunctionFactory::registerDrivers()
{
    // TODO: code to cmd
    DriverConfiguration docker_python3_tabsep = {"DockerPy3:TabSep", "TabSeparated",
        "docker run -i --rm python:3 /bin/bash -c \"echo \"print(2)\" >> tmp.py; python ./tmp.py\""};

    drivers["DockerPy3"] = docker_python3_tabsep;
    drivers["DockerPy3:TabSep"] = docker_python3_tabsep;
}

bool UserDefinedDriverFunctionFactory::registerFunction(const ContextMutablePtr & context, const String & function_name, ASTPtr create_function_query, bool throw_if_exists, bool replace_if_exists)
{
    checkCanBeRegistered(context, function_name, create_function_query);

    try
    {
        auto & loader = context->getUserDefinedSQLObjectsStorage();
        bool stored = loader.storeObject(
            context,
            UserDefinedSQLObjectType::DriverFunction,
            function_name,
            create_function_query,
            throw_if_exists,
            replace_if_exists,
            context->getSettingsRef());
        if (!stored)
            return false;
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while storing user defined driver function {}", backQuote(function_name)));
        throw;
    }

    return true;
}

bool UserDefinedDriverFunctionFactory::unregisterFunction(const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists)
{
    checkCanBeUnregistered(context, function_name);

    try
    {
        auto & storage = context->getUserDefinedSQLObjectsStorage();
        bool removed = storage.removeObject(
            context,
            UserDefinedSQLObjectType::DriverFunction,
            function_name,
            throw_if_not_exists);
        if (!removed)
            return false;
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while removing user defined driver function {}", backQuote(function_name)));
        throw;
    }

    return true;
}

FunctionOverloadResolverPtr UserDefinedDriverFunctionFactory::get(const String & function_name) const
{
    auto ptr = global_context->getUserDefinedSQLObjectsStorage().get(function_name, UserDefinedSQLObjectType::DriverFunction);
    return nullptr;
}

FunctionOverloadResolverPtr UserDefinedDriverFunctionFactory::tryGet(const String & function_name) const
{
    auto ptr = global_context->getUserDefinedSQLObjectsStorage().tryGet(function_name, UserDefinedSQLObjectType::DriverFunction);
    return nullptr;
}

bool UserDefinedDriverFunctionFactory::has(const String & function_name) const
{
    return global_context->getUserDefinedSQLObjectsStorage().has(function_name, UserDefinedSQLObjectType::DriverFunction);
}

std::vector<String> UserDefinedDriverFunctionFactory::getAllRegisteredNames() const
{
    return global_context->getUserDefinedSQLObjectsStorage().getAllObjectNames(UserDefinedSQLObjectType::DriverFunction);
}

bool UserDefinedDriverFunctionFactory::empty() const
{
    return global_context->getUserDefinedSQLObjectsStorage().empty(UserDefinedSQLObjectType::DriverFunction);
}

}
