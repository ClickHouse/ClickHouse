#include "UserDefinedDriverFunctionFactory.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Parsers/ASTCreateDriverFunctionQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Interpreters/Context.h>


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

void UserDefinedDriverFunctionFactory::checkCanBeRegistered(const ContextPtr & context, const String & function_name, const ASTPtr & query)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

    if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context)) /// NOLINT(readability-static-accessed-through-instance)
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined executable function '{}' already exists", function_name);

    checkDriverExists(query);
}

void UserDefinedDriverFunctionFactory::checkCanBeUnregistered(const ContextPtr & context, const String & function_name)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name) ||
        AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context)) /// NOLINT(readability-static-accessed-through-instance)
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined executable function '{}'", function_name);
}

void UserDefinedDriverFunctionFactory::checkDriverExists(const ASTPtr & query)
{
    auto create_driver_function = query->as<ASTCreateDriverFunctionQuery &>();
    auto engine_name = create_driver_function.getEngineName();

    if (drivers.find(engine_name) == drivers.end())
        throw Exception(ErrorCodes::UNSUPPORTED_DRIVER, "Cannot find a driver with engine name '{}'", engine_name);
}

void UserDefinedDriverFunctionFactory::registerDrivers()
{
    DriverConfiguration docker_python3_tabsep = {"DockerPy3:TabSep", "TabSeparated",
        "docker run -i --rm python:3 /bin/bash -c \"echo \"print(2)\" >> tmp.py; python ./tmp.py\""};

    drivers["DockerPy3"] = docker_python3_tabsep;
    drivers["DockerPy3:TabSep"] = docker_python3_tabsep;
}

bool UserDefinedDriverFunctionFactory::registerFunction(const ContextMutablePtr & context, const String & function_name, ASTPtr query, bool throw_if_exists, bool replace_if_exists)
{
    checkCanBeRegistered(context, function_name, query);

    if (functions.find(function_name) != functions.end()) {
        if (throw_if_exists)
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The driver function '{}' already exists", function_name);
        if (!replace_if_exists)
            return false;
    }

    auto create_driver_function = query->as<ASTCreateDriverFunctionQuery &>();
    auto engine_name = create_driver_function.getEngineName();
    auto result_type = DataTypeFactory::instance().get(create_driver_function.function_return_type);
    auto function_body = create_driver_function.function_body->as<ASTLiteral>()->value.safeGet<String>();

    std::vector<UserDefinedDriverFunctionArgument> arguments;
    for (auto & child : create_driver_function.function_params->children) {
        auto * column = child->as<ASTNameTypePair>();
        auto column_name = column->name;
        auto column_type = DataTypeFactory::instance().get(column->type);
        arguments.emplace_back(column_type, column_name);
    }

    UserDefinedDriverFunctionConfiguration configuration;
    configuration.name = function_name;
    configuration.arguments = std::move(arguments);
    configuration.result_type = result_type;
    configuration.driver_name = engine_name;
    configuration.body = function_body;

    functions[function_name] = std::move(configuration);
    // TODO: save update to XML
    return true;
}

bool UserDefinedDriverFunctionFactory::unregisterFunction(const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists)
{
    checkCanBeUnregistered(context, function_name);

    auto it = functions.find(function_name);
    if (it == functions.end()) {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "The driver function '{}' does not exist", function_name);
        else
            return false;
    }

    functions.erase(it);
    // TODO: save update to XML
    return true;
}

FunctionOverloadResolverPtr UserDefinedDriverFunctionFactory::get(const String & function_name, ContextPtr context)
{
    return nullptr;
}

FunctionOverloadResolverPtr UserDefinedDriverFunctionFactory::tryGet(const String & function_name, ContextPtr context)
{
    return nullptr;
}

bool UserDefinedDriverFunctionFactory::has(const String & function_name)
{
    return functions.find(function_name) != functions.end();
}

std::vector<String> UserDefinedDriverFunctionFactory::getRegisteredNames()
{
    std::vector<String> function_names;
    for (auto & [name, _] : functions) {
        function_names.push_back(name);
    }
    return function_names;
}

}
