#include "UserDefinedDriverFunctionFactory.h"

#include <boost/algorithm/string.hpp>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedFunction.h>
#include <Functions/UserDefined/UserDefinedDriverFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>


namespace DB
{

namespace
{

String formatCodeBlock(const String & code)
{
    std::vector<String> lines;
    boost::split(lines, code, boost::is_any_of("\n"));

    size_t min_tabs_count = String::npos;
    for (const auto & line : lines)
    {
        size_t spaces = line.find_first_not_of(" \t");
        if (spaces != String::npos)
        {
            min_tabs_count = std::min(min_tabs_count, spaces);
        }
    }

    if (min_tabs_count != String::npos)
    {
        for (auto & line : lines)
        {
            if (line.empty())
                continue;

            line.erase(0, min_tabs_count);
            boost::replace_all(line, "\"", "\\\"");
        }
    }

    if (!lines.empty())
    {
        boost::trim(lines.front());
        boost::trim(lines.back());
    }

    String result;
    for (size_t i = 0; i < lines.size(); ++i)
    {
        result += lines[i];
        if (i + 1 < lines.size())
            result += '\n';
    }

    return result;
}

}

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int CANNOT_DROP_FUNCTION;
    extern const int UNSUPPORTED_DRIVER;
}

UserDefinedDriverFunctionFactory & UserDefinedDriverFunctionFactory::instance()
{
    static UserDefinedDriverFunctionFactory result;
    return result;
}

bool UserDefinedDriverFunctionFactory::registerFunction(const ContextMutablePtr & context, const String & function_name, ASTPtr create_function_query, bool throw_if_exists, bool replace_if_exists)
{
    checkCanBeRegistered(function_name, create_function_query);

    auto function_body = create_function_query->as<ASTCreateDriverFunctionQuery>()->function_body;
    auto * literal_function_body = function_body->as<ASTLiteral>();
    auto str_function_body = literal_function_body->value.safeGet<String>();
    literal_function_body->value = formatCodeBlock(str_function_body);

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
    checkCanBeUnregistered(function_name);

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
    auto create_query = ptr->as<ASTCreateDriverFunctionQuery &>();

    auto driver_name = create_query.getEngineName();
    auto driver = UserDefinedDriverFactory::instance().get(driver_name, global_context); /// NOLINT(readability-static-accessed-through-instance)

    auto executable_function = createUserDefinedFunction(create_query, driver);

    Array parameters;
    auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), global_context, parameters);
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
}

FunctionOverloadResolverPtr UserDefinedDriverFunctionFactory::tryGet(const String & function_name) const
{
    auto ptr = global_context->getUserDefinedSQLObjectsStorage().tryGet(function_name, UserDefinedSQLObjectType::DriverFunction);
    if (!ptr)
        return nullptr;

    auto create_query = ptr->as<ASTCreateDriverFunctionQuery &>();

    auto driver_name = create_query.getEngineName();
    auto driver = UserDefinedDriverFactory::instance().tryGet(driver_name, global_context); /// NOLINT(readability-static-accessed-through-instance)
    if (!driver)
        return nullptr;

    auto executable_function = createUserDefinedFunction(create_query, driver);
    Array parameters;

    auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), global_context, parameters);
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
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

void UserDefinedDriverFunctionFactory::checkCanBeRegistered(const String & function_name, const ASTPtr & query) const
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

    if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

    if (UserDefinedSQLFunctionFactory::instance().has(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined SQL function '{}' already exists", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, global_context)) /// NOLINT(readability-static-accessed-through-instance)
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined executable function '{}' already exists", function_name);

    checkDriverExists(query);
}

void UserDefinedDriverFunctionFactory::checkCanBeUnregistered(const String & function_name)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name) || AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (UserDefinedSQLFunctionFactory::instance().has(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined SQL function '{}'", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, global_context)) /// NOLINT(readability-static-accessed-through-instance)
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined executable function '{}'", function_name);
}

void UserDefinedDriverFunctionFactory::checkDriverExists(const ASTPtr & query) const
{
    auto create_driver_function = query->as<ASTCreateDriverFunctionQuery &>();
    auto engine_name = create_driver_function.getEngineName();

    auto names = UserDefinedDriverFactory::instance().getRegisteredNames(global_context); /// NOLINT(readability-static-accessed-through-instance)

    if (!UserDefinedDriverFactory::instance().has(engine_name, global_context)) /// NOLINT(readability-static-accessed-through-instance)
        throw Exception(ErrorCodes::UNSUPPORTED_DRIVER, "Cannot find a driver with engine name '{}', drivers size = {}", engine_name, names.size());
}

UserDefinedExecutableFunctionPtr UserDefinedDriverFunctionFactory::createUserDefinedFunction(
    const ASTCreateDriverFunctionQuery & query, const UserDefinedDriverPtr & driver) const
{
    const auto & configuration = driver->getConfiguration();

    std::vector<UserDefinedExecutableFunctionArgument> arguments;
    if (query.function_params)
    {
        for (auto & child : query.function_params->children)
        {
            auto * column = child->as<ASTNameTypePair>();
            arguments.emplace_back(DataTypeFactory::instance().get(column->type), column->name);
        }
    }

    auto result_type = DataTypeFactory::instance().get(query.function_return_type->as<ASTDataType>()->name);

    auto command = configuration.command;
    auto command_arguments = configuration.command_arguments;

    const auto & code = query.getFunctionBody();
    if (command_arguments.empty())
    {
        command += " \"" + code + '\"';
    }
    else
    {
        command_arguments.push_back(code);
    }

    UserDefinedExecutableFunctionConfiguration func_configuration{
        .name = query.getFunctionName(),
        .command = command,
        .command_arguments = command_arguments,
        .arguments = std::move(arguments),
        .parameters = {},
        .result_type = result_type,
        .result_name = "result",
        .is_deterministic = false};

    ShellCommandSourceCoordinator::Configuration shell_command_coordinator_configration{
        .format = configuration.format,
        .command_termination_timeout_seconds = configuration.command_termination_timeout_seconds,
        .command_read_timeout_milliseconds = configuration.command_read_timeout_milliseconds,
        .command_write_timeout_milliseconds = configuration.command_write_timeout_milliseconds,
        .stderr_reaction = configuration.stderr_reaction,
        .check_exit_code = configuration.check_exit_code,
        .pool_size = configuration.pool_size,
        .max_command_execution_time_seconds = configuration.max_command_execution_time_seconds,
        .is_executable_pool = configuration.is_executable_pool,
        .send_chunk_header = configuration.send_chunk_header,
        .execute_direct = configuration.execute_direct};

    auto coordinator = std::make_shared<ShellCommandSourceCoordinator>(shell_command_coordinator_configration);
    return std::make_shared<UserDefinedExecutableFunction>(std::move(func_configuration), std::move(coordinator));
}
}
