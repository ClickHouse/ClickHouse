#include "ExternalUserDefinedExecutableFunctionsLoader.h"

#include <boost/algorithm/string/split.hpp>

#include <DataTypes/DataTypeFactory.h>

#include <Interpreters/UserDefinedExecutableFunction.h>
#include <Interpreters/UserDefinedExecutableFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FUNCTION_ALREADY_EXISTS;
}

ExternalUserDefinedExecutableFunctionsLoader::ExternalUserDefinedExecutableFunctionsLoader(ContextPtr global_context_)
    : ExternalLoader("external user defined function", &Poco::Logger::get("ExternalUserDefinedExecutableFunctionsLoader"))
    , WithContext(global_context_)
{
    setConfigSettings({"function", "name", "database", "uuid"});
    enableAsyncLoading(false);
    enablePeriodicUpdates(true);
    enableAlwaysLoadEverything(true);
}

ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr ExternalUserDefinedExecutableFunctionsLoader::getUserDefinedFunction(const std::string & user_defined_function_name) const
{
    return std::static_pointer_cast<const UserDefinedExecutableFunction>(load(user_defined_function_name));
}

ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr ExternalUserDefinedExecutableFunctionsLoader::tryGetUserDefinedFunction(const std::string & user_defined_function_name) const
{
    return std::static_pointer_cast<const UserDefinedExecutableFunction>(tryLoad(user_defined_function_name));
}

void ExternalUserDefinedExecutableFunctionsLoader::reloadFunction(const std::string & user_defined_function_name) const
{
    loadOrReload(user_defined_function_name);
}

ExternalLoader::LoadablePtr ExternalUserDefinedExecutableFunctionsLoader::create(const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & key_in_config,
    const std::string &) const
{
    if (FunctionFactory::instance().hasNameOrAlias(name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", name);

    if (AggregateFunctionFactory::instance().hasNameOrAlias(name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", name);

    String type = config.getString(key_in_config + ".type");

    bool is_executable_pool = false;

    if (type == "executable")
        is_executable_pool = false;
    else if (type == "executable_pool")
        is_executable_pool = true;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Wrong user defined function type expected 'executable' or 'executable_pool' actual {}",
            type);

    bool execute_direct = config.getBool(key_in_config + ".execute_direct", true);

    String command_value = config.getString(key_in_config + ".command");
    std::vector<String> command_arguments;

    if (execute_direct)
    {
        boost::split(command_arguments, command_value, [](char c) { return c == ' '; });

        command_value = std::move(command_arguments[0]);
        command_arguments.erase(command_arguments.begin());
    }

    String format = config.getString(key_in_config + ".format");
    DataTypePtr result_type = DataTypeFactory::instance().get(config.getString(key_in_config + ".return_type"));
    String result_name = "result";
    if (config.has(key_in_config + ".return_name"))
        result_name = config.getString(key_in_config + ".return_name");

    bool send_chunk_header = config.getBool(key_in_config + ".send_chunk_header", false);
    size_t command_termination_timeout_seconds = config.getUInt64(key_in_config + ".command_termination_timeout", 10);
    size_t command_read_timeout_milliseconds = config.getUInt64(key_in_config + ".command_read_timeout", 10000);
    size_t command_write_timeout_milliseconds = config.getUInt64(key_in_config + ".command_write_timeout", 10000);

    size_t pool_size = 0;
    size_t max_command_execution_time = 0;

    if (is_executable_pool)
    {
        pool_size = config.getUInt64(key_in_config + ".pool_size", 16);
        max_command_execution_time = config.getUInt64(key_in_config + ".max_command_execution_time", 10);

        size_t max_execution_time_seconds = static_cast<size_t>(getContext()->getSettings().max_execution_time.totalSeconds());
        if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
            max_command_execution_time = max_execution_time_seconds;
    }

    ExternalLoadableLifetime lifetime;

    if (config.has(key_in_config + ".lifetime"))
        lifetime = ExternalLoadableLifetime(config, key_in_config + ".lifetime");

    std::vector<UserDefinedExecutableFunctionArgument> arguments;

    Poco::Util::AbstractConfiguration::Keys config_elems;
    config.keys(key_in_config, config_elems);

    size_t argument_number = 1;

    for (const auto & config_elem : config_elems)
    {
        if (!startsWith(config_elem, "argument"))
            continue;

        UserDefinedExecutableFunctionArgument argument;

        const auto argument_prefix = key_in_config + '.' + config_elem + '.';

        argument.type = DataTypeFactory::instance().get(config.getString(argument_prefix + "type"));

        if (config.has(argument_prefix + "name"))
            argument.name = config.getString(argument_prefix + "name");
        else
            argument.name = "c" + std::to_string(argument_number);

        ++argument_number;
        arguments.emplace_back(std::move(argument));
    }

    UserDefinedExecutableFunctionConfiguration function_configuration
    {
        .name = name,
        .command = std::move(command_value),
        .command_arguments = std::move(command_arguments),
        .arguments = std::move(arguments),
        .result_type = std::move(result_type),
        .result_name = std::move(result_name),
    };

    ShellCommandSourceCoordinator::Configuration shell_command_coordinator_configration
    {
        .format = std::move(format),
        .command_termination_timeout_seconds = command_termination_timeout_seconds,
        .command_read_timeout_milliseconds = command_read_timeout_milliseconds,
        .command_write_timeout_milliseconds = command_write_timeout_milliseconds,
        .pool_size = pool_size,
        .max_command_execution_time_seconds = max_command_execution_time,
        .is_executable_pool = is_executable_pool,
        .send_chunk_header = send_chunk_header,
        .execute_direct = execute_direct
    };

    auto coordinator = std::make_shared<ShellCommandSourceCoordinator>(shell_command_coordinator_configration);
    return std::make_shared<UserDefinedExecutableFunction>(function_configuration, std::move(coordinator), lifetime);
}

}
