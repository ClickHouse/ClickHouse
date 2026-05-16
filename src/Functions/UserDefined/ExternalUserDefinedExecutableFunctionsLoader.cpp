#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>

#include <Core/UUID.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <boost/algorithm/string/split.hpp>
#include <Common/StringUtils.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/filesystemHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeFactory.h>

#include <Functions/UserDefined/UserDefinedExecutableFunction.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <filesystem>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int TYPE_MISMATCH;
}

namespace
{
    /** Extract parameters from command and replace them with parameter names placeholders.
      * Example: test_script.py {parameter_name: UInt64}
      * After run function: test_script.py {parameter_name}
      */
    std::vector<UserDefinedExecutableFunctionParameter> extractParametersFromCommand(String & command_value)
    {
        std::vector<UserDefinedExecutableFunctionParameter> parameters;
        std::unordered_map<std::string_view, DataTypePtr> parameter_name_to_type;

        size_t previous_parameter_match_position = 0;
        while (true)
        {
            auto start_parameter_pos = command_value.find('{', previous_parameter_match_position);
            if (start_parameter_pos == std::string::npos)
                break;

            auto end_parameter_pos = command_value.find('}', start_parameter_pos);
            if (end_parameter_pos == std::string::npos)
                break;

            previous_parameter_match_position = start_parameter_pos + 1;

            auto semicolon_pos = command_value.find(':', start_parameter_pos);
            if (semicolon_pos == std::string::npos)
                break;
            if (semicolon_pos > end_parameter_pos)
                continue;

            std::string parameter_name(command_value.data() + start_parameter_pos + 1, command_value.data() + semicolon_pos);
            trim(parameter_name);

            bool is_identifier = std::all_of(parameter_name.begin(), parameter_name.end(), [](char character)
            {
                return isWordCharASCII(character);
            });

            if (parameter_name.empty() && !is_identifier)
                continue;

            std::string data_type_name(command_value.data() + semicolon_pos + 1, command_value.data() + end_parameter_pos);
            trim(data_type_name);

            if (data_type_name.empty())
                continue;

            DataTypePtr parameter_data_type = DataTypeFactory::instance().get(data_type_name);
            auto parameter_name_to_type_it = parameter_name_to_type.find(parameter_name);
            if (parameter_name_to_type_it != parameter_name_to_type.end() && !parameter_data_type->equals(*parameter_name_to_type_it->second))
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Multiple parameters with same name {} does not have same type. Expected {}. Actual {}",
                    parameter_name,
                    parameter_name_to_type_it->second->getName(),
                    parameter_data_type->getName());

            size_t replace_size = end_parameter_pos - start_parameter_pos - 1;
            command_value.replace(start_parameter_pos + 1, replace_size, parameter_name);
            previous_parameter_match_position = start_parameter_pos + parameter_name.size();

            if (parameter_name_to_type_it == parameter_name_to_type.end())
            {
                parameters.emplace_back(UserDefinedExecutableFunctionParameter{std::move(parameter_name), std::move(parameter_data_type)});
                auto & last_parameter = parameters.back();
                parameter_name_to_type.emplace(last_parameter.name, last_parameter.type);
            }
        }

        return parameters;
    }

    String getDynamicFunctionWorkingDirectory(const ContextPtr & context, const String & config_file_path, const String & function_name)
    {
        if (config_file_path.empty())
            return {};

        String dynamic_path = context->getDynamicUserDefinedExecutableFunctionsPath();
        if (dynamic_path.empty())
            return {};
        if (!dynamic_path.ends_with('/'))
            dynamic_path.push_back('/');

        if (!fileOrSymlinkPathStartsWith(config_file_path, dynamic_path))
            return {};

        auto config_path = std::filesystem::path(config_file_path);
        const String extension = config_path.extension().string();
        if (extension != ".xml" && extension != ".yaml")
            return {};

        const String metadata_path = config_path.replace_extension(".workdir").string();
        if (!std::filesystem::exists(metadata_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Dynamic executable user defined function '{}' config '{}' has no working directory metadata '{}'",
                function_name,
                config_file_path,
                metadata_path);

        String directory_name;
        ReadBufferFromFile in(metadata_path);
        readStringUntilEOF(directory_name, in);

        UUID uuid;
        if (directory_name.empty() || !tryParseUUID({reinterpret_cast<const UInt8 *>(directory_name.data()), directory_name.size()}, uuid))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Invalid dynamic executable user defined function working directory name '{}' for function '{}'",
                directory_name,
                function_name);

        return dynamic_path + directory_name;
    }
}

ExternalUserDefinedExecutableFunctionsLoader::ExternalUserDefinedExecutableFunctionsLoader(ContextPtr global_context_)
    : ExternalLoader("external user defined function", getLogger("ExternalUserDefinedExecutableFunctionsLoader"))
    , WithContext(global_context_)
{
    setConfigSettings({"function", "name", "database", "uuid"});
    enableAsyncLoading(false);
    if (getContext()->getApplicationType() == Context::ApplicationType::SERVER)
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

ExternalLoader::LoadableMutablePtr ExternalUserDefinedExecutableFunctionsLoader::createObject(const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & key_in_config,
    const std::string &,
    const std::string & config_file_path) const
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
    String command_working_directory;
    if (execute_direct)
        command_working_directory = getDynamicFunctionWorkingDirectory(getContext(), config_file_path, name);
    std::vector<UserDefinedExecutableFunctionParameter> parameters = extractParametersFromCommand(command_value);

    if (!execute_direct && !parameters.empty())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Parameters are not supported if executable user defined function is not direct");

    VectorWithMemoryTracking<String> command_arguments;

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

    bool is_deterministic = config.getBool(key_in_config + ".deterministic", false);

    bool send_chunk_header = config.getBool(key_in_config + ".send_chunk_header", false);
    size_t command_termination_timeout_seconds = config.getUInt64(key_in_config + ".command_termination_timeout", 10);
    size_t command_read_timeout_milliseconds = config.getUInt64(key_in_config + ".command_read_timeout", 10000);
    size_t command_write_timeout_milliseconds = config.getUInt64(key_in_config + ".command_write_timeout", 10000);
    size_t command_pipe_capacity = config.getUInt64(key_in_config + ".command_pipe_capacity", 0);
    ExternalCommandStderrReaction stderr_reaction
        = parseExternalCommandStderrReaction(config.getString(key_in_config + ".stderr_reaction", "log_last"));
    bool check_exit_code = config.getBool(key_in_config + ".check_exit_code", true);

    size_t pool_size = 0;
    size_t max_command_execution_time = 0;

    if (is_executable_pool)
    {
        pool_size = config.getUInt64(key_in_config + ".pool_size", 16);
        max_command_execution_time = config.getUInt64(key_in_config + ".max_command_execution_time", 10);

        size_t max_execution_time_seconds = static_cast<size_t>(getContext()->getSettingsRef()[Setting::max_execution_time].totalSeconds());
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

    if (is_executable_pool && !parameters.empty())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable user defined functions with `executable_pool` type does not support parameters");

    UserDefinedExecutableFunctionConfiguration function_configuration
    {
        .name = name,
        .command = std::move(command_value),
        .command_arguments = std::move(command_arguments),
        .command_working_directory = std::move(command_working_directory),
        .arguments = std::move(arguments),
        .parameters = std::move(parameters),
        .result_type = std::move(result_type),
        .result_name = std::move(result_name),
        .is_deterministic = is_deterministic
    };

    ShellCommandSourceCoordinator::Configuration shell_command_coordinator_configration
    {
        .format = std::move(format),
        .command_termination_timeout_seconds = command_termination_timeout_seconds,
        .command_read_timeout_milliseconds = command_read_timeout_milliseconds,
        .command_write_timeout_milliseconds = command_write_timeout_milliseconds,
        .command_pipe_capacity = command_pipe_capacity,
        .stderr_reaction = stderr_reaction,
        .check_exit_code = check_exit_code,
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
