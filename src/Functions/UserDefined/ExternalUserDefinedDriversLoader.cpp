#include "ExternalUserDefinedDriversLoader.h"

#include <Common/StringUtils.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <boost/algorithm/string/split.hpp>


namespace DB
{
namespace Setting
{
extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

ExternalUserDefinedDriversLoader::ExternalUserDefinedDriversLoader(ContextPtr global_context_)
    : ExternalLoader("external user defined driver", getLogger("ExternalUserDefinedDriversLoader"))
    , WithContext(global_context_)
{
    setConfigSettings({"driver", "name", "database", "uuid"});
    enableAsyncLoading(false);
    if (getContext()->getApplicationType() == Context::ApplicationType::SERVER)
        enablePeriodicUpdates(true);
    enableAlwaysLoadEverything(true);
}

UserDefinedDriverPtr ExternalUserDefinedDriversLoader::getUserDefinedDriver(const std::string & user_defined_driver_name) const
{
    return std::static_pointer_cast<const UserDefinedDriver>(load(user_defined_driver_name));
}

UserDefinedDriverPtr ExternalUserDefinedDriversLoader::tryGetUserDefinedDriver(const std::string & user_defined_driver_name) const
{
    return std::static_pointer_cast<const UserDefinedDriver>(tryLoad(user_defined_driver_name));
}

void ExternalUserDefinedDriversLoader::reloadDriver(const std::string & user_defined_driver_name) const
{
    loadOrReload(user_defined_driver_name);
}

ExternalLoader::LoadableMutablePtr ExternalUserDefinedDriversLoader::createObject(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & key_in_config,
    const std::string &) const
{
    String type = config.getString(key_in_config + ".type");

    bool is_executable_pool = false;

    if (type == "executable")
        is_executable_pool = false;
    else if (type == "executable_pool")
        is_executable_pool = true;
    else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Wrong user defined driver type expected 'executable' or 'executable_pool' actual {}", type);

    bool execute_direct = config.getBool(key_in_config + ".execute_direct", false);

    String command_value = config.getString(key_in_config + ".command");

    std::vector<String> command_arguments;

    if (execute_direct)
    {
        boost::split(command_arguments, command_value, [](char c) { return c == ' '; });

        command_value = std::move(command_arguments[0]);
        command_arguments.erase(command_arguments.begin());
    }

    String format = config.getString(key_in_config + ".format");

    bool send_chunk_header = config.getBool(key_in_config + ".send_chunk_header", false);
    size_t command_termination_timeout_seconds = config.getUInt64(key_in_config + ".command_termination_timeout", 10);
    size_t command_read_timeout_milliseconds = config.getUInt64(key_in_config + ".command_read_timeout", 10000);
    size_t command_write_timeout_milliseconds = config.getUInt64(key_in_config + ".command_write_timeout", 10000);
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

    UserDefinedDriverConfiguration configuration
    {
        .name = name,
        .command = std::move(command_value),
        .command_arguments = std::move(command_arguments),
        .format = std::move(format),
        .command_termination_timeout_seconds = command_termination_timeout_seconds,
        .command_read_timeout_milliseconds = command_read_timeout_milliseconds,
        .command_write_timeout_milliseconds = command_write_timeout_milliseconds,
        .max_command_execution_time_seconds = max_command_execution_time,
        .check_exit_code = check_exit_code,
        .stderr_reaction = stderr_reaction,
        .is_executable_pool = is_executable_pool,
        .pool_size = pool_size,
        .send_chunk_header = send_chunk_header,
        .execute_direct = execute_direct
    };

    return std::make_shared<UserDefinedDriver>(configuration, lifetime);
}

}
