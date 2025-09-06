#pragma once

#include <Common/ShellCommandSettings.h>
#include <Interpreters/IExternalLoadable.h>

#include <string>
#include <vector>


namespace DB
{

struct UserDefinedDriverConfiguration
{
    std::string name;
    std::string command;
    std::vector<std::string> command_arguments;
    std::string format;

    size_t command_termination_timeout_seconds;
    size_t command_read_timeout_milliseconds;
    size_t command_write_timeout_milliseconds;
    size_t max_command_execution_time_seconds;

    bool check_exit_code;
    ExternalCommandStderrReaction stderr_reaction;
    bool is_executable_pool;
    size_t pool_size;
    bool send_chunk_header;
    bool execute_direct;
};

class UserDefinedDriver : public IExternalLoadable
{
public:
    UserDefinedDriver(const UserDefinedDriverConfiguration & configuration_,
        const ExternalLoadableLifetime & lifetime_)
        : configuration(configuration_)
        , lifetime(lifetime_)
    {
    }

    const UserDefinedDriverConfiguration & getConfiguration() const
    {
        return configuration;
    }

    const ExternalLoadableLifetime & getLifetime() const override
    {
        return lifetime;
    }

    std::string getLoadableName() const override
    {
        return configuration.name;
    }

    bool supportUpdates() const override
    {
        return true;
    }

    bool isModified() const override
    {
        return true;
    }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<UserDefinedDriver>(configuration, lifetime);
    }

    std::shared_ptr<UserDefinedDriver> shared_from_this()
    {
        return std::static_pointer_cast<UserDefinedDriver>(IExternalLoadable::shared_from_this());
    }

    std::shared_ptr<const UserDefinedDriver> shared_from_this() const
    {
        return std::static_pointer_cast<const UserDefinedDriver>(IExternalLoadable::shared_from_this());
    }

private:
    UserDefinedDriverConfiguration configuration;
    ExternalLoadableLifetime lifetime;
};

using UserDefinedDriverPtr = std::shared_ptr<const UserDefinedDriver>;

}
