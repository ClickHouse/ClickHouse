#pragma once

#include <string>

#include <common/scope_guard.h>

#include <DataTypes/IDataType.h>
#include <Interpreters/IExternalLoadable.h>


namespace DB
{

enum class UserDefinedExecutableFunctionType
{
    executable,
    executable_pool
};

class UserDefinedExecutableFunction final : public IExternalLoadable
{
public:

    struct Config
    {
        UserDefinedExecutableFunctionType type;
        std::string name;
        std::string script_path;
        std::string format;
        std::vector<DataTypePtr> argument_types;
        DataTypePtr result_type;
        /// Pool settings
        size_t pool_size = 0;
        size_t command_termination_timeout = 0;
        size_t max_command_execution_time = 0;
    };

    UserDefinedExecutableFunction(
        const Config & config_,
        std::shared_ptr<scope_guard> function_deregister_,
        const ExternalLoadableLifetime & lifetime_);

    const ExternalLoadableLifetime & getLifetime() const override
    {
        return lifetime;
    }

    const std::string & getLoadableName() const override
    {
        return config.name;
    }

    bool supportUpdates() const override
    {
        return true;
    }

    bool isModified() const override
    {
        return true;
    }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<UserDefinedExecutableFunction>(config, function_deregister, lifetime);
    }

    const Config & getConfig() const
    {
        return config;
    }

    std::shared_ptr<UserDefinedExecutableFunction> shared_from_this()
    {
        return std::static_pointer_cast<UserDefinedExecutableFunction>(IExternalLoadable::shared_from_this());
    }

    std::shared_ptr<const UserDefinedExecutableFunction> shared_from_this() const
    {
        return std::static_pointer_cast<const UserDefinedExecutableFunction>(IExternalLoadable::shared_from_this());
    }

private:
    Config config;
    std::shared_ptr<scope_guard> function_deregister;
    ExternalLoadableLifetime lifetime;
};

}
