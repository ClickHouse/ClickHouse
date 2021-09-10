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

struct UserDefinedExecutableFunctionConfiguration
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
    /// Send number_of_rows\n before sending chunk to process
    bool send_chunk_header = false;
};

class UserDefinedExecutableFunction final : public IExternalLoadable
{
public:

    UserDefinedExecutableFunction(
        const UserDefinedExecutableFunctionConfiguration & configuration_,
        std::shared_ptr<scope_guard> function_deregister_,
        const ExternalLoadableLifetime & lifetime_);

    const ExternalLoadableLifetime & getLifetime() const override
    {
        return lifetime;
    }

    const std::string & getLoadableName() const override
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

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<UserDefinedExecutableFunction>(configuration, function_deregister, lifetime);
    }

    const UserDefinedExecutableFunctionConfiguration & getConfiguration() const
    {
        return configuration;
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
    UserDefinedExecutableFunctionConfiguration configuration;
    std::shared_ptr<scope_guard> function_deregister;
    ExternalLoadableLifetime lifetime;
};

}
