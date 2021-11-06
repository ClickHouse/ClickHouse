#pragma once

#include <string>

#include <DataTypes/IDataType.h>
#include <Processors/Sources/ShellCommandSource.h>
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
    UserDefinedExecutableFunctionType type = UserDefinedExecutableFunctionType::executable;
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
        const ExternalLoadableLifetime & lifetime_,
        std::shared_ptr<ProcessPool> process_pool_ = nullptr);

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
        return std::make_shared<UserDefinedExecutableFunction>(configuration, lifetime, process_pool);
    }

    const UserDefinedExecutableFunctionConfiguration & getConfiguration() const
    {
        return configuration;
    }

    std::shared_ptr<ProcessPool> getProcessPool() const
    {
        return process_pool;
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
    UserDefinedExecutableFunction(const UserDefinedExecutableFunctionConfiguration & configuration_,
        std::shared_ptr<ProcessPool> process_pool_,
        const ExternalLoadableLifetime & lifetime_);

    UserDefinedExecutableFunctionConfiguration configuration;
    ExternalLoadableLifetime lifetime;
    std::shared_ptr<ProcessPool> process_pool;
};

}
