#pragma once

#include <string>

#include <DataTypes/IDataType.h>
#include <Processors/Sources/ShellCommandSource.h>
#include <Interpreters/IExternalLoadable.h>


namespace DB
{

struct UserDefinedExecutableFunctionArgument
{
    DataTypePtr type;
    String name;
};

struct UserDefinedExecutableFunctionParameter
{
    String name;
    DataTypePtr type;
};

struct UserDefinedExecutableFunctionConfiguration
{
    std::string name;
    std::string command;
    std::vector<std::string> command_arguments;
    std::vector<UserDefinedExecutableFunctionArgument> arguments;
    std::vector<UserDefinedExecutableFunctionParameter> parameters;
    DataTypePtr result_type;
    String result_name;
};

class UserDefinedExecutableFunction final : public IExternalLoadable
{
public:

    UserDefinedExecutableFunction(
        const UserDefinedExecutableFunctionConfiguration & configuration_,
        std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
        const ExternalLoadableLifetime & lifetime_);

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

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<UserDefinedExecutableFunction>(configuration, coordinator, lifetime);
    }

    const UserDefinedExecutableFunctionConfiguration & getConfiguration() const
    {
        return configuration;
    }

    std::shared_ptr<ShellCommandSourceCoordinator> getCoordinator() const
    {
        return coordinator;
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
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator;
    ExternalLoadableLifetime lifetime;
};

}
