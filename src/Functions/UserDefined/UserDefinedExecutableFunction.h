#pragma once

#include <string>
#include <memory>

#include <DataTypes/IDataType.h>
#include <Processors/Sources/ShellCommandSource.h>


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
    bool is_deterministic;
};

class UserDefinedExecutableFunction
{
public:
    UserDefinedExecutableFunction(
        const UserDefinedExecutableFunctionConfiguration & configuration_,
        std::shared_ptr<ShellCommandSourceCoordinator> coordinator_);

    const UserDefinedExecutableFunctionConfiguration & getConfiguration() const
    {
        return configuration;
    }

    std::shared_ptr<ShellCommandSourceCoordinator> getCoordinator() const
    {
        return coordinator;
    }

protected:
    UserDefinedExecutableFunctionConfiguration configuration;
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator;
};

using UserDefinedExecutableFunctionPtr = std::shared_ptr<const UserDefinedExecutableFunction>;

}
