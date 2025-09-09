#pragma once

#include <DataTypes/IDataType.h>
#include <Functions/UserDefined/UserDefinedExecutableFunction.h>
#include <Interpreters/IExternalLoadable.h>


namespace DB
{

class UserDefinedLoadableFunction final : public UserDefinedExecutableFunction, public IExternalLoadable
{
public:
    UserDefinedLoadableFunction(
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

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<UserDefinedLoadableFunction>(configuration, coordinator, lifetime);
    }

    std::shared_ptr<UserDefinedLoadableFunction> shared_from_this()
    {
        return std::static_pointer_cast<UserDefinedLoadableFunction>(IExternalLoadable::shared_from_this());
    }

    std::shared_ptr<const UserDefinedLoadableFunction> shared_from_this() const
    {
        return std::static_pointer_cast<const UserDefinedLoadableFunction>(IExternalLoadable::shared_from_this());
    }

private:
    ExternalLoadableLifetime lifetime;
};

}
