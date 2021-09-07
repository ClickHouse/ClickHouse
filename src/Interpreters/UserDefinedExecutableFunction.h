#pragma once

#include <string>

#include <DataTypes/IDataType.h>
#include <Interpreters/IExternalLoadable.h>


namespace DB
{

class UserDefinedExecutableFunction final : public IExternalLoadable
{
public:
    struct Config
    {
        std::string name;
        std::string script_path;
        std::string format;
        std::vector<DataTypePtr> argument_types;
        DataTypePtr result_type;
    };

    UserDefinedExecutableFunction(
        const Config & config_,
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
        return std::make_shared<UserDefinedExecutableFunction>(config, lifetime);
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
    ExternalLoadableLifetime lifetime;
};

}
