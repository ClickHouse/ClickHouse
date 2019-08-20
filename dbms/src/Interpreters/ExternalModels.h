#pragma once

#include <Interpreters/CatBoostModel.h>
#include <Interpreters/ExternalLoader.h>
#include <common/logger_useful.h>
#include <memory>


namespace DB
{

class Context;

/// Manages user-defined models.
class ExternalModels : public ExternalLoader
{
public:
    using ModelPtr = std::shared_ptr<const IModel>;

    /// Models will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalModels(
        std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
        Context & context_);

    ModelPtr getModel(const std::string & name) const
    {
        return std::static_pointer_cast<const IModel>(getLoadable(name));
    }

protected:
    LoadablePtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config,
                       const std::string & key_in_config) const override;

    friend class StorageSystemModels;
private:

    Context & context;
};

}
