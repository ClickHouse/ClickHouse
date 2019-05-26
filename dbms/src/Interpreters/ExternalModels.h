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
    using ModelPtr = std::shared_ptr<IModel>;

    /// Models will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalModels(
        std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
        Context & context);

    /// Forcibly reloads specified model.
    void reloadModel(const std::string & name) { reload(name); }

    ModelPtr getModel(const std::string & name) const
    {
        return std::static_pointer_cast<IModel>(getLoadable(name));
    }

protected:

    std::unique_ptr<IExternalLoadable> create(const std::string & name, const Configuration & config,
                                              const std::string & config_prefix) const override;

    using ExternalLoader::getObjectsMap;

    friend class StorageSystemModels;
private:

    Context & context;
};

}
