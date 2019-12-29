#pragma once

#include <Interpreters/CatBoostModel.h>
#include <Interpreters/ExternalLoader.h>
#include <common/logger_useful.h>
#include <memory>


namespace DB
{

class Context;

/// Manages user-defined models.
class ExternalModelsLoader : public ExternalLoader
{
public:
    using ModelPtr = std::shared_ptr<const IModel>;

    /// Models will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalModelsLoader(Context & context_);

    ModelPtr getModel(const std::string & name) const
    {
        return std::static_pointer_cast<const IModel>(load(name));
    }

    void addConfigRepository(const String & name,
        std::unique_ptr<IExternalLoaderConfigRepository> config_repository);


protected:
    LoadablePtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config,
            const std::string & key_in_config, const std::string & repository_name) const override;

    friend class StorageSystemModels;
private:

    Context & context;
};

}
