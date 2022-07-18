#pragma once

#include <Interpreters/CatBoostModel.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExternalLoader.h>
#include <base/logger_useful.h>

#include <memory>


namespace DB
{

/// Manages user-defined models.
class ExternalModelsLoader : public ExternalLoader, WithContext
{
public:
    using ModelPtr = std::shared_ptr<const IModel>;

    /// Models will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    explicit ExternalModelsLoader(ContextPtr context_);

    ModelPtr getModel(const std::string & model_name) const
    {
        return std::static_pointer_cast<const IModel>(load(model_name));
    }

    void reloadModel(const std::string & model_name) const
    {
        loadOrReload(model_name);
    }

protected:
    LoadablePtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config,
            const std::string & config_prefix, const std::string & repository_name) const override;

    friend class StorageSystemModels;
};

}
