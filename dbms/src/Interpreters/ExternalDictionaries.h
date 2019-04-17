#pragma once

#include <Dictionaries/IDictionary.h>
#include <Interpreters/ExternalLoader.h>
#include <common/logger_useful.h>
#include <memory>


namespace DB
{

class Context;

/// Manages user-defined dictionaries.
class ExternalDictionaries : public ExternalLoader
{
public:
    using DictPtr = std::shared_ptr<IDictionaryBase>;

    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalDictionaries(
        std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
        const Poco::Util::AbstractConfiguration & config,
        Context & context);

    /// Forcibly reloads specified dictionary.
    void reloadDictionary(const std::string & name) { reload(name); }

    DictPtr getDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<IDictionaryBase>(getLoadable(name));
    }

    DictPtr tryGetDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<IDictionaryBase>(tryGetLoadable(name));
    }

protected:

    std::unique_ptr<IExternalLoadable> create(const std::string & name, const Configuration & config,
                                              const std::string & config_prefix) const override;

    using ExternalLoader::getObjectsMap;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;

private:

    Context & context;
};

}
