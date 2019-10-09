#pragma once

#include <Dictionaries/IDictionary.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Interpreters/ExternalLoader.h>
#include <common/logger_useful.h>
#include <memory>

namespace DB
{

class Context;

/// Manages user-defined dictionaries.
class ExternalDictionariesLoader : public ExternalLoader
{
public:
    using DictPtr = std::shared_ptr<const IDictionaryBase>;

    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalDictionariesLoader(
        ExternalLoaderConfigRepositoryPtr config_repository,
        Context & context_);

    DictPtr getDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<const IDictionaryBase>(getLoadable(name));
    }

    DictPtr tryGetDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<const IDictionaryBase>(tryGetLoadable(name));
    }

    /// Override ExternalLoader::reload to reset mysqlxx::PoolFactory.h
    /// since connection parameters might have changed. Inherited method is called afterward
    void reload(const String & name, bool load_never_loading = false);

    /// Override ExternalLoader::reload to reset mysqlxx::PoolFactory.h
    /// since connection parameters might have changed. Inherited method is called afterward
    void reload(bool load_never_loading = false);

protected:
    LoadablePtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config,
                       const std::string & key_in_config) const override;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;

private:

    Context & context;
};

}
