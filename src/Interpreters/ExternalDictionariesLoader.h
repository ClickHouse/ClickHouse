#pragma once

#include <Dictionaries/IDictionary.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Interpreters/ExternalLoader.h>
#include <common/logger_useful.h>
#include <Parsers/ASTCreateQuery.h>
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
    ExternalDictionariesLoader(Context & context_);

    DictPtr getDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<const IDictionaryBase>(getLoadable(name));
    }

    DictPtr tryGetDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<const IDictionaryBase>(tryGetLoadable(name));
    }

    void addConfigRepository(
        const std::string & repository_name,
        std::unique_ptr<IExternalLoaderConfigRepository> config_repository);

    /// Starts reloading of a specified object.
    void addDictionaryWithConfig(
        const String & dictionary_name,
        const String & repo_name,
        const ASTCreateQuery & query,
        bool load_never_loading = false) const;


protected:
    LoadablePtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config,
            const std::string & key_in_config, const std::string & repository_name) const override;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;

private:

    Context & context;
};

}
