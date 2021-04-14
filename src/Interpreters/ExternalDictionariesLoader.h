#pragma once

#include <memory>

#include <Common/quoteString.h>
#include <Interpreters/ExternalLoader.h>
#include <Dictionaries/IDictionary.h>

namespace DB
{
class Context;
class IExternalLoaderConfigRepository;

/// Manages user-defined dictionaries.
class ExternalDictionariesLoader : public ExternalLoader
{
public:
    using DictPtr = std::shared_ptr<const IDictionaryBase>;

    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    explicit ExternalDictionariesLoader(Context & global_context_);

    DictPtr getDictionary(const std::string & dictionary_name, const Context & context) const;

    DictPtr tryGetDictionary(const std::string & dictionary_name, const Context & context) const;

    void reloadDictionary(const std::string & dictionary_name, const Context & context) const;

    DictionaryStructure getDictionaryStructure(const std::string & dictionary_name, const Context & context) const;

    static DictionaryStructure getDictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config = "dictionary");

    static DictionaryStructure getDictionaryStructure(const ObjectConfig & config);

    static void resetAll();

protected:
    LoadablePtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config,
            const std::string & key_in_config, const std::string & repository_name) const override;

    std::string resolveDictionaryName(const std::string & dictionary_name, const std::string & current_database_name) const;

    /// Try convert qualified dictionary name to persistent UUID
    std::string resolveDictionaryNameFromDatabaseCatalog(const std::string & name) const;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;

private:
    Context & global_context;
};

}
