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
    explicit ExternalDictionariesLoader(Context & context_);

    DictPtr getDictionary(const std::string & dictionary_name) const;

    DictPtr tryGetDictionary(const std::string & dictionary_name) const;

    void reloadDictionary(const std::string & dictionary_name) const;

    DictionaryStructure getDictionaryStructure(const std::string & dictionary_name) const;

    static DictionaryStructure getDictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config = "dictionary");
    static DictionaryStructure getDictionaryStructure(const ObjectConfig & config);

    static void resetAll();

protected:
    LoadablePtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config,
            const std::string & key_in_config, const std::string & repository_name) const override;

    std::string resolveDictionaryName(const std::string & dictionary_name) const;

    /// Try convert qualified dictionary name to persistent UUID
    std::string resolveDictionaryNameFromDatabaseCatalog(const std::string & name) const;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;

private:
    Context & context;
};

}
