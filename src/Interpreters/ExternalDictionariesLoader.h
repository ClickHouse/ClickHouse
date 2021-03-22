#pragma once

#include <Dictionaries/IDictionary.h>
#include <Interpreters/ExternalLoader.h>
#include <memory>

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
    ExternalDictionariesLoader(Context & context_);

    DictPtr getDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<const IDictionaryBase>(load(name));
    }

    DictPtr tryGetDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<const IDictionaryBase>(tryLoad(name));
    }

    static DictionaryStructure getDictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config = "dictionary");
    static DictionaryStructure getDictionaryStructure(const ObjectConfig & config);

    static void resetAll();

protected:
    LoadablePtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config,
            const std::string & key_in_config, const std::string & repository_name) const override;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;

private:
    Context & context;
};

}
