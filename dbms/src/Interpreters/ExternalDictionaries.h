#pragma once

#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/IDictionary.h>
#include <Interpreters/ExternalLoader.h>
#include <common/logger_useful.h>
#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_ALREADY_EXISTS;
};

class Context;

/// Manages user-defined dictionaries.
class ExternalDictionaries : public ExternalLoader
{
public:
    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalDictionaries(
        std::unique_ptr<IConfigRepository> config_repository,
        Context & context,
        bool throw_on_error);

    /// Forcibly reloads specified dictionary.
    void reloadDictionary(const std::string & name) { reload(name); }

    DictionaryPtr getDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<IDictionaryBase>(getLoadable(name));
    }

    DictionaryPtr tryGetDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<IDictionaryBase>(tryGetLoadable(name));
    }

protected:

    std::shared_ptr<IExternalLoadable> create(const std::string & name, const Configuration & config,
                                              const std::string & config_prefix) override;

    using ExternalLoader::getObjectsMap;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;

private:

    Context & context;
};

}
