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
    extern const int DICTIONARY_ALREADY_EXIST;
};

class Context;

class Dictionaries : public ExternalLoader
{
public:
    using DictPtr = std::shared_ptr<IDictionaryBase>;

    Dictionaries(
        std::unique_ptr<IConfigRepository> config_repository,
        Context & context,
        bool throw_on_error);

    void addDictionary(const String & name, ASTCreateQuery & create)
    {
        std::lock_guard lock{internal_dictionaries_mutex};
        auto it = internal_dictionaries.find(name);
        if (it != internal_dictionaries.end())
            throw Exception("Dictionary " + name + " already exist.", ErrorCodes::DICTIONARY_ALREADY_EXIST);

        internal_dictionaries[name] = DictionaryFactory::instance().create(name, create, context);
    }

    DictPtr getDictionary(const String & name) const
    {
        auto ptr = std::static_pointer_cast<IDictionaryBase>(getLoadable(name));
        if (ptr)
            return ptr;

        std::lock_guard lock{internal_dictionaries_mutex};
        auto it = internal_dictionaries.find(name);
        if (it == internal_dictionaries.end())
            throw Exception(name + " dictionary not found", ErrorCodes::BAD_ARGUMENTS);

        return std::static_pointer_cast<IDictionaryBase>(it->second);
    }

    DictPtr tryGetDictionary(const String & name) const
    {
        auto ptr = std::static_pointer_cast<IDictionaryBase>(tryGetLoadable(name));
        if (ptr)
            return ptr;


        std::lock_guard lock{internal_dictionaries_mutex};
        auto it = internal_dictionaries.find(name);
        if (it == internal_dictionaries.end())
            return {};

        return std::static_pointer_cast<IDictionaryBase>(it->second);
    }

protected:

    std::mutex internal_dictionaries_mutex;
    std::unordered_map<String, DictPtr> internal_dictionaries;

private:
    Context & context;
};

/// Manages user-defined dictionaries.
class ExternalDictionaries : public ExternalLoader
{
public:
    using DictPtr = std::shared_ptr<IDictionaryBase>;

    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalDictionaries(
        std::unique_ptr<IConfigRepository> config_repository,
        Context & context,
        bool throw_on_error);

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
                                              const std::string & config_prefix) override;

    using ExternalLoader::getObjectsMap;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;

private:

    Context & context;
};

}
