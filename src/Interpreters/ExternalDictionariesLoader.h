#pragma once

#include <Dictionaries/IDictionary.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExternalLoader.h>
#include <Common/quoteString.h>
#include <Core/QualifiedTableName.h>

#include <memory>

namespace DB
{

class IExternalLoaderConfigRepository;

/// Manages user-defined dictionaries.
class ExternalDictionariesLoader : public ExternalLoader, WithContext
{
public:
    using DictPtr = std::shared_ptr<const IDictionary>;

    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    explicit ExternalDictionariesLoader(ContextPtr global_context_);

    DictPtr getDictionary(const std::string & dictionary_name, ContextPtr context) const;

    DictPtr tryGetDictionary(const std::string & dictionary_name, ContextPtr context) const;

    void reloadDictionary(const std::string & dictionary_name, ContextPtr context) const;

    QualifiedTableName qualifyDictionaryNameWithDatabase(const std::string & dictionary_name, ContextPtr context) const;

    DictionaryStructure getDictionaryStructure(const std::string & dictionary_name, ContextPtr context) const;

    /// A dictionary name resolved once, so that an access check and the reads after it refer to the same dictionary.
    /// The reads go by `key`, which for a dictionary in an `Atomic` database is its UUID and does not change on rename.
    /// For an XML dictionary or one in an `Ordinary` database the key is the name.
    struct ResolvedDictionary
    {
        /// The name as the query wrote it.
        std::string name;
        /// The key the loader stores the dictionary under, empty if there is no such dictionary.
        std::string key;
        /// The ID to check access rights with, taken from the loaded dictionary if there is one, otherwise from its
        /// configuration. For a dictionary that does not exist, the ID it would have, so that a user without access
        /// gets the access error and cannot tell a missing dictionary from an existing one.
        StorageID id;

        /// Throws if there is no such dictionary.
        [[nodiscard]] const std::string & getKey() const;
    };

    /// Resolves the name without loading the dictionary, so that access can be checked before loading.
    [[nodiscard]] ResolvedDictionary resolveDictionary(const std::string & dictionary_name, ContextPtr query_context) const;

    /// The same, resolving an unqualified name against the given database rather than the current database of the query.
    [[nodiscard]] ResolvedDictionary resolveDictionary(const std::string & dictionary_name, const std::string & current_database_name, ContextPtr query_context) const;

    /// The same, and checks that the user of `query_context` has `dictGet` on the dictionary.
    [[nodiscard]] ResolvedDictionary resolveDictionaryCheckingAccess(const std::string & dictionary_name, ContextPtr query_context) const;

    [[nodiscard]] DictPtr getDictionary(const ResolvedDictionary & dictionary, ContextPtr local_context) const;
    [[nodiscard]] DictionaryStructure getDictionaryStructure(const ResolvedDictionary & dictionary) const;

    /// Reloads all previously tried-to-load dictionaries in topological order
    /// based on their source dependencies (dictionaries sourcing from other dictionaries).
    /// Dictionaries within the same dependency level are reloaded in parallel.
    void reloadAllTriedToLoadInOrder() const;

    static DictionaryStructure getDictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config = "dictionary");

    static DictionaryStructure getDictionaryStructure(const ObjectConfig & config);

    static void resetAll();

protected:
    LoadableMutablePtr createObject(const std::string & name, const Poco::Util::AbstractConfiguration & config,
                                    const std::string & key_in_config, const std::string & repository_name) const override;

    bool doesConfigChangeRequiresReloadingObject(const Poco::Util::AbstractConfiguration & old_config, const String & old_key_in_config,
                                                 const Poco::Util::AbstractConfiguration & new_config, const String & new_key_in_config) const override;

    void updateObjectFromConfigWithoutReloading(
        IExternalLoadable & object, const Poco::Util::AbstractConfiguration & config, const String & key_in_config) const override;

    std::string resolveDictionaryName(const std::string & dictionary_name, const std::string & current_database_name) const;

    /// Both readings of one resolution: the key the loader stores the dictionary under, and the name as a query refers
    /// to it with the database filled in.
    struct ResolvedDictionaryName
    {
        std::string key;
        QualifiedTableName qualified_name;
    };

    /// The same as `resolveDictionaryName`, but the key may name no dictionary.
    ResolvedDictionaryName resolveDictionaryNameUnchecked(const std::string & dictionary_name, const std::string & current_database_name) const;

    DictPtr loadDictionary(const std::string & key, ContextPtr local_context) const;
    DictionaryStructure getDictionaryStructureByKey(const std::string & key, const std::string & dictionary_name) const;

    /// Try convert qualified dictionary name to persistent UUID
    std::string resolveDictionaryNameFromDatabaseCatalog(const std::string & name, const std::string & current_database_name) const;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;
    friend class TableFunctionDictionary;
};

}
