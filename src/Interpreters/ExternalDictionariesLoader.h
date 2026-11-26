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

    [[nodiscard]] DictPtr getDictionary(const std::string & dictionary_name, ContextPtr context) const;

    [[nodiscard]] DictPtr tryGetDictionary(const std::string & dictionary_name, ContextPtr context) const;

    void reloadDictionary(const std::string & dictionary_name, ContextPtr context) const;

    void reloadDictionary(const QualifiedTableName & dictionary_name, ContextPtr local_context) const;

    bool unloadDictionary(const std::string & dictionary_name, ContextPtr context) const; /// NOLINT(modernize-use-nodiscard): often called only for its side effect

    bool unloadDictionary(const QualifiedTableName & dictionary_name, ContextPtr local_context) const; /// NOLINT(modernize-use-nodiscard): often called only for its side effect

    void unloadAllDictionaries() const;

    /// Stops the background threads of the loaded dictionaries and joins the loading threads.
    void shutdownDictionaries();

    [[nodiscard]] QualifiedTableName qualifyDictionaryNameWithDatabase(const std::string & dictionary_name, ContextPtr context) const;

    /// Translates the name a query uses (`db.dict`, or `dict` against the current database) into the key the loader
    /// stores the dictionary under (the UUID for a DDL dictionary, the name for an XML one), recording the query's use of
    /// the dictionary (`DistributedPlanLocalObject`) on the way. Every consumer that names a dictionary goes through one of these.
    [[nodiscard]] std::string resolveDictionaryName(const std::string & dictionary_name, ContextPtr local_context) const;
    [[nodiscard]] std::string resolveDictionaryName(const QualifiedTableName & dictionary_name, ContextPtr local_context) const;

    /// The same, but resolving an unqualified name against the given database rather than the
    /// current database of a context.
    [[nodiscard]] QualifiedTableName qualifyDictionaryNameWithDatabase(const std::string & dictionary_name, const std::string & current_database_name) const;

    [[nodiscard]] DictionaryStructure getDictionaryStructure(const std::string & dictionary_name, ContextPtr context) const;

    /// The layout type of the dictionary as written in its definition, e.g. "hashed" or "naive_bayes".
    /// Read from the dictionary configuration, so it works without loading the dictionary. Throws for
    /// a definition whose layout element does not have exactly one child, like loading it would.
    [[nodiscard]] std::string getDictionaryLayoutType(const std::string & dictionary_name, ContextPtr context) const;

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
    [[nodiscard]] std::string getDictionaryLayoutType(const ResolvedDictionary & dictionary) const;

    /// Reloads all previously tried-to-load dictionaries in topological order
    /// based on their source dependencies (dictionaries sourcing from other dictionaries).
    /// Dictionaries within the same dependency level are reloaded in parallel.
    void reloadAllTriedToLoadInOrder() const;

    static DictionaryStructure getDictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config = "dictionary");

    static DictionaryStructure getDictionaryStructure(const ObjectConfig & config);

    [[nodiscard]] std::optional<bool> getObjectLazyLoadOverride(const Poco::Util::AbstractConfiguration & config, const String & key_in_config) const override;

    static void resetAll();

protected:
    [[nodiscard]] LoadableMutablePtr createObject(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & key_in_config,
        const std::string & repository_name,
        const std::string & config_file_path) const override;

    [[nodiscard]] bool doesConfigChangeRequiresReloadingObject(const Poco::Util::AbstractConfiguration & old_config, const String & old_key_in_config,
                                                 const Poco::Util::AbstractConfiguration & new_config, const String & new_key_in_config) const override;

    void updateObjectFromConfigWithoutReloading(
        IExternalLoadable & object, const Poco::Util::AbstractConfiguration & config, const String & key_in_config) const override;

    /// Both readings of one resolution: the key the loader stores the dictionary under, and the name as a query refers
    /// to it with the database filled in, which the context overloads record.
    struct ResolvedDictionaryName
    {
        std::string key;
        QualifiedTableName qualified_name;
    };

    /// Resolution without a record.
    [[nodiscard]] ResolvedDictionaryName resolveDictionaryName(const std::string & dictionary_name, const std::string & current_database_name) const;

    /// The same, but the key may name no dictionary.
    [[nodiscard]] ResolvedDictionaryName resolveDictionaryNameUnchecked(const std::string & dictionary_name, const std::string & current_database_name) const;

    [[nodiscard]] DictPtr loadDictionary(const std::string & key, ContextPtr local_context) const;
    [[nodiscard]] DictionaryStructure getDictionaryStructureByKey(const std::string & key, const std::string & dictionary_name) const;
    [[nodiscard]] std::string getDictionaryLayoutTypeByKey(const std::string & key, const std::string & dictionary_name) const;

    [[nodiscard]] std::string resolveDictionaryName(const QualifiedTableName & dictionary_name) const;

    /// Try convert qualified dictionary name to persistent UUID
    [[nodiscard]] ResolvedDictionaryName resolveDictionaryNameFromDatabaseCatalog(const std::string & name, const std::string & current_database_name) const;

    [[nodiscard]] std::string resolveDictionaryNameFromDatabaseCatalog(const QualifiedTableName & name) const;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;
    friend class TableFunctionDictionary;
};

}
