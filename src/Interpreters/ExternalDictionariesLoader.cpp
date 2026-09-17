#include <Core/Settings.h>
#include <Core/SettingsFields.h>
#include <Core/UUID.h>
#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/IDatabase.h>
#include <Databases/UDT/AuthorityStorageOperationGate.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalLoaderDictionaryStorageConfigRepository.h>
#include <Interpreters/ProcessList.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <memory>
#include <string_view>
#include <tuple>

#include "config.h"

#if USE_MYSQL
#include <mysqlxx/PoolFactory.h>
#endif


namespace DB
{
namespace Setting
{
extern const SettingsBool log_queries;
}

namespace ErrorCodes
{
extern const int ABORTED;
extern const int BAD_ARGUMENTS;
extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

namespace
{

enum class MissingDDLDictionaryStoragePolicy : UInt8
{
    Reject,
    AllowReservedUUIDDuringBootstrap,
};

struct MappedDDLDictionaryStorage
{
    std::shared_ptr<StorageDictionary> storage;
    StorageMetadataPtr metadata;
};

struct DictionaryAuthorityReadLifetime
{
    UDT::AuthorityStorageReadContinuationEvidence::Ptr evidence;
    ExternalDictionariesLoader::DictPtr dictionary;
};

constexpr size_t max_dictionary_read_admission_attempts = 3;

ExternalDictionariesLoader::DictPtr retainDictionaryAuthorityReadEvidence(
    ExternalDictionariesLoader::DictPtr dictionary, UDT::AuthorityStorageReadContinuationEvidence::Ptr evidence)
{
    if (!dictionary || !evidence)
        return dictionary;

    auto lifetime = std::make_shared<DictionaryAuthorityReadLifetime>(
        DictionaryAuthorityReadLifetime{.evidence = std::move(evidence), .dictionary = std::move(dictionary)});
    return ExternalDictionariesLoader::DictPtr(lifetime, lifetime->dictionary.get());
}

bool hasSameDictionaryImage(const ExternalLoader::LoadResult & lhs, const ExternalLoader::LoadResult & rhs)
{
    return lhs.object.get() == rhs.object.get() && lhs.config == rhs.config;
}

bool tryParseNonNilUUID(std::string_view text, UUID & uuid)
{
    if (text.empty() || !tryParseUUID({reinterpret_cast<const UInt8 *>(text.data()), text.size()}, uuid) || uuid == UUIDHelpers::Nil)
        return false;
    return true;
}

/// A StorageDictionary-backed DDL repository is named by, and contains one
/// path named by, the dictionary UUID. XML repositories have an empty name;
/// Ordinary repositories use a qualified name; temporary repositories are
/// filtered by the ObjectConfig overload below.
bool tryGetDDLDictionaryRepositoryUUID(std::string_view repository_name, std::string_view config_file_path, UUID & uuid)
{
    return repository_name == config_file_path && tryParseNonNilUUID(repository_name, uuid);
}

std::optional<MappedDDLDictionaryStorage> resolveMappedDDLDictionaryStorage(
    const ExternalDictionariesLoader & loader,
    const String & loader_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & key_in_config,
    const String & repository_name,
    const String & config_file_path,
    ContextPtr context,
    MissingDDLDictionaryStoragePolicy missing_storage_policy)
{
    UUID repository_uuid = UUIDHelpers::Nil;
    if (!tryGetDDLDictionaryRepositoryUUID(repository_name, config_file_path, repository_uuid))
        return std::nullopt;

    const auto configured_id = StorageID::fromDictionaryConfig(config, key_in_config);
    if (configured_id.uuid != repository_uuid)
    {
        throw Exception(
            ErrorCodes::ABORTED, "DDL dictionary '{}' configuration UUID does not match its storage repository UUID", loader_name);
    }

    auto & catalog = DatabaseCatalog::instance();
    auto [database, table] = catalog.tryGetByUUID(repository_uuid);
    if (!table)
    {
        /// StorageFactory may synchronously load a non-lazy dictionary while
        /// its exact StorageDictionary repository is active and DatabaseAtomic
        /// still owns the unpublished UUID reservation. Requiring both facts
        /// distinguishes bootstrap from the empty mappings retained after
        /// DETACH/drop.
        if (missing_storage_policy == MissingDDLDictionaryStoragePolicy::AllowReservedUUIDDuringBootstrap
            && catalog.hasUUIDMapping(repository_uuid))
        {
            auto repository_lease
                = ExternalLoaderDictionaryStorageConfigRepository::acquireActiveUUIDRepository(loader, toString(repository_uuid));
            /// The UUID mapping may have become live between the first lookup
            /// and the reservation check. Re-resolve it before granting the
            /// only no-storage exception; a concurrently published table must
            /// go through its exact mapped-image gate below. Rechecking the
            /// retained repository lease also makes detach/drop the
            /// fail-closed side of the empty-mapping race.
            std::tie(database, table) = catalog.tryGetByUUID(repository_uuid);
            if (repository_lease && !database && !table)
            {
                const auto * reserved_storage = repository_lease.getStorage();
                const auto reserved_id = reserved_storage ? reserved_storage->getStorageID() : StorageID::createEmpty();
                const auto reserved_configuration = reserved_storage ? reserved_storage->getConfiguration() : LoadablesConfigurationPtr{};
                auto reserved_metadata_handle
                    = reserved_storage ? reserved_storage->getInMemoryMetadataPtr(context, false) : StorageMetadataHandle{};
                StorageMetadataPtr reserved_metadata = reserved_metadata_handle;
                if (!reserved_storage || reserved_id.uuid != repository_uuid || reserved_id.database_name != configured_id.database_name
                    || reserved_id.table_name != configured_id.table_name || reserved_configuration.get() != std::addressof(config)
                    || reserved_storage->getName() != "Dictionary")
                {
                    throw Exception(
                        ErrorCodes::ABORTED,
                        "UUID-backed DDL dictionary '{}' bootstrap repository has a foreign storage owner",
                        loader_name);
                }

                if (reserved_metadata && reserved_metadata->getBoundUDTReferences())
                {
                    reserved_metadata->validateBoundUDTReferences();
                    const auto reserved_database = catalog.tryGetDatabase(reserved_id.database_name);
                    const auto reserved_atomic = std::dynamic_pointer_cast<DatabaseAtomic>(reserved_database);
                    const auto & object = reserved_metadata->getBoundUDTReferences()->getObject();
                    if (!reserved_atomic || object.kind != UDT::SchemaObjectKind::Dictionary
                        || object.database_uuid != reserved_atomic->getUUID() || object.object_uuid != reserved_id.uuid)
                    {
                        throw Exception(
                            ErrorCodes::ABORTED,
                            "UUID-backed DDL dictionary '{}' bootstrap metadata has a foreign authority identity",
                            loader_name);
                    }
                }
                return std::nullopt;
            }
        }

        if (!table)
        {
            throw Exception(
                ErrorCodes::ABORTED,
                "UUID-backed DDL dictionary '{}' has no live StorageDictionary for its authority boundary",
                loader_name);
        }
    }

    auto metadata_handle = table->getInMemoryMetadataPtr(context, false);
    StorageMetadataPtr metadata = metadata_handle;
    if (!metadata || !metadata->getBoundUDTReferences())
        return std::nullopt;

    const auto storage = std::dynamic_pointer_cast<StorageDictionary>(table);
    const auto live_id = table->getStorageID();
    const auto live_configuration = storage ? storage->getConfiguration() : LoadablesConfigurationPtr{};
    const auto named_database = live_id.database_name.empty() ? DatabasePtr{} : catalog.tryGetDatabase(live_id.database_name);
    if (!database || database != named_database || !storage || table->getName() != "Dictionary" || loader_name != toString(repository_uuid)
        || live_id.uuid != configured_id.uuid || live_id.database_name != configured_id.database_name
        || live_id.table_name != configured_id.table_name || live_configuration.get() != std::addressof(config))
    {
        throw Exception(
            ErrorCodes::ABORTED,
            "UUID-backed DDL dictionary '{}' does not resolve to its exact live mapped StorageDictionary image",
            loader_name);
    }

    return MappedDDLDictionaryStorage{.storage = storage, .metadata = std::move(metadata)};
}

std::optional<MappedDDLDictionaryStorage> resolveMappedDDLDictionaryStorage(
    const ExternalDictionariesLoader & loader,
    const String & loader_name,
    const ExternalLoader::ObjectConfig & object_config,
    ContextPtr context,
    MissingDDLDictionaryStoragePolicy missing_storage_policy)
{
    if (object_config.from_temp_repository)
        return std::nullopt;

    return resolveMappedDDLDictionaryStorage(
        loader,
        loader_name,
        *object_config.config,
        object_config.key_in_config,
        object_config.repository_name,
        object_config.path,
        std::move(context),
        missing_storage_policy);
}

UDT::AuthorityStorageReadContinuationEvidence::Ptr acquireMappedDDLDictionaryReadAdmission(
    const ExternalDictionariesLoader & loader,
    const String & loader_name,
    const ExternalLoader::ObjectConfig & object_config,
    ContextPtr context)
{
    auto mapped = resolveMappedDDLDictionaryStorage(loader, loader_name, object_config, context, MissingDDLDictionaryStoragePolicy::Reject);
    if (!mapped)
        return {};
    return UDT::acquireAuthorityStorageReadContinuationEvidence(*mapped->storage, mapped->metadata);
}

std::optional<ExternalDictionariesLoader::DictPtr> tryAcquireStableDictionaryRead(
    const ExternalDictionariesLoader & loader,
    const String & loader_name,
    const ExternalLoader::LoadResult & loaded_result,
    ContextPtr context)
{
    auto dictionary = std::static_pointer_cast<const IDictionary>(loaded_result.object);
    if (!dictionary)
        return ExternalDictionariesLoader::DictPtr{};

    try
    {
        auto authority_read_evidence = loaded_result.config
            ? acquireMappedDDLDictionaryReadAdmission(loader, loader_name, *loaded_result.config, context)
            : UDT::AuthorityStorageReadContinuationEvidence::Ptr{};

        /// Configuration repositories may republish an equivalent ObjectConfig
        /// without reloading the dictionary object. The admission must still be
        /// paired with one exact published object/config image, so let the caller
        /// retry the whole acquisition when either pointer changed.
        if (!hasSameDictionaryImage(loaded_result, loader.getLoadResult(loader_name)))
            return std::nullopt;

        return retainDictionaryAuthorityReadEvidence(std::move(dictionary), std::move(authority_read_evidence));
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::ABORTED || hasSameDictionaryImage(loaded_result, loader.getLoadResult(loader_name)))
            throw;

        /// An ABORTED result for a superseded image is a publication race. Do
        /// not hide an ABORTED result for the still-current image: that signals
        /// a real quarantine, identity, or authority-boundary violation.
        return std::nullopt;
    }
}

void assertMappedDDLDictionaryActivationAllowed(
    const ExternalDictionariesLoader & loader,
    const String & loader_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & key_in_config,
    const String & repository_name,
    const String & config_file_path,
    ContextPtr context,
    UDT::AuthorityQuarantineOperationKind kind,
    MissingDDLDictionaryStoragePolicy missing_storage_policy)
{
    auto mapped = resolveMappedDDLDictionaryStorage(
        loader, loader_name, config, key_in_config, repository_name, config_file_path, context, missing_storage_policy);
    if (mapped)
        UDT::assertAuthorityStorageNewOperationAllowed(*mapped->storage, mapped->metadata, kind);
}

void assertMappedDDLDictionaryActivationAllowed(
    const ExternalDictionariesLoader & loader,
    const String & loader_name,
    const ExternalLoader::ObjectConfig & object_config,
    ContextPtr context,
    UDT::AuthorityQuarantineOperationKind kind,
    MissingDDLDictionaryStoragePolicy missing_storage_policy = MissingDDLDictionaryStoragePolicy::Reject)
{
    if (object_config.from_temp_repository)
        return;

    assertMappedDDLDictionaryActivationAllowed(
        loader,
        loader_name,
        *object_config.config,
        object_config.key_in_config,
        object_config.repository_name,
        object_config.path,
        std::move(context),
        kind,
        missing_storage_policy);
}

}

/// Must not acquire Context lock in constructor to avoid possibility of deadlocks.
ExternalDictionariesLoader::ExternalDictionariesLoader(ContextPtr global_context_)
    : ExternalLoader("external dictionary", getLogger("ExternalDictionariesLoader"))
    , WithContext(global_context_)
{
    setConfigSettings({"dictionary", "name", "database", "uuid"});
    enableAsyncLoading(true);
    if (getContext()->getApplicationType() == Context::ApplicationType::SERVER)
        enablePeriodicUpdates(true);
}

ExternalLoader::LoadableMutablePtr ExternalDictionariesLoader::createObject(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & key_in_config,
    const std::string & repository_name,
    const std::string & /* config_file_path */) const
{
    /// For dictionaries from databases (created with DDL queries) we have to perform
    /// additional checks, so we identify them here.
    bool created_from_ddl = !repository_name.empty();
    return DictionaryFactory::instance().create(name, config, key_in_config, getContext(), created_from_ddl);
}

void ExternalDictionariesLoader::beforeCreateOrCloneObject(
    const String & name, const ObjectConfig & config, const LoadablePtr & previous_version) const
{
    /// This is the common pre-work boundary for both first construction and
    /// ExternalLoader's periodic clone path. First construction alone may run
    /// while StorageFactory still owns an unpublished Atomic UUID reservation;
    /// every reload requires the exact live StorageDictionary image. Temporary
    /// repositories are filtered by the ObjectConfig overload.
    assertMappedDDLDictionaryActivationAllowed(
        *this,
        name,
        config,
        getContext(),
        previous_version ? UDT::AuthorityQuarantineOperationKind::DDL : UDT::AuthorityQuarantineOperationKind::Attach,
        previous_version ? MissingDDLDictionaryStoragePolicy::Reject : MissingDDLDictionaryStoragePolicy::AllowReservedUUIDDuringBootstrap);
}

bool ExternalDictionariesLoader::doesConfigChangeRequiresReloadingObject(
    const Poco::Util::AbstractConfiguration & old_config,
    const String & old_key_in_config,
    const Poco::Util::AbstractConfiguration & new_config,
    const String & new_key_in_config) const
{
    std::unordered_set<std::string_view> ignore_keys;
    ignore_keys.insert("comment"); /// We always can change the comment without reloading a dictionary.

    /// If the database is atomic then a dictionary can be renamed without reloading.
    if (!old_config.getString(old_key_in_config + ".uuid", "").empty() && !new_config.getString(new_key_in_config + ".uuid", "").empty())
    {
        ignore_keys.insert("name");
        ignore_keys.insert("database");
    }

    return !isSameConfiguration(old_config, old_key_in_config, new_config, new_key_in_config, ignore_keys);
}

std::optional<bool>
ExternalDictionariesLoader::isObjectLazy(const Poco::Util::AbstractConfiguration & config, const String & key_in_config) const
{
    SettingFieldBoolAuto lazy_load;
    lazy_load.parseFromString(config.getString(key_in_config + ".settings.dictionary_lazy_load", SettingFieldBoolAuto::keyword));
    return lazy_load.valueOrNullopt();
}

void ExternalDictionariesLoader::updateObjectFromConfigWithoutReloading(
    IExternalLoadable & object, const Poco::Util::AbstractConfiguration & config, const String & key_in_config) const
{
    IDictionary & dict = static_cast<IDictionary &>(object);

    auto new_dictionary_id = StorageID::fromDictionaryConfig(config, key_in_config);
    auto old_dictionary_id = dict.getDictionaryID();
    if ((new_dictionary_id.table_name != old_dictionary_id.table_name)
        || (new_dictionary_id.database_name != old_dictionary_id.database_name))
    {
        /// We can update the dictionary ID without reloading only if it's in the atomic database.
        if ((new_dictionary_id.uuid == old_dictionary_id.uuid) && (new_dictionary_id.uuid != UUIDHelpers::Nil))
            dict.updateDictionaryID(new_dictionary_id);
    }

    dict.updateDictionaryComment(config.getString(key_in_config + ".comment", ""));
}

ExternalDictionariesLoader::DictPtr
ExternalDictionariesLoader::getDictionary(const std::string & dictionary_name, ContextPtr local_context) const
{
    std::string resolved_dictionary_name = resolveDictionaryName(dictionary_name, local_context->getCurrentDatabase());

    /// Check if we have a cancellable query context
    QueryStatusPtr process_list_element;
    if (local_context->hasQueryContext())
        process_list_element = local_context->getProcessListElement();

    for (size_t attempt = 0; attempt < max_dictionary_read_admission_attempts; ++attempt)
    {
        LoadResult loaded_result;
        if (process_list_element)
        {
            /// Wait with periodic cancellation checks
            while (true)
            {
                auto result = tryLoad<LoadResult>(resolved_dictionary_name, /* timeout = */ std::chrono::milliseconds(1000));
                if (result.object)
                {
                    loaded_result = std::move(result);
                    break;
                }
                /// If loading has terminally failed, call load() to throw the proper error
                if (result.status != Status::LOADING && result.status != Status::NOT_LOADED)
                {
                    loaded_result = load<LoadResult>(resolved_dictionary_name);
                    break;
                }
                /// Check if the query was cancelled while we were waiting
                process_list_element->checkTimeLimit();
            }
        }
        else
        {
            loaded_result = load<LoadResult>(resolved_dictionary_name);
        }

        auto stable_dictionary = tryAcquireStableDictionaryRead(*this, resolved_dictionary_name, loaded_result, local_context);
        if (stable_dictionary)
        {
            auto dictionary = std::move(*stable_dictionary);
            if (local_context->hasQueryContext() && local_context->getSettingsRef()[Setting::log_queries])
                local_context->getQueryContext()->addQueryFactoriesInfo(
                    Context::QueryLogFactories::Dictionary, dictionary->getQualifiedName());

            return dictionary;
        }

        if (process_list_element)
            process_list_element->checkTimeLimit();
    }

    throw Exception(
        ErrorCodes::ABORTED,
        "Dictionary '{}' kept changing while acquiring its mapped UDT read admission after {} attempts",
        resolved_dictionary_name,
        max_dictionary_read_admission_attempts);
}

ExternalDictionariesLoader::DictPtr
ExternalDictionariesLoader::tryGetDictionary(const std::string & dictionary_name, ContextPtr local_context) const
{
    std::string resolved_dictionary_name = resolveDictionaryName(dictionary_name, local_context->getCurrentDatabase());
    for (size_t attempt = 0; attempt < max_dictionary_read_admission_attempts; ++attempt)
    {
        auto loaded_result = tryLoad<LoadResult>(resolved_dictionary_name);
        auto stable_dictionary = tryAcquireStableDictionaryRead(*this, resolved_dictionary_name, loaded_result, local_context);
        if (!stable_dictionary)
            continue;

        auto dictionary = std::move(*stable_dictionary);
        if (!dictionary)
            return {};

        if (local_context->hasQueryContext() && local_context->getSettingsRef()[Setting::log_queries])
            local_context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Dictionary, dictionary->getQualifiedName());

        return dictionary;
    }

    throw Exception(
        ErrorCodes::ABORTED,
        "Dictionary '{}' kept changing while acquiring its mapped UDT read admission after {} attempts",
        resolved_dictionary_name,
        max_dictionary_read_admission_attempts);
}


void ExternalDictionariesLoader::reloadDictionary(const std::string & dictionary_name, ContextPtr local_context) const
{
    std::string resolved_dictionary_name = resolveDictionaryName(dictionary_name, local_context->getCurrentDatabase());
    const auto load_result_before_reload = getLoadResult(resolved_dictionary_name);
    if (load_result_before_reload.config)
    {
        assertMappedDDLDictionaryActivationAllowed(
            *this, resolved_dictionary_name, *load_result_before_reload.config, local_context, UDT::AuthorityQuarantineOperationKind::DDL);
    }
    loadOrReload(resolved_dictionary_name);
}

bool ExternalDictionariesLoader::unloadDictionary(const std::string & dictionary_name, ContextPtr local_context) const
{
    std::string resolved_dictionary_name = resolveDictionaryName(dictionary_name, local_context->getCurrentDatabase());
    return unload(resolved_dictionary_name);
}

void ExternalDictionariesLoader::unloadAllDictionaries() const
{
    unloadAll();
}

DictionaryStructure ExternalDictionariesLoader::getDictionaryStructure(const std::string & dictionary_name, ContextPtr query_context) const
{
    std::string resolved_name = resolveDictionaryName(dictionary_name, query_context->getCurrentDatabase());

    auto load_result = getLoadResult(resolved_name);

    if (load_result.object)
    {
        const auto dictionary = std::static_pointer_cast<const IDictionary>(load_result.object);
        return dictionary->getStructure();
    }

    if (!load_result.config)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary {} config not found", backQuote(dictionary_name));

    return ExternalDictionariesLoader::getDictionaryStructure(*load_result.config);
}

std::string ExternalDictionariesLoader::getDictionaryLayoutType(const std::string & dictionary_name, ContextPtr query_context) const
{
    std::string resolved_name = resolveDictionaryName(dictionary_name, query_context->getCurrentDatabase());

    auto load_result = getLoadResult(resolved_name);
    if (!load_result.config)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary {} config not found", backQuote(dictionary_name));

    Poco::Util::AbstractConfiguration::Keys layout_keys;
    load_result.config->config->keys(load_result.config->key_in_config + ".layout", layout_keys);

    if (layout_keys.size() != 1)
        throw Exception(
            ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
            "Dictionary {}: element dictionary.layout should have exactly one child element",
            backQuote(dictionary_name));

    return layout_keys.front();
}

void ExternalDictionariesLoader::assertDictionaryStructureExists(const std::string & dictionary_name, ContextPtr query_context) const
{
    getDictionaryStructure(dictionary_name, query_context);
}

QualifiedTableName
ExternalDictionariesLoader::qualifyDictionaryNameWithDatabase(const std::string & dictionary_name, ContextPtr query_context) const
{
    auto qualified_name = QualifiedTableName::tryParseFromString(dictionary_name);
    if (!qualified_name)
    {
        QualifiedTableName qualified_dictionary_name;
        qualified_dictionary_name.table = dictionary_name;
        return qualified_dictionary_name;
    }

    /// If dictionary was not qualified with database name, try to resolve dictionary as xml dictionary.
    if (qualified_name->database.empty() && !has(qualified_name->table))
    {
        std::string current_database_name = query_context->getCurrentDatabase();
        std::string resolved_name = resolveDictionaryNameFromDatabaseCatalog(dictionary_name, current_database_name);

        /// If after qualify dictionary_name with default_database_name we find it, add default_database to qualified name.
        if (has(resolved_name))
            qualified_name->database = std::move(current_database_name);
    }

    return *qualified_name;
}

std::string
ExternalDictionariesLoader::resolveDictionaryName(const std::string & dictionary_name, const std::string & current_database_name) const
{
    if (has(dictionary_name))
        return dictionary_name;

    std::string resolved_name = resolveDictionaryNameFromDatabaseCatalog(dictionary_name, current_database_name);

    if (has(resolved_name))
        return resolved_name;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary ({}) not found", backQuote(dictionary_name));
}

std::string ExternalDictionariesLoader::resolveDictionaryNameFromDatabaseCatalog(
    const std::string & name, const std::string & current_database_name) const
{
    /// If it's dictionary from Atomic database, then we need to convert qualified name to UUID.
    /// Try to split name and get id from associated StorageDictionary.
    /// If something went wrong, return name as is.

    String res = name;

    auto qualified_name = QualifiedTableName::tryParseFromString(name);
    if (!qualified_name)
        return res;

    if (qualified_name->database.empty())
    {
        /// Either database name is not specified and we should use current one
        /// or it's an XML dictionary.
        bool is_xml_dictionary = has(name);
        if (is_xml_dictionary)
            return res;

        qualified_name->database = current_database_name;
        res = current_database_name + '.' + name;
    }

    auto [db, table] = DatabaseCatalog::instance().tryGetDatabaseAndTable(
        {qualified_name->database, qualified_name->table}, const_pointer_cast<Context>(getContext()));

    if (!db)
        return res;
    chassert(table);

    if (db->getUUID() == UUIDHelpers::Nil)
        return res;
    if (table->getName() != "Dictionary")
        return res;

    return toString(table->getStorageID().uuid);
}

DictionaryStructure
ExternalDictionariesLoader::getDictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config)
{
    return DictionaryStructure(config, key_in_config);
}

DictionaryStructure ExternalDictionariesLoader::getDictionaryStructure(const ObjectConfig & config)
{
    return getDictionaryStructure(*config.config, config.key_in_config);
}


void ExternalDictionariesLoader::reloadAllTriedToLoadInOrder() const
{
    auto all_names = getAllTriedToLoadNames();
    if (all_names.empty())
        return;

    /// Create a filter matching all previously tried-to-load names
    std::unordered_set<String> names_set(all_names.begin(), all_names.end());

    /// Get load results with configs to extract dependency information
    auto load_results = getLoadResults<LoadResults>([&names_set](const String & name) { return names_set.contains(name); });

    /// Build mapping: QualifiedTableName -> loader_name
    /// This allows resolving source table references back to loader names.
    std::unordered_map<QualifiedTableName, String> qualified_name_to_loader;

    struct DictInfo
    {
        String loader_name;
        String database;
        DictionaryConfigurationPtr config;
    };

    std::vector<DictInfo> dict_infos;
    dict_infos.reserve(load_results.size());

    for (const auto & result : load_results)
    {
        if (!result.config)
            continue;

        auto storage_id = StorageID::fromDictionaryConfig(*result.config->config, result.config->key_in_config);
        QualifiedTableName qname{storage_id.database_name, storage_id.table_name};
        qualified_name_to_loader[qname] = result.name;

        dict_infos.push_back({result.name, storage_id.database_name, result.config->config});
    }

    /// Build dependency graph using reverse adjacency list for Kahn's algorithm.
    /// dependents[A] contains all dictionaries that depend on A (i.e., source from A).
    std::unordered_map<String, Strings> dependents;
    std::unordered_map<String, size_t> in_degree;

    for (const auto & name : all_names)
        in_degree[name] = 0;

    for (const auto & info : dict_infos)
    {
        DictionaryConfigurationPtr config_copy = info.config;
        std::optional<ClickHouseDictionarySourceInfo> source_info;
        try
        {
            source_info = getInfoIfClickHouseDictionarySource(config_copy, getContext());
        }
        catch (...)
        {
            LOG_WARNING(
                getLogger("ExternalDictionariesLoader"),
                "Failed to parse source config for dictionary '{}', skipping dependency resolution: {}",
                info.loader_name,
                getCurrentExceptionMessage(false));
            continue;
        }

        if (!source_info || !source_info->is_local || source_info->table_name.table.empty())
            continue;

        /// Resolve source table to a loader name.
        /// If source database is empty, try the dictionary's own database first.
        String dep_name;
        if (source_info->table_name.database.empty() && !info.database.empty())
        {
            QualifiedTableName with_own_db{info.database, source_info->table_name.table};
            auto it = qualified_name_to_loader.find(with_own_db);
            if (it != qualified_name_to_loader.end())
                dep_name = it->second;
        }

        /// Try with the source database as-is (either explicitly specified or empty for XML dicts)
        if (dep_name.empty())
        {
            auto it = qualified_name_to_loader.find(source_info->table_name);
            if (it != qualified_name_to_loader.end())
                dep_name = it->second;
        }

        if (!dep_name.empty() && dep_name != info.loader_name)
        {
            dependents[dep_name].push_back(info.loader_name);
            in_degree[info.loader_name]++;
        }
    }

    /// Kahn's algorithm: produce levels of dictionaries with equal depth
    std::vector<Strings> levels;

    Strings current_level;
    for (const auto & name : all_names)
    {
        if (in_degree[name] == 0)
            current_level.push_back(name);
    }

    std::unordered_set<String> processed;
    while (!current_level.empty())
    {
        levels.push_back(current_level);
        for (const auto & name : current_level)
            processed.insert(name);

        Strings next_level;
        for (const auto & name : current_level)
        {
            auto it = dependents.find(name);
            if (it == dependents.end())
                continue;
            for (const auto & dep : it->second)
            {
                if (--in_degree[dep] == 0)
                    next_level.push_back(dep);
            }
        }

        current_level = std::move(next_level);
    }

    /// Handle circular dependencies: add remaining unprocessed nodes to a final level
    Strings circular;
    for (const auto & name : all_names)
    {
        if (!processed.contains(name))
            circular.push_back(name);
    }

    if (!circular.empty())
    {
        auto logger = getLogger("ExternalDictionariesLoader");
        for (const auto & name : circular)
            LOG_WARNING(logger, "Dictionary '{}' is part of a circular dependency chain, will be reloaded last", name);
        levels.push_back(std::move(circular));
    }

    /// Reload level by level. Errors at one level don't block subsequent levels.
    std::exception_ptr first_exception;
    for (const auto & level : levels)
    {
        std::unordered_set<String> level_set(level.begin(), level.end());
        auto level_filter = [&level_set](const String & name) { return level_set.contains(name); };

        try
        {
            /// Reject the whole dependency level before any of its reloads can
            /// start if one mapped DDL dictionary is quarantined. The config
            /// repository repeats this gate against the exact storage image
            /// when loadOrReload refreshes all repository configurations.
            for (const auto & result : load_results)
            {
                if (result.config && level_set.contains(result.name))
                {
                    assertMappedDDLDictionaryActivationAllowed(
                        *this, result.name, *result.config, getContext(), UDT::AuthorityQuarantineOperationKind::DDL);
                }
            }
            loadOrReload<LoadResults>(level_filter);
        }
        catch (...)
        {
            if (!first_exception)
                first_exception = std::current_exception();
        }
    }

    if (first_exception)
        std::rethrow_exception(first_exception);
}


void ExternalDictionariesLoader::resetAll()
{
#if USE_MYSQL
    mysqlxx::PoolFactory::instance().reset();
#endif
}

}
