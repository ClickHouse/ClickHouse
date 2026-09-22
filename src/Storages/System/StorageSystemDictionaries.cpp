#include <DataTypes/DataTypeArray.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Databases/DatabaseOverlay.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Access/ContextAccess.h>
#include <Storages/System/StorageSystemDictionaries.h>
#include <Storages/VirtualColumnUtils.h>
#include <Columns/ColumnString.h>
#include <Core/Names.h>


#include <ranges>

namespace DB
{
namespace
{

StorageID getDictionaryID(const ExternalLoader::LoadResult & load_result, const std::shared_ptr<const IDictionary> & dict_ptr)
{
    StorageID dict_id = StorageID::createEmpty();

    if (dict_ptr)
        dict_id = dict_ptr->getDictionaryID();
    else if (load_result.config)
        dict_id = StorageID::fromDictionaryConfig(*load_result.config->config, load_result.config->key_in_config);
    else
        dict_id.table_name = load_result.name;

    return dict_id;
}

std::optional<DictionaryStructure>
getDictionaryStructure(const ExternalLoader::LoadResult & load_result, std::exception_ptr & last_exception)
try
{
    return ExternalDictionariesLoader::getDictionaryStructure(*load_result.config);
}
catch (const DB::Exception &)
{
    if (!last_exception)
        last_exception = std::current_exception();
    return {};
}

}

StorageSystemDictionaries::StorageSystemDictionaries(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, columns_description_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_description_);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

ColumnsDescription StorageSystemDictionaries::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries."},
        {"name", std::make_shared<DataTypeString>(), "Dictionary name."},
        {"uuid", std::make_shared<DataTypeUUID>(), "Dictionary UUID."},
        {"status", std::make_shared<DataTypeEnum8>(getExternalLoaderStatusEnumAllPossibleValues()),
            "Dictionary status. Possible values: "
            "NOT_LOADED — Dictionary not currently loaded in memory, "
            "LOADED — Dictionary loaded successfully, "
            "FAILED — Unable to load the dictionary as a result of an error, "
            "LOADING — Dictionary is loading now, "
            "LOADED_AND_RELOADING — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: SYSTEM RELOAD DICTIONARY query, timeout, dictionary config has changed), "
            "FAILED_AND_RELOADING — Could not load the dictionary as a result of an error and is loading now."
        },
        {"origin", std::make_shared<DataTypeString>(), "Path to the configuration file that describes the dictionary."},
        {"type", std::make_shared<DataTypeString>(), "Type of a dictionary allocation. Storing Dictionaries in Memory."},
        {"key.names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Array of key names provided by the dictionary."},
        {"key.types", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Corresponding array of key types provided by the dictionary."},
        {"attribute.names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Array of attribute names provided by the dictionary."},
        {"attribute.types", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Corresponding array of attribute types provided by the dictionary."},
        {"bytes_allocated", std::make_shared<DataTypeUInt64>(), "Amount of RAM allocated for the dictionary."},
        {"hierarchical_index_bytes_allocated", std::make_shared<DataTypeUInt64>(), "Amount of RAM allocated for hierarchical index."},
        {"query_count", std::make_shared<DataTypeUInt64>(), "Number of queries since the dictionary was loaded or since the last successful reboot."},
        {"hit_rate", std::make_shared<DataTypeFloat64>(), "For cache dictionaries, the percentage of uses for which the value was in the cache."},
        {"found_rate", std::make_shared<DataTypeFloat64>(), "The percentage of uses for which the value was found."},
        {"element_count", std::make_shared<DataTypeUInt64>(), "Number of items stored in the dictionary."},
        {"load_factor", std::make_shared<DataTypeFloat64>(), "Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table)."},
        {"source", std::make_shared<DataTypeString>(), "Text describing the data source for the dictionary."},
        {"lifetime_min", std::make_shared<DataTypeUInt64>(), "Minimum lifetime of the dictionary in memory, after which ClickHouse tries to reload the dictionary (if invalidate_query is set, then only if it has changed). Set in seconds."},
        {"lifetime_max", std::make_shared<DataTypeUInt64>(), "Maximum lifetime of the dictionary in memory, after which ClickHouse tries to reload the dictionary (if invalidate_query is set, then only if it has changed). Set in seconds."},
        {"loading_start_time", std::make_shared<DataTypeDateTime>(), "Start time for loading the dictionary."},
        {"last_successful_update_time", std::make_shared<DataTypeDateTime>(), "End time for loading or updating the dictionary. Helps to monitor some troubles with dictionary sources and investigate the causes."},
        {"error_count", std::make_shared<DataTypeUInt64>(), "Number of errors since last successful loading. Helps to monitor some troubles with dictionary sources and investigate the causes."},
        {"loading_duration", std::make_shared<DataTypeFloat32>(), "Duration of a dictionary loading."},
        {"last_exception", std::make_shared<DataTypeString>(), "Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn't be created."},
        {"comment", std::make_shared<DataTypeString>(), "Text of the comment to dictionary."}
    };
}

VirtualColumnsDescription StorageSystemDictionaries::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("key", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    return desc;
}

void StorageSystemDictionaries::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool need_to_check_access_for_dictionaries = !access->isGranted(AccessType::SHOW_DICTIONARIES);

    const auto & external_dictionaries = context->getExternalDictionariesLoader();

    /// A dictionary created in a source database of a read-only `Overlay` facade is registered in
    /// the loader under its source name only, but it is reachable through the facade name as well
    /// (`SHOW CREATE DICTIONARY ov.d`, `EXISTS DICTIONARY ov.d`, `dictGet('ov.d', ...)`). The
    /// listing must agree with those point lookups, so every facade that resolves the name to this
    /// very source contributes a row under the facade name — `SHOW DICTIONARIES FROM ov` is a
    /// query over `system.dictionaries WHERE database = 'ov'`. The facade row follows the
    /// `Overlay` access-control contract: it is shown only when `SHOW DICTIONARIES` is granted on
    /// both the facade name and the source name (the facade must not widen visibility), and the
    /// resolution is fail-closed and never loads the source object.
    std::vector<std::shared_ptr<const DatabaseOverlay>> readonly_facades;
    for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
        if (DatabaseOverlay::isReadonlyFacade(database.get()))
            readonly_facades.push_back(std::static_pointer_cast<const DatabaseOverlay>(database));

    for (const auto & load_result : external_dictionaries.getLoadResults())
    {
        const auto dict_ptr = std::dynamic_pointer_cast<const IDictionary>(load_result.object);

        std::exception_ptr last_exception = load_result.exception;
        auto dict_structure = getDictionaryStructure(load_result, last_exception);

        StorageID dict_id = getDictionaryID(load_result, dict_ptr);

        /// The database names under which this dictionary is listed: its own, and the name of
        /// every read-only `Overlay` facade through which it is visible to the caller.
        std::vector<String> listed_databases;

        String db_or_tag = dict_id.database_name.empty() ? IDictionary::NO_DATABASE_TAG : dict_id.database_name;
        if (!need_to_check_access_for_dictionaries || access->isGranted(AccessType::SHOW_DICTIONARIES, db_or_tag, dict_id.table_name))
            listed_databases.push_back(dict_id.database_name);

        if (!dict_id.database_name.empty())
        {
            for (const auto & facade : readonly_facades)
            {
                if (!facade->usesSourceDatabase(dict_id.database_name))
                    continue;
                /// A name that an earlier source shadows (with a table or a dictionary of its own)
                /// resolves to that source through the facade, so this dictionary is not what
                /// `ov.name` denotes and must not be advertised under the facade name.
                if (facade->tryGetVisibleSourceDatabaseNameNoLoad(dict_id.table_name, context, AccessType::SHOW_DICTIONARIES)
                    == dict_id.database_name)
                    listed_databases.push_back(facade->getDatabaseName());
            }
        }

        for (const auto & listed_database : listed_databases)
        {
            size_t i = 0;
            res_columns[i++]->insert(listed_database);
            res_columns[i++]->insert(dict_id.table_name);
            res_columns[i++]->insert(dict_id.uuid);
            res_columns[i++]->insert(static_cast<Int8>(load_result.status));
            res_columns[i++]->insert(load_result.config ? load_result.config->path : "");

            if (dict_ptr)
                res_columns[i++]->insert(dict_ptr->getTypeName());
            else
                res_columns[i++]->insertDefault();

            if (dict_structure)
            {
                res_columns[i++]->insert(
                    Array{std::from_range_t{}, dict_structure->getKeysNames() | std::views::transform([](auto & name) { return name; })});

                if (dict_structure->id)
                    res_columns[i++]->insert(Array({"UInt64"}));
                else
                    res_columns[i++]->insert(Array{
                        std::from_range_t{}, *dict_structure->key | std::views::transform([](auto & attr) { return attr.type->getName(); })});

                res_columns[i++]->insert(
                    Array{std::from_range_t{}, dict_structure->attributes | std::views::transform([](auto & attr) { return attr.name; })});
                res_columns[i++]->insert(Array{
                    std::from_range_t{}, dict_structure->attributes | std::views::transform([](auto & attr) { return attr.type->getName(); })});
            }
            else
            {
                for (size_t j = 0; j != 4; ++j) // Number of empty fields if dict_structure is null
                    res_columns[i++]->insertDefault();
            }

            if (dict_ptr)
            {
                res_columns[i++]->insert(dict_ptr->getBytesAllocated());
                res_columns[i++]->insert(dict_ptr->getHierarchicalIndexBytesAllocated());
                res_columns[i++]->insert(dict_ptr->getQueryCount());
                res_columns[i++]->insert(dict_ptr->getHitRate());
                res_columns[i++]->insert(dict_ptr->getFoundRate());
                res_columns[i++]->insert(dict_ptr->getElementCount());
                res_columns[i++]->insert(dict_ptr->getLoadFactor());
                res_columns[i++]->insert(dict_ptr->getSource()->toString());

                const auto & lifetime = dict_ptr->getLifetime();
                res_columns[i++]->insert(lifetime.min_sec);
                res_columns[i++]->insert(lifetime.max_sec);
                if (!last_exception)
                    last_exception = dict_ptr->getLastException();
            }
            else
            {
                for (size_t j = 0; j != 10; ++j) // Number of empty fields if dict_ptr is null
                    res_columns[i++]->insertDefault();
            }

            res_columns[i++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(load_result.loading_start_time)));
            res_columns[i++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(load_result.last_successful_update_time)));
            res_columns[i++]->insert(load_result.error_count);
            res_columns[i++]->insert(std::chrono::duration_cast<std::chrono::duration<float>>(load_result.loading_duration).count());

            if (last_exception)
                res_columns[i++]->insert(getExceptionMessage(last_exception, false));
            else
                res_columns[i++]->insertDefault();

            if (dict_ptr)
            {
                res_columns[i++]->insert(dict_ptr->getDictionaryComment());
            }
            else
            {
                if (load_result.config && load_result.config->config->has("dictionary.comment"))
                    res_columns[i++]->insert(load_result.config->config->getString("dictionary.comment"));
                else
                    res_columns[i++]->insertDefault();
            }

            /// Start fill virtual columns

            if (dict_structure)
                res_columns[i++]->insert(dict_structure->getKeyDescription());
            else
                res_columns[i++]->insertDefault();
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDictionaries) }
