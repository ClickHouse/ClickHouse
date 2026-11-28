#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageProxy.h>
#include <Storages/System/StorageSystemIcebergMetadataLogEntries.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/extractTablesFilter.h>
#include <Storages/VirtualColumnUtils.h>

/// Iceberg specs mention that the timestamps are stored in ms: https://iceberg.apache.org/spec/#table-metadata-fields
static constexpr auto TIME_SCALE = 3;

namespace DB
{

namespace Setting
{
extern const SettingsSeconds lock_acquire_timeout;
extern const SettingsBool use_iceberg_metadata_files_cache;
extern const SettingsBool database_datalake_require_metadata_access;
}

ColumnsDescription StorageSystemIcebergMetadataLogEntries::getColumnsDescription()
{
    return ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"timestamp", std::make_shared<DataTypeDateTime64>(TIME_SCALE), "Date & time when this metadata file was made current."},
        {"file", std::make_shared<DataTypeString>(), "Path of the metadata file."},
        {"latest_snapshot_id",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "Id of the latest snapshot at the time of this metadata file."},
        {"latest_schema_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "Schema id of the latest snapshot."},
        {"latest_sequence_number",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
         "Sequence number of the latest snapshot."}};
}

void StorageSystemIcebergMetadataLogEntries::fillData(
    [[maybe_unused]] MutableColumns & res_columns,
    [[maybe_unused]] ContextPtr context,
    [[maybe_unused]] const ActionsDAG::Node * predicate,
    std::vector<UInt8>) const
{
#if USE_AVRO
    ContextMutablePtr context_copy = Context::createCopy(context);
    Settings settings_copy = context_copy->getSettingsCopy();
    settings_copy[Setting::use_iceberg_metadata_files_cache] = false;
    context_copy->setSettings(settings_copy);

    const auto access = context_copy->getAccess();

    if (!access->isGranted(AccessType::SHOW_TABLES))
        return;

    auto log = getLogger("SystemIcebergMetadataLogEntries");

    auto add_metadata_log_entries = [&](const String & database_name, const String & table_name, StoragePtr & storage)
    {
        TableLockHolder lock
            = storage->tryLockForShare(context_copy->getCurrentQueryId(), context_copy->getSettingsRef()[Setting::lock_acquire_timeout]);
        if (!lock)
            /// Table was dropped while acquiring the lock, skipping table
            return;

        auto * object_storage = castStorage<StorageObjectStorage>(storage, DeferredTable::Skip).get();

        if (!object_storage || !object_storage->isIcebergStorage())
            return;

        if (auto iceberg_metadata = std::dynamic_pointer_cast<IcebergMetadata>(object_storage->getExternalMetadata(context_copy));
            iceberg_metadata)
        {
            IcebergMetadata::IcebergMetadataLogEntries iceberg_metadata_log_entries = iceberg_metadata->getMetadataLogEntries(context_copy);

            for (const auto & iceberg_metadata_log_entry : iceberg_metadata_log_entries)
            {
                size_t column_index = 0;
                res_columns[column_index++]->insert(database_name);
                res_columns[column_index++]->insert(table_name);
                res_columns[column_index++]->insert(iceberg_metadata_log_entry.timestamp);
                res_columns[column_index++]->insert(iceberg_metadata_log_entry.file);

                if (iceberg_metadata_log_entry.latest_snapshot_id)
                    res_columns[column_index++]->insert(*iceberg_metadata_log_entry.latest_snapshot_id);
                else
                    res_columns[column_index++]->insertDefault();

                if (iceberg_metadata_log_entry.latest_schema_id)
                    res_columns[column_index++]->insert(*iceberg_metadata_log_entry.latest_schema_id);
                else
                    res_columns[column_index++]->insertDefault();

                if (iceberg_metadata_log_entry.latest_sequence_number)
                    res_columns[column_index++]->insert(*iceberg_metadata_log_entry.latest_sequence_number);
                else
                    res_columns[column_index++]->insertDefault();
            }
        }
    };

    auto filter_databases = [&]()
    {
        const auto & catalog = DatabaseCatalog::instance();
        /// The block filter below only sees `database`, so it cannot use a condition that names the
        /// database together with the table, such as `(database, table) IN ((db, t))`; the extraction
        /// reads that shape too, and shortlists the databases first.
        const auto database_name_filter = extractNameFilter(predicate, "database", context_copy);
        MutableColumnPtr database_column = ColumnString::create();
        for (const auto & [name, db] : catalog.getDatabases({.with_datalake_catalogs = true, .with_remote_databases = false}))
            if (!database_name_filter || database_name_filter(name))
                database_column->insert(name);

        Block databases_block{
            {std::move(database_column), std::make_shared<DataTypeString>(), "database"},
        };
        VirtualColumnUtils::filterBlockWithPredicate(predicate, databases_block, context_copy);
        return databases_block;
    };

    auto filter_tables = [&]()
    {
        auto filtered_databases_block = filter_databases();
        const auto & filtered_databases = assert_cast<const ColumnString &>(*filtered_databases_block.getByName("database").column);

        const TablesFilter tables_filter = extractTablesFilter(predicate, "table", context_copy);

        MutableColumnPtr database_column = ColumnString::create();
        MutableColumnPtr table_column = ColumnString::create();

        for (size_t i = 0; i < filtered_databases.size(); ++i)
        {
            auto database_name = filtered_databases.getDataAt(i);
            DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name);
            if (!database)
                continue;

            for (const auto & table_details : database->getLightweightTablesIteratorWithHint(context_copy, {}, true, tables_filter))
            {
                database_column->insert(database_name);
                table_column->insert(table_details.name);
            }
        }

        Block filtered_block{
            {std::move(database_column), std::make_shared<DataTypeString>(), "database"},
            {std::move(table_column), std::make_shared<DataTypeString>(), "table"},
        };
        VirtualColumnUtils::filterBlockWithPredicate(predicate, filtered_block, context_copy);
        return filtered_block;
    };

    auto filtered_block = filter_tables();

    const auto & databases = assert_cast<const ColumnString &>(*filtered_block.getByName("database").column);
    const auto & tables = assert_cast<const ColumnString &>(*filtered_block.getByName("table").column);

    const bool rethrow_table_exception = settings_copy[Setting::database_datalake_require_metadata_access];

    for (size_t i = 0; i < databases.size(); ++i)
    {
        const String database_name{databases.getDataAt(i)};
        const String table_name{tables.getDataAt(i)};

        if (!access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
            continue;

        DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (!database)
            continue;

        StoragePtr storage;

        try
        {
            storage = database->tryGetTable(table_name, context_copy);
            if (!storage)
                continue;
        }
        catch (...)
        {
            if (rethrow_table_exception)
                throw;

            tryLogCurrentException(log, fmt::format("Ignoring broken table {}.{}", database_name, table_name));
            continue;
        }

        add_metadata_log_entries(database_name, table_name, storage);
    }
#endif
}

Block StorageSystemIcebergMetadataLogEntries::getFilterSampleBlock() const
{
#if USE_AVRO
    return {
        {{}, std::make_shared<DataTypeString>(), "database"},
        {{}, std::make_shared<DataTypeString>(), "table"},
    };
#else
    return {};
#endif
}
}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemIcebergMetadataLogEntries) }
