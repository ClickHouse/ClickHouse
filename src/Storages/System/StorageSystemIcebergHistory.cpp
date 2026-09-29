#include <mutex>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/LimitTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotSummary.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemIcebergHistory.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/extractTableNameFilter.h>
#include <Storages/VirtualColumnUtils.h>

#if USE_AVRO
#include <base/EnumReflection.h>
#endif

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

ColumnsDescription StorageSystemIcebergHistory::getColumnsDescription()
{
    auto operation_column_description = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"UNKNOWN", static_cast<Int8>(-1)},
#if USE_AVRO
        {"APPEND", static_cast<Int8>(Iceberg::SnapshotSummaryOperation::APPEND)},
        {"OVERWRITE", static_cast<Int8>(Iceberg::SnapshotSummaryOperation::OVERWRITE)},
        {"REPLACE", static_cast<Int8>(Iceberg::SnapshotSummaryOperation::REPLACE)},
        {"DELETE", static_cast<Int8>(Iceberg::SnapshotSummaryOperation::DELETE)},
#endif
    });

    return ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"made_current_at",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)),
         "Date & time when this snapshot was made current snapshot"},
        {"snapshot_id", std::make_shared<DataTypeUInt64>(), "Snapshot id which is used to identify a snapshot."},
        {"parent_id", std::make_shared<DataTypeUInt64>(), "Parent id of this snapshot."},
        {"is_current_ancestor",
         std::make_shared<DataTypeUInt8>(),
         "Flag that indicates if this snapshot is an ancestor of the current snapshot."},
        {"operation",
         std::move(operation_column_description),
         "Snapshot operation (APPEND, OVERWRITE, DELETE, REPLACE and UNKNOWN). The UNKNOWN status means either that we were unable to read "
         "the summary or that it is empty (it's optional for v1). The correct 'operation' field is required"},
        {"summary",
         std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()),
         "Snapshot summary fields"}};
}

void StorageSystemIcebergHistory::fillData(
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

    auto log = getLogger("SystemIcebergHistory");

    auto try_add_history_record = [&](const String & database_name, const String & table_name, StoragePtr & storage)
    {
        /// Unfortunately this try/catch is unavoidable. Iceberg tables can be broken in arbitrary way, it's impossible
        /// to handle properly all possible errors which we can get when attempting to read metadata of iceberg table
        try
        {
            TableLockHolder lock = storage->tryLockForShare(
                context_copy->getCurrentQueryId(), context_copy->getSettingsRef()[Setting::lock_acquire_timeout]);
            if (!lock)
                // Table was dropped while acquiring the lock, skipping table
                return;

            auto * object_storage = dynamic_cast<StorageObjectStorage *>(storage.get());

            if (!object_storage || !object_storage->isIcebergStorage())
                return;

            if (auto iceberg_metadata = std::dynamic_pointer_cast<IcebergMetadata>(object_storage->getExternalMetadata(context_copy));
                iceberg_metadata)
            {
                IcebergMetadata::IcebergHistory iceberg_history_items = iceberg_metadata->getHistory(context_copy);

                for (auto & iceberg_history_item : iceberg_history_items)
                {
                    size_t column_index = 0;
                    res_columns[column_index++]->insert(database_name);
                    res_columns[column_index++]->insert(table_name);
                    res_columns[column_index++]->insert(iceberg_history_item.made_current_at);
                    res_columns[column_index++]->insert(iceberg_history_item.snapshot_id);
                    res_columns[column_index++]->insert(iceberg_history_item.parent_id);
                    res_columns[column_index++]->insert(iceberg_history_item.is_current_ancestor);

                    if (!iceberg_history_item.snapshot_summary)
                    {
                        res_columns[column_index++]->insert(Int8(-1));
                        res_columns[column_index++]->insertDefault();
                    }
                    else
                    {
                        const auto & snapshot_summary = iceberg_history_item.snapshot_summary;
                        res_columns[column_index++]->insert(snapshot_summary->getOperation());
                        res_columns[column_index++]->insert(snapshot_summary->toMap());
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Ignoring broken table {}.{}", database_name, table_name));
        }
    };

    auto filter_databases = [&]()
    {
        const auto & catalog = DatabaseCatalog::instance();
        MutableColumnPtr database_column = ColumnString::create();
        for (const auto & [name, db] : catalog.getDatabases({.with_datalake_catalogs = true, .with_remote_databases = false}))
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

        const TablesFilter tables_filter = extractTableNameFilter(predicate, "table");

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

        try_add_history_record(database_name, table_name, storage);
    }
#endif
}
Block StorageSystemIcebergHistory::getFilterSampleBlock() const
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
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemIcebergHistory) }
