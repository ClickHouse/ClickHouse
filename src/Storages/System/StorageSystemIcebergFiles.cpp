#include <Storages/System/StorageSystemIcebergFiles.h>

#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>


namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool use_iceberg_metadata_files_cache;
}

ColumnsDescription StorageSystemIcebergFiles::getColumnsDescription()
{
    /// Values match `Iceberg::FileContentType` in `Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h`,
    /// duplicated here because that header is only available when `USE_AVRO` is enabled.
    auto content_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"DATA", 0},
        {"POSITION_DELETE", 1},
        {"EQUALITY_DELETE", 2},
    });

    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"snapshot_id", std::make_shared<DataTypeInt64>(), "Snapshot ID at which the file was added."},
        {"content", content_enum, "File content kind."},
        {"file_path", std::make_shared<DataTypeString>(), "Resolved storage path of the file."},
        {"file_format", std::make_shared<DataTypeString>(), "File format, e.g. 'PARQUET'."},
        {"record_count", std::make_shared<DataTypeInt64>(), "Number of records in the file."},
        {"file_size_in_bytes", std::make_shared<DataTypeInt64>(), "Size of the file in bytes."},
        {"partition", std::make_shared<DataTypeString>(), "Textual representation of the partition tuple."},
        {"schema_id", std::make_shared<DataTypeInt32>(), "Schema ID resolved for this manifest entry."},
        {"sequence_number", std::make_shared<DataTypeInt64>(), "Resolved sequence number of the manifest entry (always 0 for format v1)."},
        {"sort_order_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "Sort order ID of the file, if specified."},
        {"null_value_counts", std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()), "Per-column null value count (column id -> count)."},
        {"column_sizes", std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()), "Per-column on-disk size in bytes (column id -> bytes)."},
        {"value_counts", std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()), "Per-column total value count (column id -> count)."},
        {"equality_ids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()), "Equality field IDs for equality delete files (empty for non-equality-delete files)."},
    };
}

void StorageSystemIcebergFiles::fillData([[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
#if USE_AVRO
    ContextMutablePtr context_copy = Context::createCopy(context);
    Settings settings_copy = context_copy->getSettingsCopy();
    // Do not use the cache for now. It has previously caused correctness issues in system.iceberg_history (https://github.com/ClickHouse/ClickHouse/pull/89003).
    settings_copy[Setting::use_iceberg_metadata_files_cache] = false;
    context_copy->setSettings(settings_copy);

    const auto access = context_copy->getAccess();

    auto add_file_records = [&](const DatabaseTablesIteratorPtr & it, StorageObjectStorage * object_storage)
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
            return;

        /// Iceberg tables can be broken in arbitrary ways; tolerate any error from a single table
        /// instead of failing the whole query.
        try
        {
            if (IcebergMetadata * iceberg_metadata = dynamic_cast<IcebergMetadata *>(object_storage->getExternalMetadata(context_copy)); iceberg_metadata)
            {
                IcebergMetadata::IcebergFiles files = iceberg_metadata->getFiles(context_copy);

                for (auto & file : files)
                {
                    size_t column_index = 0;
                    res_columns[column_index++]->insert(it->databaseName());
                    res_columns[column_index++]->insert(it->name());
                    res_columns[column_index++]->insert(file.snapshot_id);
                    res_columns[column_index++]->insert(static_cast<Int8>(file.content));
                    res_columns[column_index++]->insert(file.file_path);
                    res_columns[column_index++]->insert(file.file_format);
                    res_columns[column_index++]->insert(file.record_count);
                    res_columns[column_index++]->insert(file.file_size_in_bytes);
                    res_columns[column_index++]->insert(file.partition);
                    res_columns[column_index++]->insert(file.schema_id);
                    res_columns[column_index++]->insert(file.sequence_number);

                    if (file.sort_order_id.has_value())
                        res_columns[column_index++]->insert(*file.sort_order_id);
                    else
                        res_columns[column_index++]->insertDefault();

                    auto insert_id_to_int_map = [&](const std::map<Int32, Int64> & values)
                    {
                        Map map_field;
                        map_field.reserve(values.size());
                        for (const auto & [key, value] : values)
                            map_field.push_back(Tuple{key, value});
                        res_columns[column_index++]->insert(map_field);
                    };

                    insert_id_to_int_map(file.null_value_counts);
                    insert_id_to_int_map(file.column_sizes);
                    insert_id_to_int_map(file.value_counts);

                    Array equality_ids_array;
                    if (file.equality_ids.has_value())
                    {
                        equality_ids_array.reserve(file.equality_ids->size());
                        for (auto id : *file.equality_ids)
                            equality_ids_array.push_back(id);
                    }
                    res_columns[column_index++]->insert(equality_ids_array);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("SystemIcebergFiles"), fmt::format("Ignoring broken table {}", object_storage->getStorageID().getFullTableName()));
        }
    };

    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    if (show_tables_granted)
    {
        auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true});
        for (const auto & db : databases)
        {
            for (auto iterator = db.second->getTablesIterator(context_copy, {}, true); iterator->isValid(); iterator->next())
            {
                StoragePtr storage = iterator->table();

                TableLockHolder lock = storage->tryLockForShare(context_copy->getCurrentQueryId(), context_copy->getSettingsRef()[Setting::lock_acquire_timeout]);
                if (!lock)
                    continue;

                if (auto * object_storage_table = dynamic_cast<StorageObjectStorage *>(storage.get()))
                {
                    add_file_records(iterator, object_storage_table);
                }
            }
        }
    }
#endif
}

}
