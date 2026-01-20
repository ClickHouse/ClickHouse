#include <mutex>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/LimitTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemDeltaLakeHistory.h>

#include "config.h"

#if USE_PARQUET
#    include <filesystem>
#    include <IO/ReadBufferFromFileBase.h>
#    include <IO/ReadHelpers.h>
#    include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#    include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#    include <Storages/ObjectStorage/Utils.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>
#endif

/// Delta Lake timestamps are stored in milliseconds
static constexpr auto TIME_SCALE = 3;

namespace DB
{

namespace Setting
{
extern const SettingsSeconds lock_acquire_timeout;
}

ColumnsDescription StorageSystemDeltaLakeHistory::getColumnsDescription()
{
    return ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"version", std::make_shared<DataTypeUInt64>(), "Version number of the Delta Lake table."},
        {"timestamp",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)),
         "Timestamp when this version was committed."},
        {"operation", std::make_shared<DataTypeString>(), "Operation type (e.g., WRITE, DELETE, MERGE)."},
        {"operation_parameters",
         std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()),
         "Parameters of the operation."},
        {"is_current", std::make_shared<DataTypeUInt8>(), "Flag indicating if this is the current version."}};
}

void StorageSystemDeltaLakeHistory::fillData(
    [[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
#if USE_PARQUET
    ContextMutablePtr context_copy = Context::createCopy(context);

    const auto access = context_copy->getAccess();

    auto add_history_record = [&](const DatabaseTablesIteratorPtr & it, StorageObjectStorage * object_storage_table)
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
            return;

        try
        {
            /// Check if this is a Delta Lake table using dynamic_cast (similar to IcebergMetadata check)
            DeltaLakeMetadata * delta_metadata = dynamic_cast<DeltaLakeMetadata *>(object_storage_table->getExternalMetadata(context_copy));
            if (!delta_metadata)
                return;

            /// Get the table path and object storage from metadata getters
            const auto & object_storage = delta_metadata->getObjectStorage();
            if (!object_storage)
                return;

            const auto table_path = delta_metadata->getTablePath();
            static constexpr auto deltalake_metadata_directory = "_delta_log";
            static constexpr auto metadata_file_suffix = ".json";

            /// List all JSON files in _delta_log directory
            auto log = getLogger("SystemDeltaLakeHistory");
            Strings metadata_files;
            try
            {
                metadata_files = listFiles(*object_storage, table_path, deltalake_metadata_directory, metadata_file_suffix);
            }
            catch (...)
            {
                tryLogCurrentException(
                    log,
                    fmt::format("Failed to list metadata files for table {}", object_storage_table->getStorageID().getFullTableName()));
                return;
            }

            /// Sort files to get versions in order
            std::sort(metadata_files.begin(), metadata_files.end());

            /// Find the current (latest) version
            UInt64 current_version = 0;
            if (!metadata_files.empty())
            {
                const auto & last_file = metadata_files.back();
                auto filename = std::filesystem::path(last_file).filename().string();
                if (filename.size() > 5 && filename.ends_with(".json"))
                {
                    filename = filename.substr(0, filename.size() - 5);
                    try
                    {
                        current_version = std::stoull(filename);
                    }
                    catch (...)
                    {
                        /// Ignore parsing errors for version extraction - non-numeric filenames are skipped
                        current_version = 0;
                    }
                }
            }

            /// Process each metadata file to extract history
            for (const auto & metadata_file : metadata_files)
            {
                try
                {
                    /// Extract version from filename
                    auto filename = std::filesystem::path(metadata_file).filename().string();
                    if (filename.size() <= 5 || !filename.ends_with(".json"))
                        continue;

                    filename = filename.substr(0, filename.size() - 5);
                    UInt64 version = 0;
                    try
                    {
                        version = std::stoull(filename);
                    }
                    catch (...)
                    {
                        continue;
                    }

                    /// Read the metadata file
                    RelativePathWithMetadata object_info(metadata_file);
                    auto buf = createReadBuffer(object_info, object_storage, context_copy, log);

                    String operation;
                    std::map<String, String> operation_parameters;
                    std::optional<DateTime64> timestamp;

                    char c;
                    while (!buf->eof())
                    {
                        /// Skip invalid characters before JSON
                        while (buf->peek(c) && c != '{')
                            buf->ignore();

                        if (buf->eof())
                            break;

                        String json_str;
                        readJSONObjectPossiblyInvalid(json_str, *buf);

                        if (json_str.empty())
                            continue;

                        Poco::JSON::Parser parser;
                        Poco::Dynamic::Var json = parser.parse(json_str);
                        const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

                        if (!object)
                            continue;

                        /// Extract commitInfo
                        if (object->has("commitInfo"))
                        {
                            const auto commit_info = object->get("commitInfo").extract<Poco::JSON::Object::Ptr>();
                            if (commit_info)
                            {
                                if (commit_info->has("timestamp"))
                                {
                                    auto ts = commit_info->getValue<Int64>("timestamp");
                                    timestamp = static_cast<DateTime64>(ts);
                                }

                                if (commit_info->has("operation"))
                                    operation = commit_info->getValue<String>("operation");

                                if (commit_info->has("operationParameters"))
                                {
                                    const auto params = commit_info->get("operationParameters").extract<Poco::JSON::Object::Ptr>();
                                    if (params)
                                    {
                                        for (const auto & param : *params)
                                            operation_parameters[param.first] = param.second.toString();
                                    }
                                }
                            }
                            break;
                        }
                    }

                    /// Insert the record
                    size_t column_index = 0;
                    res_columns[column_index++]->insert(it->databaseName());
                    res_columns[column_index++]->insert(it->name());
                    res_columns[column_index++]->insert(version);

                    if (timestamp.has_value())
                        res_columns[column_index++]->insert(timestamp.value());
                    else
                        res_columns[column_index++]->insertDefault();

                    res_columns[column_index++]->insert(operation);

                    /// Insert operation_parameters as Map
                    Map params_map;
                    for (const auto & [key, value] : operation_parameters)
                    {
                        Tuple tuple;
                        tuple.push_back(key);
                        tuple.push_back(value);
                        params_map.push_back(std::move(tuple));
                    }
                    res_columns[column_index++]->insert(params_map);

                    res_columns[column_index++]->insert(version == current_version ? 1 : 0);
                }
                catch (...)
                {
                    tryLogCurrentException(
                        getLogger("SystemDeltaLakeHistory"),
                        fmt::format(
                            "Failed to process metadata file {} for table {}",
                            metadata_file,
                            object_storage_table->getStorageID().getFullTableName()));
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(
                getLogger("SystemDeltaLakeHistory"),
                fmt::format("Ignoring broken table {}", object_storage_table->getStorageID().getFullTableName()));
        }
    };

    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    if (show_tables_granted)
    {
        auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true});
        for (const auto & db : databases)
        {
            for (auto iterator = db.second->getLightweightTablesIterator(context_copy, {}, true); iterator->isValid(); iterator->next())
            {
                StoragePtr storage = iterator->table();

                TableLockHolder lock = storage->tryLockForShare(
                    context_copy->getCurrentQueryId(), context_copy->getSettingsRef()[Setting::lock_acquire_timeout]);
                if (!lock)
                    continue;

                if (auto * object_storage_table = dynamic_cast<StorageObjectStorage *>(storage.get()))
                {
                    add_history_record(iterator, object_storage_table);
                }
            }
        }
    }
#endif
}

}
