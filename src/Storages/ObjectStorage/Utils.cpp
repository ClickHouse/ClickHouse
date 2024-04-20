#include <Storages/ObjectStorage/Utils.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::optional<String> checkAndGetNewFileOnInsertIfNeeded(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const StorageObjectStorage::QuerySettings & settings,
    const String & key,
    size_t sequence_number)
{
    if (settings.truncate_on_insert
        || !object_storage.exists(StoredObject(key)))
        return std::nullopt;

    if (settings.create_new_file_on_insert)
    {
        auto pos = key.find_first_of('.');
        String new_key;
        do
        {
            new_key = key.substr(0, pos) + "." + std::to_string(sequence_number) + (pos == std::string::npos ? "" : key.substr(pos));
            ++sequence_number;
        }
        while (object_storage.exists(StoredObject(new_key)));

        return new_key;
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Object in bucket {} with key {} already exists. "
        "If you want to overwrite it, enable setting s3_truncate_on_insert, if you "
        "want to create a new file on each insert, enable setting s3_create_new_file_on_insert",
        configuration.getNamespace(), key);
}

StorageInMemoryMetadata getStorageMetadata(
    ObjectStoragePtr object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    std::optional<FormatSettings> format_settings,
    const String & comment,
    const ContextPtr & context)
{
    StorageInMemoryMetadata storage_metadata;
    if (columns.empty())
    {
        auto fetched_columns = StorageObjectStorage::getTableStructureFromData(object_storage, configuration, format_settings, context);
        storage_metadata.setColumns(fetched_columns);
    }
    else if (!columns.hasOnlyOrdinary())
    {
        /// We don't allow special columns.
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Special columns are not supported for {} storage"
                        "like MATERIALIZED, ALIAS or EPHEMERAL", configuration->getTypeName());
    }
    else
    {
        if (configuration->format == "auto")
            StorageObjectStorage::setFormatFromData(object_storage, configuration, format_settings, context);

        storage_metadata.setColumns(columns);
    }
    storage_metadata.setConstraints(constraints);
    storage_metadata.setComment(comment);
    return storage_metadata;
}

}
