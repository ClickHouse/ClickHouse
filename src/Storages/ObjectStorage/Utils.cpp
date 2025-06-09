#include <Storages/ObjectStorage/Utils.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

std::optional<String> checkAndGetNewFileOnInsertIfNeeded(
    const IObjectStorage & object_storage,
    const StorageObjectStorage::Configuration & configuration,
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
        "If you want to overwrite it, enable setting {}_truncate_on_insert, if you "
        "want to create a new file on each insert, enable setting {}_create_new_file_on_insert",
        configuration.getNamespace(), key, configuration.getTypeName(), configuration.getTypeName());
}

void resolveSchemaAndFormat(
    ColumnsDescription & columns,
    std::string & format,
    ObjectStoragePtr object_storage,
    const StorageObjectStorage::ConfigurationPtr & configuration,
    std::optional<FormatSettings> format_settings,
    std::string & sample_path,
    const ContextPtr & context)
{
    if (format == "auto")
    {
        if (configuration->isDataLakeConfiguration())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Format must be already specified for {} storage.",
                configuration->getTypeName());
        }
    }

    if (columns.empty())
    {
        if (configuration->isDataLakeConfiguration())
        {
            auto table_structure = configuration->tryGetTableStructureFromMetadata();
            if (table_structure)
                columns = table_structure.value();
        }

        if (columns.empty())
        {
            if (format == "auto")
            {
                std::tie(columns, format) = StorageObjectStorage::resolveSchemaAndFormatFromData(
                    object_storage, configuration, format_settings, sample_path, context);
            }
            else
            {
                chassert(!format.empty());
                columns = StorageObjectStorage::resolveSchemaFromData(object_storage, configuration, format_settings, sample_path, context);
            }
        }
    }
    else if (format == "auto")
    {
        format = StorageObjectStorage::resolveFormatFromData(object_storage, configuration, format_settings, sample_path, context);
    }

    validateSupportedColumns(columns, *configuration);
}

void validateSupportedColumns(
    ColumnsDescription & columns,
    const StorageObjectStorage::Configuration & configuration)
{
    if (!columns.hasOnlyOrdinary())
    {
        /// We don't allow special columns.
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Special columns like MATERIALIZED, ALIAS or EPHEMERAL are not supported for {} storage.",
            configuration.getTypeName());
    }
}

}
