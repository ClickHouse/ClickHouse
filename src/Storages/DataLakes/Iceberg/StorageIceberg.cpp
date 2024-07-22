#include <Storages/DataLakes/Iceberg/StorageIceberg.h>

#if USE_AWS_S3 && USE_AVRO

namespace DB
{

StoragePtr StorageIceberg::create(
    const DB::StorageIceberg::Configuration & base_configuration,
    DB::ContextPtr context_,
    LoadingStrictnessLevel mode,
    const DB::StorageID & table_id_,
    const DB::ColumnsDescription & columns_,
    const DB::ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_)
{
    auto configuration{base_configuration};
    configuration.update(context_);
    std::unique_ptr<IcebergMetadata> metadata;
    NamesAndTypesList schema_from_metadata;
    try
    {
        metadata = parseIcebergMetadata(configuration, context_);
        schema_from_metadata = metadata->getTableSchema();
        configuration.keys = metadata->getDataFiles();
    }
    catch (...)
    {
        if (mode <= LoadingStrictnessLevel::CREATE)
            throw;
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return std::make_shared<StorageIceberg>(
        std::move(metadata),
        configuration,
        context_,
        table_id_,
        columns_.empty() ? ColumnsDescription(schema_from_metadata) : columns_,
        constraints_,
        comment,
        format_settings_);
}

StorageIceberg::StorageIceberg(
    std::unique_ptr<IcebergMetadata> metadata_,
    const Configuration & configuration_,
    ContextPtr context_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_)
    : StorageS3(configuration_, context_, table_id_, columns_, constraints_, comment, format_settings_)
    , current_metadata(std::move(metadata_))
    , base_configuration(configuration_)
{
}

ColumnsDescription StorageIceberg::getTableStructureFromData(
    Configuration & base_configuration,
    const std::optional<FormatSettings> &,
    const ContextPtr & local_context)
{
    auto configuration{base_configuration};
    configuration.update(local_context);
    auto metadata = parseIcebergMetadata(configuration, local_context);
    return ColumnsDescription(metadata->getTableSchema());
}

void StorageIceberg::updateConfigurationImpl(const ContextPtr & local_context)
{
    const bool updated = base_configuration.update(local_context);
    auto new_metadata = parseIcebergMetadata(base_configuration, local_context);

    if (!current_metadata || new_metadata->getVersion() != current_metadata->getVersion())
        current_metadata = std::move(new_metadata);
    else if (!updated)
        return;

    auto updated_configuration{base_configuration};
    /// If metadata wasn't changed, we won't list data files again.
    updated_configuration.keys = current_metadata->getDataFiles();
    StorageS3::useConfiguration(updated_configuration);
}

}

#endif
