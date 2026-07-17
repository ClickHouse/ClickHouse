#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeMetadata.h>

#if USE_PARQUET

#include <Databases/DataLake/DatabaseDataLake.h>
#include <Databases/DataLake/DuckLakeCatalog.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakePositionalDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Common/Exception.h>

namespace DB
{

namespace DataLakeStorageSetting
{
extern const DataLakeStorageSettingsString ducklake_schema_name;
extern const DataLakeStorageSettingsString ducklake_table_name;
extern const DataLakeStorageSettingsString ducklake_database_name;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace
{

String stripScheme(const String & path)
{
    const auto pos = path.find("://");
    if (pos == String::npos)
        return path;
    return path.substr(pos + 3);
}

/// Simple iterator over a pre-materialized file list (DuckLake file listing is one catalog query).
class DuckLakeObjectIterator final : public IObjectIterator
{
public:
    explicit DuckLakeObjectIterator(std::vector<DuckLakeDataObjectInfoPtr> && infos_)
        : infos(std::move(infos_))
    {
    }

    ObjectInfoPtr next(size_t) override
    {
        if (index >= infos.size())
            return nullptr;
        return infos[index++];
    }

    size_t estimatedKeysCount() override { return infos.size(); }

private:
    std::vector<DuckLakeDataObjectInfoPtr> infos;
    size_t index = 0;
};

}

DuckLakeMetadata::DuckLakeMetadata(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationWeakPtr configuration_,
    std::shared_ptr<DuckLakeCatalog> catalog_,
    Int64 snapshot_id_,
    Int64 table_id_,
    NamesAndTypesList schema_,
    ColumnMapperPtr column_mapper_,
    String catalog_table_path_,
    String storage_table_path_)
    : object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , catalog(std::move(catalog_))
    , snapshot_id(snapshot_id_)
    , table_id(table_id_)
    , schema(std::move(schema_))
    , column_mapper(std::move(column_mapper_))
    , catalog_table_path(std::move(catalog_table_path_))
    , storage_table_path(std::move(storage_table_path_))
{
}

DataLakeMetadataPtr DuckLakeMetadata::create(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & /* local_context */)
{
    auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to create DuckLake table, but storage configuration is expired");

    const auto & data_lake_settings = configuration_ptr->getDataLakeSettings();
    const String schema_name = data_lake_settings[DataLakeStorageSetting::ducklake_schema_name].value;
    const String table_name = data_lake_settings[DataLakeStorageSetting::ducklake_table_name].value;
    const String database_name = data_lake_settings[DataLakeStorageSetting::ducklake_database_name].value;

    if (schema_name.empty() || table_name.empty() || database_name.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DuckLake table was created outside of a DataLakeCatalog database with catalog_type = 'ducklake', "
            "which is not supported");

    auto database = DatabaseCatalog::instance().tryGetDatabase(database_name);
    auto datalake_database = std::dynamic_pointer_cast<DatabaseDataLake>(database);
    if (!datalake_database)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake catalog database '{}' does not exist", database_name);

    auto catalog = std::dynamic_pointer_cast<DuckLakeCatalog>(datalake_database->getCatalog());
    if (!catalog)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database '{}' does not have a DuckLake catalog", database_name);

    auto info = catalog->getTableSnapshotInfo(schema_name, table_name);

    auto column_mapper = std::make_shared<ColumnMapper>();
    column_mapper->setStorageColumnEncoding(std::move(info.field_id_map));

    String catalog_path = stripScheme(catalog->getTableDataPath(schema_name, table_name, info.snapshot_id));
    while (catalog_path.ends_with('/'))
        catalog_path.pop_back();

    String storage_path = configuration_ptr->getPathForRead().path;
    while (storage_path.ends_with('/'))
        storage_path.pop_back();

    return std::make_unique<DuckLakeMetadata>(
        object_storage,
        configuration,
        std::move(catalog),
        info.snapshot_id,
        info.table_id,
        std::move(info.schema),
        std::move(column_mapper),
        std::move(catalog_path),
        std::move(storage_path));
}

void DuckLakeMetadata::createInitial(
    const ObjectStoragePtr & /*object_storage*/,
    const StorageObjectStorageConfigurationWeakPtr & /*configuration*/,
    const ContextPtr & /*local_context*/,
    const std::optional<ColumnsDescription> & /*columns*/,
    ASTPtr /*partition_by*/,
    ASTPtr /*order_by*/,
    bool /*if_not_exists*/,
    std::shared_ptr<DataLake::ICatalog> /*catalog*/,
    const StorageID & /*table_id*/)
{
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "Creating DuckLake tables is not supported: the DuckLake integration is read-only");
}

ObjectIterator DuckLakeMetadata::iterate(
    const ActionsDAG * /* filter_dag */,
    FileProgressCallback /* callback */,
    size_t /* list_batch_size */,
    StorageMetadataPtr /* storage_metadata_snapshot */,
    ContextPtr /* context */) const
{
    const auto files = catalog->getDataFiles(table_id, snapshot_id);

    std::vector<DuckLakeDataObjectInfoPtr> infos;
    infos.reserve(files.size());
    for (const auto & file : files)
    {
        std::vector<DuckLakeDataObjectInfo::PositionalDeleteFile> delete_files;
        delete_files.reserve(file.delete_files.size());
        for (const auto & delete_file : file.delete_files)
        {
            delete_files.push_back(DuckLakeDataObjectInfo::PositionalDeleteFile{
                .path = toObjectPath(delete_file.path, delete_file.path_is_relative),
                .delete_count = delete_file.delete_count,
            });
        }

        infos.push_back(std::make_shared<DuckLakeDataObjectInfo>(
            toObjectPath(file.path, file.path_is_relative),
            std::move(delete_files),
            file.record_count,
            file.file_size_bytes));
    }
    return std::make_shared<DuckLakeObjectIterator>(std::move(infos));
}

bool DuckLakeMetadata::operator==(const IDataLakeMetadata & other) const
{
    const auto * ducklake_metadata = dynamic_cast<const DuckLakeMetadata *>(&other);
    return ducklake_metadata
        && snapshot_id == ducklake_metadata->snapshot_id
        && table_id == ducklake_metadata->table_id;
}

void DuckLakeMetadata::modifyFormatSettings(FormatSettings & format_settings, const Context &) const
{
    /// DeletionVectorTransform needs ChunkInfoRowNumbers on chunks.
    format_settings.parquet.preserve_order = true;
    /// Files written before an ADD COLUMN lack the new field ids; fill them with defaults.
    format_settings.parquet.allow_missing_columns = true;
}

void DuckLakeMetadata::addDeleteTransformers(
    ObjectInfoPtr object_info,
    QueryPipelineBuilder & builder,
    const std::optional<FormatSettings> & format_settings,
    FormatParserSharedResourcesPtr parser_shared_resources,
    ContextPtr context) const
{
    auto ducklake_object_info = std::dynamic_pointer_cast<DuckLakeDataObjectInfo>(object_info);
    if (!ducklake_object_info || ducklake_object_info->positional_delete_files.empty())
        return;

    builder.addSimpleTransform(
        [&](const SharedHeader & header)
        {
            return std::make_shared<DuckLakePositionalDeleteTransform>(
                header, object_storage, ducklake_object_info, format_settings, parser_shared_resources, context);
        });
}

String DuckLakeMetadata::toObjectPath(const String & path, bool path_is_relative) const
{
    String relative;
    if (path_is_relative)
    {
        relative = path;
    }
    else
    {
        const String fs_path = stripScheme(path);
        if (!fs_path.starts_with(catalog_table_path + "/"))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DuckLake file '{}' is located outside of the table data path '{}'; reading such files is not supported",
                path,
                catalog_table_path);
        relative = fs_path.substr(catalog_table_path.size() + 1);
    }
    return storage_table_path + "/" + relative;
}

}

#endif
