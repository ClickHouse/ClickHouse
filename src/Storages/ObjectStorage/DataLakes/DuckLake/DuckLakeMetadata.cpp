#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeMetadata.h>

#if USE_PARQUET

#include <Databases/DataLake/DatabaseDataLake.h>
#include <Databases/DataLake/DuckLakeCatalog.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeInlinedDataSource.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeInlinedValues.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakePositionalDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakePruning.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

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
extern const int SUPPORT_IS_DISABLED;
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
    std::unordered_map<Int64, NameAndTypePair> column_types_by_id_,
    String catalog_table_path_,
    String storage_table_path_)
    : object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , catalog(std::move(catalog_))
    , snapshot_id(snapshot_id_)
    , table_id(table_id_)
    , schema(std::move(schema_))
    , column_mapper(std::move(column_mapper_))
    , column_types_by_id(std::move(column_types_by_id_))
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
        std::move(info.column_types),
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
    const ActionsDAG * filter_dag,
    FileProgressCallback /* callback */,
    size_t /* list_batch_size */,
    StorageMetadataPtr /* storage_metadata_snapshot */,
    ContextPtr context) const
{
    auto listing = catalog->getDataFiles(table_id, snapshot_id);

    static const std::unordered_map<String, Int64> no_field_ids;
    const auto & field_id_map = column_mapper ? column_mapper->getStorageColumnEncoding() : no_field_ids;
    DuckLake::FilePruner pruner(filter_dag, field_id_map, column_types_by_id, context);

    size_t pruned_files = 0;
    std::vector<DuckLakeDataObjectInfoPtr> infos;
    infos.reserve(listing.files.size());
    for (const auto & file : listing.files)
    {
        const std::vector<DuckLakePartitionField> * partition_spec = nullptr;
        if (file.partition_id.has_value())
        {
            auto spec_it = listing.partition_specs.find(*file.partition_id);
            if (spec_it != listing.partition_specs.end())
                partition_spec = &spec_it->second;
        }

        if (pruner.canBePruned(file, partition_spec))
        {
            ++pruned_files;
            continue;
        }

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
            file.file_size_bytes,
            file.inlined_deleted_positions));
    }

    if (pruned_files > 0)
        LOG_DEBUG(
            log,
            "DuckLake: pruned {} of {} files of table (id {}) at snapshot {}",
            pruned_files,
            listing.files.size(),
            table_id,
            snapshot_id);

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
    if (!ducklake_object_info
        || (ducklake_object_info->positional_delete_files.empty() && ducklake_object_info->inlined_deleted_positions.empty()))
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

Pipe DuckLakeMetadata::getAdditionalReadPipe(
    const ReadFromFormatInfo & info,
    StorageMetadataPtr /*storage_metadata_snapshot*/,
    ContextPtr /*context*/,
    size_t /*max_block_size*/) const
{
    const auto inlined_tables = catalog->getInlinedDataTables(table_id);
    if (inlined_tables.empty())
        return {};

    struct InlinedTableData
    {
        Int64 schema_version;
        std::vector<String> column_names;
        std::vector<std::vector<std::optional<String>>> rows;
    };
    std::vector<InlinedTableData> tables_with_rows;
    for (const auto & inlined_table : inlined_tables)
    {
        auto [column_names, rows] = catalog->getInlinedRows(inlined_table.table_name, snapshot_id);
        if (!rows.empty())
            tables_with_rows.push_back(InlinedTableData{inlined_table.schema_version, std::move(column_names), std::move(rows)});
    }
    if (tables_with_rows.empty())
        return {};

    /// Virtual columns (_path, _file, ...) have no meaning for catalog-stored rows.
    if (!info.requested_virtual_columns.empty())
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Virtual columns are not supported for DuckLake tables with inlined data (table id {})",
            table_id);

    /// Columns the source must produce: requested columns plus the inputs of the row-level
    /// filter and prewhere (the fallback FilterTransforms run after us).
    NameSet needed_columns;
    const auto add_needed = [&](const NamesAndTypesList & columns)
    {
        for (const auto & column : columns)
            needed_columns.insert(column.name.substr(0, column.name.find('.')));
    };
    add_needed(info.requested_columns);
    if (info.row_level_filter)
        add_needed(info.row_level_filter->actions.getRequiredColumns());
    if (info.prewhere_info)
        add_needed(info.prewhere_info->prewhere_actions.getRequiredColumns());

    NamesAndTypesList header_columns;
    for (const auto & column : schema)
    {
        if (needed_columns.contains(column.name))
            header_columns.push_back(column);
    }
    for (const auto & needed_name : needed_columns)
    {
        if (!schema.contains(needed_name))
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Column '{}' is not a DuckLake table column; such columns are not supported "
                "for DuckLake tables with inlined data (table id {})",
                needed_name,
                table_id);
    }

    const auto & field_id_map = column_mapper->getStorageColumnEncoding();
    const auto schema_version_snapshots = catalog->getSchemaVersionFirstSnapshots();
    const auto column_history = catalog->getColumnRows(table_id);
    const bool postgres_backend = catalog->isPostgres();

    const std::vector<NameAndTypePair> header_columns_vec(header_columns.begin(), header_columns.end());

    std::vector<Chunk> chunks;
    for (const auto & table_data : tables_with_rows)
    {
        /// SQL columns of this inlined table carry the column names as of its (global)
        /// schema version; resolve them to column ids via the column history visible at
        /// the first snapshot stamped with that version.
        const auto version_it = schema_version_snapshots.lower_bound(table_data.schema_version);
        if (version_it == schema_version_snapshots.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DuckLake inlined data table has unknown schema version {}",
                table_data.schema_version);
        const Int64 version_snapshot = version_it->second;

        std::unordered_map<String, Int64> id_by_version_name;
        for (const auto & column : column_history)
        {
            if (column.isVisibleAt(version_snapshot))
                id_by_version_name[column.name] = column.column_id;
        }

        /// Map every header column to its SQL column index in this inlined table (or none
        /// when the column was added after this schema version).
        std::vector<std::optional<size_t>> sql_index_by_header_column;
        sql_index_by_header_column.reserve(header_columns_vec.size());
        for (const auto & column : header_columns_vec)
        {
            const auto field_it = field_id_map.find(column.name);
            if (field_it == field_id_map.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "DuckLake column '{}' is missing in the field id map", column.name);
            std::optional<size_t> sql_index;
            for (size_t i = 0; i < table_data.column_names.size(); ++i)
            {
                const auto id_it = id_by_version_name.find(table_data.column_names[i]);
                if (id_it != id_by_version_name.end() && id_it->second == field_it->second)
                {
                    sql_index = i;
                    break;
                }
            }
            sql_index_by_header_column.push_back(sql_index);
        }

        const size_t num_rows = table_data.rows.size();
        Columns columns;
        columns.reserve(header_columns_vec.size());
        for (size_t c = 0; c < header_columns_vec.size(); ++c)
        {
            const auto & column = header_columns_vec[c];
            const auto sql_index = sql_index_by_header_column[c];
            if (!sql_index.has_value())
            {
                /// Column added after this inlined table's schema version: fill with defaults.
                columns.push_back(column.type->createColumn()->cloneResized(num_rows));
                continue;
            }
            std::vector<std::optional<String>> values;
            values.reserve(num_rows);
            for (const auto & row : table_data.rows)
                values.push_back(row[*sql_index]);
            columns.push_back(DuckLake::buildInlinedColumn(values, column.type, postgres_backend));
        }
        chunks.emplace_back(std::move(columns), num_rows);
    }

    Block header;
    for (const auto & column : header_columns)
        header.insert({column.type->createColumn(), column.type, column.name});

    /// The source materializes whole top-level columns; rebuild requested subcolumns
    /// (`s.x`) as proper subcolumn pairs so ExtractColumnsTransform can resolve them
    /// against those columns (plain `s.x` names would be looked up literally).
    NamesAndTypesList adjusted_requested_columns;
    for (const auto & requested : info.requested_columns)
    {
        const auto dot = requested.name.find('.');
        if (dot != String::npos && header.has(requested.name.substr(0, dot)))
        {
            const String top_name = requested.name.substr(0, dot);
            adjusted_requested_columns.emplace_back(
                top_name,
                requested.name.substr(dot + 1),
                header.getByName(top_name).type,
                requested.type);
            continue;
        }
        adjusted_requested_columns.push_back(requested);
    }

    auto source = std::make_shared<DuckLakeInlinedDataSource>(std::make_shared<const Block>(std::move(header)), std::move(chunks));
    Pipe pipe(std::move(source));

    /// Mirror the fallback filter path of StorageObjectStorageSource: row-level filter
    /// first, then prewhere; the union with file-reading pipes requires identical headers.
    if (info.row_level_filter)
    {
        auto row_level_actions = std::make_shared<ExpressionActions>(info.row_level_filter->actions.clone());
        pipe.addSimpleTransform(
            [row_level_actions, row_level_filter = info.row_level_filter](const SharedHeader & header_)
        {
            return std::make_shared<FilterTransform>(
                header_, row_level_actions, row_level_filter->column_name, row_level_filter->do_remove_column);
        });
    }
    if (info.prewhere_info)
    {
        auto prewhere_actions = std::make_shared<ExpressionActions>(info.prewhere_info->prewhere_actions.clone());
        pipe.addSimpleTransform(
            [prewhere_actions, prewhere_info = info.prewhere_info](const SharedHeader & header_)
        {
            return std::make_shared<FilterTransform>(
                header_, prewhere_actions, prewhere_info->prewhere_column_name, prewhere_info->remove_prewhere_column);
        });
    }

    pipe.addSimpleTransform(
        [requested_columns = adjusted_requested_columns](const SharedHeader & header_)
    {
        return std::make_shared<ExtractColumnsTransform>(header_, requested_columns);
    });

    return pipe;
}

}

#endif
