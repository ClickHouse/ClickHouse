#include <Storages/System/StorageSystemIcebergFiles.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/VirtualColumnUtils.h>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool use_iceberg_metadata_files_cache;
}
namespace
{

class SystemIcebergFilesSource : public ISource
{
public:
    SystemIcebergFilesSource(
        SharedHeader header_,
        ContextPtr context_,
        size_t max_block_size_,
        ExpressionActionsPtr virtual_columns_filter_)
        : ISource(header_)
        , context(context_)
        , max_block_size(max_block_size_)
        , virtual_columns_filter(std::move(virtual_columns_filter_))
    {
        databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true});
        current_database_iterator = databases.begin();
        if (current_database_iterator != databases.end())
            current_table_iterator = current_database_iterator->second->getTablesIterator(context, {}, true);
    }

    String getName() const override { return "SystemIcebergFilesSource"; }

protected:
    Chunk generate() override
    {
        ContextMutablePtr context_copy = Context::createCopy(context);
        Settings settings_copy = context_copy->getSettingsCopy();
        // Do not use the cache for now. It has previously caused correctness issues in system.iceberg_history (https://github.com/ClickHouse/ClickHouse/pull/89003).
        settings_copy[Setting::use_iceberg_metadata_files_cache] = false;
        context_copy->setSettings(settings_copy);

        const auto access = context_copy->getAccess();
        const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

        if (!show_tables_granted)
            return {};

        MutableColumnPtr col_database = ColumnString::create();
        MutableColumnPtr col_table = ColumnString::create();
        MutableColumnPtr col_snapshot_id = ColumnInt64::create();
        MutableColumnPtr col_content = ColumnInt8::create();

        MutableColumnPtr col_file_path = ColumnString::create();
        MutableColumnPtr col_file_format = ColumnString::create();
        MutableColumnPtr col_record_count = ColumnInt64::create();
        MutableColumnPtr col_file_size_in_bytes = ColumnInt64::create();

        MutableColumnPtr col_partition = ColumnString::create();
        MutableColumnPtr col_schema_id = ColumnInt32::create();
        MutableColumnPtr col_sequence_number = ColumnInt64::create();
        MutableColumnPtr col_sort_order_id = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());

        auto make_int32_int64_map = [] -> MutableColumnPtr
        {
            MutableColumns tuple_columns;
            tuple_columns.emplace_back(ColumnInt32::create());
            tuple_columns.emplace_back(ColumnInt64::create());
            MutableColumnPtr nested = ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)));
            return ColumnMap::create(std::move(nested));
        };

        MutableColumnPtr col_null_value_counts = make_int32_int64_map();
        MutableColumnPtr col_column_sizes = make_int32_int64_map();
        MutableColumnPtr col_value_counts = make_int32_int64_map();
        MutableColumnPtr col_equality_ids = ColumnArray::create(ColumnInt32::create());

        std::vector<IColumn *> col_ptrs{
            col_database.get(), col_table.get(), col_snapshot_id.get(), col_content.get(),
            col_file_path.get(), col_file_format.get(), col_record_count.get(), col_file_size_in_bytes.get(),
            col_partition.get(), col_schema_id.get(), col_sequence_number.get(), col_sort_order_id.get(),
            col_null_value_counts.get(), col_column_sizes.get(), col_value_counts.get(), col_equality_ids.get()};

        auto get_total_size = [&] -> size_t
        {
            size_t total_size = 0;
            for (const auto & col : col_ptrs)
                total_size += col->byteSize();
            return total_size;
        };

        size_t num_rows = 0;

        auto add_file_records = [&](const DatabaseTablesIteratorPtr & it, StorageObjectStorage * object_storage)
        {
            if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
                return;

            if (!object_storage->isIcebergStorage())
                return;

#if USE_AVRO
            /// Checkpoints to revert to pre-row state in case any per-column inserts throw midway.
            std::vector<ColumnCheckpointPtr> checkpoints(col_ptrs.size());
            for (size_t i = 0; i < col_ptrs.size(); ++i)
                checkpoints[i] = col_ptrs[i]->getCheckpoint();

            /// Iceberg tables can be broken in arbitrary ways; tolerate any error from a single table
            /// instead of failing the whole query.
            try
            {
                if (IcebergMetadata * iceberg_metadata = dynamic_cast<IcebergMetadata *>(object_storage->getExternalMetadata(context_copy)); iceberg_metadata)
                {
                    IcebergMetadata::IcebergFiles files = iceberg_metadata->getFiles(context_copy);

                    for (auto & file : files)
                    {
                        col_database->insert(it->databaseName());
                        col_table->insert(it->name());
                        col_snapshot_id->insert(file.snapshot_id);
                        col_content->insert(static_cast<Int8>(file.content));
                        col_file_path->insert(file.file_path);
                        col_file_format->insert(file.file_format);
                        col_record_count->insert(file.record_count);
                        col_file_size_in_bytes->insert(file.file_size_in_bytes);
                        col_partition->insert(file.partition);
                        col_schema_id->insert(file.schema_id);
                        col_sequence_number->insert(file.sequence_number);

                        if (file.sort_order_id.has_value())
                            col_sort_order_id->insert(*file.sort_order_id);
                        else
                            col_sort_order_id->insertDefault();

                        auto insert_id_to_int_map = [&](MutableColumnPtr & col, const std::map<Int32, Int64> & values)
                        {
                            Map map_field;
                            map_field.reserve(values.size());
                            for (const auto & [key, value] : values)
                                map_field.push_back(Tuple{key, value});
                            col->insert(map_field);
                        };

                        insert_id_to_int_map(col_null_value_counts, file.null_value_counts);
                        insert_id_to_int_map(col_column_sizes, file.column_sizes);
                        insert_id_to_int_map(col_value_counts, file.value_counts);

                        Array equality_ids_array;
                        if (file.equality_ids.has_value())
                        {
                            equality_ids_array.reserve(file.equality_ids->size());
                            for (auto id : *file.equality_ids)
                                equality_ids_array.push_back(id);
                        }
                        col_equality_ids->insert(equality_ids_array);

                        ++num_rows;
                    }
                }
            }
            catch (...)
            {
                for (size_t i = 0; i < col_ptrs.size(); ++i)
                    col_ptrs[i]->rollback(*checkpoints[i]);
                tryLogCurrentException(getLogger("SystemIcebergFiles"), fmt::format("Ignoring broken table {}", object_storage->getStorageID().getFullTableName()));
            }
#endif
        };


        while (true)
        {
            if (num_rows && max_block_size && get_total_size() > max_block_size)
                break;

            if (current_database_iterator == databases.end())
                break;

            if (!current_table_iterator || !current_table_iterator->isValid())
            {
                ++current_database_iterator;
                if (current_database_iterator != databases.end())
                    current_table_iterator = current_database_iterator->second->getTablesIterator(context, {}, true);
                continue;
            }

            if (virtual_columns_filter)
            {
                MutableColumnPtr db_col = ColumnString::create();
                MutableColumnPtr tbl_col = ColumnString::create();
                db_col->insert(current_database_iterator->first);
                tbl_col->insert(current_table_iterator->name());
                Block check_block
                {
                    { std::move(db_col), std::make_shared<DataTypeString>(), "database" },
                    { std::move(tbl_col), std::make_shared<DataTypeString>(), "table" },
                };
                VirtualColumnUtils::filterBlockWithExpression(virtual_columns_filter, check_block);
                if (!check_block.rows())
                {
                    current_table_iterator->next();
                    continue;
                }
            }

            {
                StoragePtr storage = current_table_iterator->table();

                TableLockHolder lock = storage->tryLockForShare(context_copy->getCurrentQueryId(), context_copy->getSettingsRef()[Setting::lock_acquire_timeout]);
                if (lock)
                {
                    if (auto * object_storage_table = dynamic_cast<StorageObjectStorage *>(storage.get()))
                        add_file_records(current_table_iterator, object_storage_table);
                }
            }
            current_table_iterator->next();
        }

        if (!num_rows)
            return {};

        Columns columns{
            std::move(col_database), std::move(col_table), std::move(col_snapshot_id), std::move(col_content),
            std::move(col_file_path), std::move(col_file_format), std::move(col_record_count), std::move(col_file_size_in_bytes),
            std::move(col_partition), std::move(col_schema_id), std::move(col_sequence_number), std::move(col_sort_order_id),
            std::move(col_null_value_counts), std::move(col_column_sizes), std::move(col_value_counts), std::move(col_equality_ids)};

        return Chunk(std::move(columns), num_rows);
    }

private:
    ContextPtr context;
    [[maybe_unused]] const size_t max_block_size;
    ExpressionActionsPtr virtual_columns_filter;
    DB::Databases databases;
    DB::Databases::const_iterator current_database_iterator;
    DB::DatabaseTablesIteratorPtr current_table_iterator;
};

class ReadFromSystemIcebergFiles final : public SourceStepWithFilter
{
public:
    ReadFromSystemIcebergFiles(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Block & header,
        size_t max_block_size_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(header),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage_limits(query_info.storage_limits)
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return "ReadFromSystemIcebergFiles"; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override
    {
        SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

        if (filter_actions_dag)
        {
            Block block_to_filter
            {
                { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
                { ColumnString::create(), std::make_shared<DataTypeString>(), "table" },
            };

            auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);
            if (dag)
                virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
        }
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto source = std::make_shared<SystemIcebergFilesSource>(getOutputHeader(), context, max_block_size, virtual_columns_filter);
        source->setStorageLimits(storage_limits);
        processors.emplace_back(source);
        pipeline.init(Pipe(std::move(source)));
    }

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;
    const size_t max_block_size;
    ExpressionActionsPtr virtual_columns_filter;
};

}

StorageSystemIcebergFiles::StorageSystemIcebergFiles(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;

    // Values match `Iceberg::FileContentType` in `Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h`,
    // duplicated here because that header is only available when `USE_AVRO` is enabled.
    auto content_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"DATA", 0},
        {"POSITION_DELETE", 1},
        {"EQUALITY_DELETE", 2},
    });

    storage_metadata.setColumns(ColumnsDescription
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
    });
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemIcebergFiles::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader);
    auto read_step = std::make_unique<ReadFromSystemIcebergFiles>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        header,
        max_block_size);
    query_plan.addStep(std::move(read_step));
}

}
