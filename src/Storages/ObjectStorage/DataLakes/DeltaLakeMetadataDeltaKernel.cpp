#include <DataTypes/DataTypeNullable.h>
#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeSink.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakePartitionedSink.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_delta_lake_writes;
}

[[maybe_unused]] static void tracingCallback(struct ffi::Event event)
{
    /// Do not pollute logs.
    if (event.message.len > 100)
        return;

    const auto message = fmt::format(
        "Message: {}, code line: {}:{}, target: {}",
        std::string_view(event.message.ptr, event.message.len),
        DeltaLake::KernelUtils::fromDeltaString(event.file),
        event.line,
        DeltaLake::KernelUtils::fromDeltaString(event.target));

    auto log = getLogger("DeltaKernelTracing");
    if (event.level == ffi::Level::ERROR)
    {
        LOG_ERROR(log, "{}", message);
    }
    else if (event.level == ffi::Level::WARN)
    {
        LOG_WARNING(log, "{}", message);
    }
    else if (event.level == ffi::Level::INFO)
    {
        LOG_INFO(log, "{}", message);
    }
    else if (event.level == ffi::Level::DEBUG)
    {
        LOG_DEBUG(log, "{}", message);
    }
    else if (event.level == ffi::Level::TRACE)
    {
        LOG_TRACE(log, "{}", message);
    }
}


DeltaLakeMetadataDeltaKernel::DeltaLakeMetadataDeltaKernel(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationWeakPtr configuration_,
    ContextPtr context)
    : log(getLogger("DeltaLakeMetadata"))
    , kernel_helper(DB::getKernelHelper(configuration_.lock(), object_storage))
    , table_snapshot(std::make_shared<DeltaLake::TableSnapshot>(
            kernel_helper,
            object_storage,
            context,
            log))
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    //ffi::enable_event_tracing(tracingCallback, ffi::Level::TRACE);
#endif
}

bool DeltaLakeMetadataDeltaKernel::operator ==(const IDataLakeMetadata & metadata) const
{
    const auto & delta_lake_metadata = dynamic_cast<const DeltaLakeMetadataDeltaKernel &>(metadata);
    std::lock_guard lk1(table_snapshot_mutex);
    std::lock_guard lk2(delta_lake_metadata.table_snapshot_mutex);
    return table_snapshot->getVersion() == delta_lake_metadata.table_snapshot->getVersion();
}

bool DeltaLakeMetadataDeltaKernel::update(const ContextPtr & context)
{
    std::lock_guard lock(table_snapshot_mutex);
    return table_snapshot->update(context);
}

ObjectIterator DeltaLakeMetadataDeltaKernel::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t list_batch_size,
    ContextPtr /* context  */) const
{
    std::lock_guard lock(table_snapshot_mutex);
    return table_snapshot->iterate(filter_dag, callback, list_batch_size);
}

NamesAndTypesList DeltaLakeMetadataDeltaKernel::getTableSchema() const
{
    std::lock_guard lock(table_snapshot_mutex);
    return table_snapshot->getTableSchema();
}

void DeltaLakeMetadataDeltaKernel::modifyFormatSettings(FormatSettings & format_settings) const
{
    /// There can be missing columns because of ALTER ADD/DROP COLUMN.
    /// So to support reading from such tables it is enough to turn on this setting.
    format_settings.parquet.allow_missing_columns = true;
}

static void checkTypesAndNestedTypesEqual(DataTypePtr type, DataTypePtr expected_type, const std::string & column_name)
{
    /// Checks types recursively if needed.
    if (expected_type && !type->equals(*expected_type))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Types mismatch for column `{}`. Got: {}, expected: {}",
            column_name, type->getName(), expected_type->getName());
    }
}

/// Returns column and whether it is readable from data file.
static std::pair<NameAndTypePair, bool> getColumn(
    const NameAndTypePair & column,
    const NamesAndTypesList & read_schema,
    const NameToNameMap & physical_names_map,
    bool get_name_in_storgae,
    LoggerPtr log)
{
    NameAndTypePair result_column = column;

    auto physical_name = DeltaLake::getPhysicalName(get_name_in_storgae ? column.getNameInStorage() : column.name, physical_names_map);
    auto physical_name_and_type = read_schema.tryGetByName(physical_name);

    /// Not a readable column.
    if (!physical_name_and_type.has_value())
    {
        LOG_TEST(log, "Name: {}, name in storage: {}, subcolumn name: {}, physical name: {}",
                 column.name, column.getNameInStorage(), column.getSubcolumnName(), physical_name);

        /// Not a readable column, but if columnMapping.mode == 'name'
        /// we will still have it named as col-<uuid> in metadata.
        result_column.name = physical_name;
        return {result_column, false};
    }

    std::string physical_subcolumn_name;
    if (!get_name_in_storgae && column.isSubcolumn())
    {
        auto pos = physical_name.find_first_of('.');
        physical_subcolumn_name = physical_name.substr(pos + 1);
        physical_name = physical_name.substr(0, pos);
    }

    LOG_TEST(
        log, "Name: {}, name in storage: {}, subcolumn name: {}, "
        "physical name: {}, physical subcolumn name: {}, physical type: {}",
        column.name, column.getNameInStorage(), column.getSubcolumnName(),
        physical_name, physical_subcolumn_name, physical_name_and_type->type->getName());

    if (column.isSubcolumn())
    {
        result_column = NameAndTypePair(
            physical_name,
            /// physical_subcolumn_name can be empty here while column.getSubcolumnName() is not,
            /// if that subcolumn is size0, null, etc.
            physical_subcolumn_name.empty() ? column.getSubcolumnName() : physical_subcolumn_name,
            /* type_in_storage */physical_name_and_type->type,
            /* subcolumn_type */result_column.type);
    }
    else
    {
        result_column = NameAndTypePair(physical_name, physical_name_and_type->type);
    }

    return {result_column, true};
}

ReadFromFormatInfo DeltaLakeMetadataDeltaKernel::prepareReadingFromFormat(
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns,
    bool /*supports_tuple_elements*/)
{
    /// The below code is similar to what can be found in `prepareReadingFromFormat.cpp`,
    /// but is adjusted for delta-lake.
    ReadFromFormatInfo info;


    /// Read schema is different from table schema in case:
    /// 1. we have partition columns (they are not stored in the actual data)
    /// 2. columnMapping.mode = 'name' or 'id'.
    DB::NameToNameMap physical_names_map;
    ColumnsDescription table_columns_description;
    ColumnsDescription read_columns_description;
    {
        std::lock_guard lock(table_snapshot_mutex);
        physical_names_map = table_snapshot->getPhysicalNamesMap();
        table_columns_description = ColumnsDescription(table_snapshot->getTableSchema());
        read_columns_description = ColumnsDescription(table_snapshot->getReadSchema());
    }

    Names non_virtual_requested_columns;
    for (const auto & column_name : requested_columns)
    {
        if (auto virtual_column = storage_snapshot->virtual_columns->tryGet(column_name))
            info.requested_virtual_columns.emplace_back(std::move(*virtual_column));
        else
            non_virtual_requested_columns.push_back(column_name);
    }
    /// Create header for Source that will contain all requested columns including virtual at the end
    /// (because they will be added to the chunk after reading regular columns).
    info.source_header = storage_snapshot->getSampleBlockForColumns(non_virtual_requested_columns);
    for (const auto & column : info.requested_virtual_columns)
        info.source_header.insert({column.type->createColumn(), column.type, column.name});

    /// If only virtual columns were requested, just read the smallest column.
    if (supports_subset_of_columns && non_virtual_requested_columns.empty())
        non_virtual_requested_columns.push_back(ExpressionActions::getSmallestColumn(table_columns_description.getAll()).name);

    auto requested_table_columns = table_columns_description.getByNames(
        GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(),
        non_virtual_requested_columns);
    auto all_read_columns_with_subcolumns = read_columns_description.get(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());

    Names column_names_to_read = non_virtual_requested_columns;
    /// Difference in `requested_table_columns` and `column_names_to_read` after executing `filterTupleColumnsToRead`:
    /// If we have a column `c1.c2` which is an array and have `SELECT c1.c2.size0`,
    /// then in requested_table_columns we will have `c1.c2` as name in storage and `size0` as subcolumn name,
    /// while in column_names_to_read we will have `c1` as name in storage and `c2` as subcolumn name.
    filterTupleColumnsToRead(requested_table_columns, column_names_to_read);

    /// Set format_header with columns that should be read from data.
    /// However, we include partition columns in requested_columns,
    /// because we will insert partition columns into chunk right after it is read from data file,
    /// so we want it to be verified that chunk contains all the requested columns.
    for (auto & column : requested_table_columns)
    {
        auto expected_type = info.source_header.getByName(column.name).type;
        checkTypesAndNestedTypesEqual(column.type, expected_type, column.name);

        auto [result_column, _] = getColumn(
            column,
            all_read_columns_with_subcolumns,
            physical_names_map,
            /* get_name_in_storage */true,
            log);

        info.requested_columns.emplace_back(result_column);
    }

    auto table_columns_to_read = table_columns_description.getByNames(
        GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(),
        column_names_to_read);

    for (auto & column : table_columns_to_read)
    {
        auto [result_column, readable] = getColumn(
            column,
            all_read_columns_with_subcolumns,
            physical_names_map,
            /* get_name_in_storage */false,
            log);

        if (readable)
            info.format_header.insert(ColumnWithTypeAndName{result_column.type, result_column.name});
    }

    info.serialization_hints = getSerializationHintsForFileLikeStorage(storage_snapshot->metadata, context);
    info.columns_description = storage_snapshot->getDescriptionForColumns(column_names_to_read);

    LOG_TEST(log, "Format header: {}", info.format_header.dumpStructure());
    LOG_TEST(log, "Source header: {}", info.source_header.dumpStructure());
    LOG_TEST(log, "Requested columns: {}", info.requested_columns.toString());
    LOG_TEST(log, "Columns description: {}", info.columns_description.toString());
    return info;
}

SinkToStoragePtr DeltaLakeMetadataDeltaKernel::write(
    SharedHeader sample_block,
    const StorageID & /* table_id */,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context,
    std::shared_ptr<DataLake::ICatalog> /* catalog */)
{
    if (!context->getSettingsRef()[Setting::allow_experimental_delta_lake_writes])
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "To enable delta lake writes, use allow_experimental_delta_lake_writes = 1");
    }

    Names partition_columns;
    {
        std::lock_guard lock(table_snapshot_mutex);
        partition_columns = table_snapshot->getPartitionColumns();
    }

    auto delta_transaction = std::make_shared<DeltaLake::WriteTransaction>(kernel_helper);
    delta_transaction->create();

    if (partition_columns.empty())
    {
        return std::make_shared<DeltaLakeSink>(
            delta_transaction, configuration, object_storage, context, sample_block, format_settings);
    }

    return std::make_shared<DeltaLakePartitionedSink>(
        delta_transaction, configuration, partition_columns, object_storage,
        context, sample_block, format_settings);
}

}

#endif
