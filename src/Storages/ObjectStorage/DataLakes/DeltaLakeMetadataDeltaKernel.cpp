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
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_delta_lake_writes;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
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
    if (!type->equals(*expected_type))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Types mismatch for column `{}`. Got: {}, expected: {}",
            column_name, type->getName(), expected_type->getName());
    }
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
    DB::NamesAndTypesList read_schema;
    DB::NamesAndTypesList table_schema;
    {
        std::lock_guard lock(table_snapshot_mutex);
        physical_names_map = table_snapshot->getPhysicalNamesMap();
        read_schema = table_snapshot->getReadSchema();
        table_schema = table_snapshot->getTableSchema();
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

    /// Set format_header with columns that should be read from data.
    /// However, we include partition columns in requested_columns,
    /// because we will insert partition columns into chunk right after it is read from data file,
    /// so we want it to be verified that chunk contains all the requested columns.
    for (const auto & column_name : non_virtual_requested_columns)
    {
        auto name_and_type = table_schema.tryGetByName(column_name);
        if (!name_and_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found column in schema: {}", column_name);

        auto expected_type = info.source_header.getByName(column_name).type;

        /// Convert for compatibility, because it used to work this way.
        if (name_and_type->type->isNullable() && !expected_type->isNullable())
            name_and_type->type = removeNullable(name_and_type->type);

        checkTypesAndNestedTypesEqual(name_and_type->type, expected_type, column_name);

        auto physical_name = DeltaLake::getPhysicalName(column_name, physical_names_map);
        auto physical_name_and_type = read_schema.tryGetByName(physical_name);

        LOG_TEST(
            log, "Name: {}, physical name: {}, contained in read schema: {}",
            column_name, physical_name, physical_name_and_type.has_value());

        if (physical_name_and_type.has_value())
        {
            /// Convert for compatibility, because it used to work this way.
            /// Note: however, we do not convert nested types.
            if (physical_name_and_type->type->isNullable() && !expected_type->isNullable())
                physical_name_and_type->type = removeNullable(physical_name_and_type->type);

            info.requested_columns.push_back(physical_name_and_type.value());
            info.format_header.insert(ColumnWithTypeAndName
            {
                physical_name_and_type->type->createColumn(),
                physical_name_and_type->type,
                physical_name_and_type->name
            });
        }
        else
        {
            info.requested_columns.emplace_back(physical_name, name_and_type->type);
        }
    }

    /// If only virtual columns were requested, just read the smallest column.
    if (supports_subset_of_columns && non_virtual_requested_columns.empty())
        non_virtual_requested_columns.push_back(ExpressionActions::getSmallestColumn(table_schema).name);

    info.columns_description = storage_snapshot->getDescriptionForColumns(non_virtual_requested_columns);
    info.serialization_hints = getSerializationHintsForFileLikeStorage(storage_snapshot->metadata, context);

    LOG_TEST(log, "Format header: {}", info.format_header.dumpStructure());
    LOG_TEST(log, "Source header: {}", info.source_header.dumpStructure());
    LOG_TEST(log, "Requested columns: {}", info.requested_columns.toString());
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
