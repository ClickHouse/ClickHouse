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

namespace DB
{

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

ReadFromFormatInfo DeltaLakeMetadataDeltaKernel::prepareReadingFromFormat(
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns,
    bool supports_tuple_elements)
{
    auto info = DB::prepareReadingFromFormat(requested_columns, storage_snapshot, context, supports_subset_of_columns, supports_tuple_elements);

    /// Read schema is different from table schema in case:
    /// 1. we have partition columns (they are not stored in the actual data)
    /// 2. columnMapping.mode = 'name' or 'id'.
    /// So we add partition columns to read schema and put it together into format_header.
    /// Partition values will be added to result data right after data is read.
    DB::NameToNameMap physical_names_map;
    DB::NameSet read_columns;
    {
        std::lock_guard lock(table_snapshot_mutex);
        physical_names_map = table_snapshot->getPhysicalNamesMap();
        read_columns = table_snapshot->getReadSchema().getNameSet();
    }

    Block format_header;
    for (auto && column_with_type_and_name : info.format_header)
    {
        auto physical_name = DeltaLake::getPhysicalName(column_with_type_and_name.name, physical_names_map);
        if (!read_columns.contains(physical_name))
        {
            LOG_TEST(log, "Filtering out non-readable column: {}", column_with_type_and_name.name);
            continue;
        }
        column_with_type_and_name.name = physical_name;
        format_header.insert(std::move(column_with_type_and_name));
    }
    info.format_header = std::move(format_header);

    /// Update requested columns to reference actual physical column names.
    if (!physical_names_map.empty())
    {
        for (auto & [column_name, _] : info.requested_columns)
            column_name = DeltaLake::getPhysicalName(column_name, physical_names_map);
    }

    LOG_TEST(log, "Format header: {}", info.format_header.dumpNames());
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
