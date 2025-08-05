#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Common/logger_useful.h>

namespace DB
{

DeltaLakeMetadataDeltaKernel::DeltaLakeMetadataDeltaKernel(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationWeakPtr configuration_,
    ContextPtr context)
    : log(getLogger("DeltaLakeMetadata"))
    , table_snapshot(
        std::make_shared<DeltaLake::TableSnapshot>(
            getKernelHelper(configuration_.lock(), object_storage),
            object_storage,
            context,
            log))
{
}

bool DeltaLakeMetadataDeltaKernel::operator ==(const IDataLakeMetadata & metadata) const
{
    const auto & delta_lake_metadata = dynamic_cast<const DeltaLakeMetadataDeltaKernel &>(metadata);
    return table_snapshot->getVersion() == delta_lake_metadata.table_snapshot->getVersion();
}

bool DeltaLakeMetadataDeltaKernel::update(const ContextPtr & context)
{
    return table_snapshot->update(context);
}

ObjectIterator DeltaLakeMetadataDeltaKernel::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t list_batch_size,
    ContextPtr /* context  */) const
{
    return table_snapshot->iterate(filter_dag, callback, list_batch_size);
}

NamesAndTypesList DeltaLakeMetadataDeltaKernel::getTableSchema() const
{
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
    bool supports_subset_of_columns)
{
    auto info = DB::prepareReadingFromFormat(requested_columns, storage_snapshot, context, supports_subset_of_columns);

    /// Read schema is different from table schema in case:
    /// 1. we have partition columns (they are not stored in the actual data)
    /// 2. columnMapping.mode = 'name' or 'id'.
    /// So we add partition columns to read schema and put it together into format_header.
    /// Partition values will be added to result data right after data is read.
    const auto & physical_names_map = table_snapshot->getPhysicalNamesMap();
    const auto read_columns = table_snapshot->getReadSchema().getNameSet();

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
    return info;
}

}

#endif
